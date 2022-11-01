package com.netease.arctic.demo;

import com.netease.arctic.demo.entity.CallContext;
import com.netease.arctic.demo.entity.SyncDbParams;
import com.netease.arctic.demo.source.MysqlCdcCatalog;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.MySqlDeserializationConverterFactory;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.MetadataConverter;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.function.Consumer;

import static com.ververica.cdc.connectors.mysql.table.MySqlReadableMetadata.DATABASE_NAME;
import static com.ververica.cdc.connectors.mysql.table.MySqlReadableMetadata.TABLE_NAME;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * TODO: per table config. per table sql. multiple table merge into one
 */
@SuppressWarnings("unused")
public class SyncDbFunction {
    private static final ConfigOption<String> SRC_DB = ConfigOptions.key("custom.sync-db.source.db").stringType().noDefaultValue();
    private static final ConfigOption<String> DEST_DB = ConfigOptions.key("custom.sync-db.dest.db").stringType().noDefaultValue();

    private static Logger logger = Logger.getLogger(SyncDbFunction.class);

    public static List<Tuple2<ObjectPath, ResolvedCatalogTable>> getPathAndTable(final MysqlCdcCatalog mysql, final String mysqlDb) throws DatabaseNotExistException {
        return mysql.listTables(mysqlDb).stream().filter(t -> !t.equals("heartbeat")).map(t -> {
            final ObjectPath p = new ObjectPath(mysqlDb, t);
            try {
                return Tuple2.of(p, ((ResolvedCatalogTable) mysql.getTable(p)));
            } catch (TableNotExistException e) {
                e.printStackTrace();
            }
            return null;
        }).collect(toList());
    }

    public static MySqlSource<RowData> getMySqlSource(final String srcCatalogDb, List<String> tableList, final MysqlCdcCatalog mysql, final Map<String, RowDataDebeziumDeserializeSchema> maps) {

        // TODO: extract hostname port from url
        return MySqlSource.<RowData>builder()
                .hostname(mysql.getHostname())
                .port(mysql.getPort())
                .username(mysql.getUsername())
                .password(mysql.getPassword())
//                .databaseList(srcCatalogDb)
                .databaseList(srcCatalogDb)
                .tableList(".*")
//                .tableList(tableList.toArray(new String[tableList.size()]))
                .deserializer(new CompositeDebeziuDeserializationSchema(maps))
                .build();
    }

    public static Map<String, RowDataDebeziumDeserializeSchema> getDebeziumDeserializeSchemas(final List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {
        return pathAndTable.stream().collect(toMap(e -> e.f0.toString(), e -> RowDataDebeziumDeserializeSchema.newBuilder()
                .setPhysicalRowType((RowType) e.f1.getResolvedSchema().toPhysicalRowDataType().getLogicalType())
                .setUserDefinedConverterFactory(MySqlDeserializationConverterFactory.instance())
                .setMetadataConverters(new MetadataConverter[]{
                        TABLE_NAME.getConverter(),
                        DATABASE_NAME.getConverter()
                })
                .setResultTypeInfo(TypeInformation.of(RowData.class))
                .build()));
    }

    public static Map<String, RowRowConverter> getConverters(final List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {
        return pathAndTable.stream().collect(toMap(e -> e.f0.toString(), e -> RowRowConverter.create(e.f1.getResolvedSchema().toPhysicalRowDataType())));
    }

    public static List<SyncDbParams> getParamsList(final String mysqlDb, final List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {
        return pathAndTable.stream().map(e -> {
            final OutputTag<Row> tag = new OutputTag<Row>(e.f0.getFullName()) {
            };
            final List<DataTypes.Field> fields = e.f1.getResolvedSchema().getColumns()
                    .stream().map(c -> DataTypes.FIELD(c.getName(), c.getDataType())).collect(toList());
            final Schema schema = Schema.newBuilder()
                    .column("f0", DataTypes.ROW(fields.toArray(new DataTypes.Field[]{}))).build();
            return SyncDbParams.builder()
                    .table(e.f0.getObjectName())
                    .path(new ObjectPath(mysqlDb, e.f0.getObjectName()))
                    .tag(tag)
                    .schema(schema)
                    .build();
        }).collect(toList());
    }

    private static String getKey(RowData r) {
        final JoinedRowData rowData = (JoinedRowData) r;
        return rowData.getString(rowData.getArity() - 3).toString() + "." + rowData.getString(rowData.getArity() - 2).toString();
    }

    private static class CompositeDebeziuDeserializationSchema implements DebeziumDeserializationSchema<RowData> {

        private final Map<String, RowDataDebeziumDeserializeSchema> deserializationSchemaMap;

        public CompositeDebeziuDeserializationSchema(final Map<String, RowDataDebeziumDeserializeSchema> deserializationSchemaMap) {
            this.deserializationSchemaMap = deserializationSchemaMap;
        }

        @Override
        public void deserialize(final SourceRecord record, final Collector<RowData> out) throws Exception {
            final Struct value = (Struct) record.value();
            final Struct source = value.getStruct("source");
            final String db = source.getString("db");
            final String table = source.getString("table");
            if (deserializationSchemaMap == null)
                throw new IllegalStateException("deserializationSchemaMap can not be null!");
            deserializationSchemaMap.get(db + "." + table).deserialize(record, out);
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return TypeInformation.of(RowData.class);
        }
    }

    static class RowDataVoidProcessFunction extends ProcessFunction<RowData, Void> {

        private final Map<String, RowRowConverter> converters;

        public RowDataVoidProcessFunction(final Map<String, RowRowConverter> converterMap) {
            this.converters = converterMap;
        }

        @Override
        public void processElement(final RowData rowData, final ProcessFunction<RowData, Void>.Context ctx, final Collector<Void> out) throws Exception {
            final String key = rowData.getString(rowData.getArity() - 1).toString() + "." + rowData.getString(rowData.getArity() - 2).toString();
            ctx.output(new OutputTag<Row>(key) {
            }, this.converters.get(key).toExternal(rowData));
        }
    }

//    public static void main(String[] args) throws DatabaseNotExistException, TableNotExistException {
//        final MysqlCdcCatalog catalog = new MysqlCdcCatalog("mysql", "test2w", "rds",
//                "123456", "sloth-commerce-test2.jd.163.org", 3332);
//        catalog.listDatabases().forEach(System.out::println);
//        catalog.listTables("test2w").forEach(System.out::println);
//        System.out.println(catalog.getTable(new ObjectPath("test2w", "warehouse")).getOptions());
//    }
}
