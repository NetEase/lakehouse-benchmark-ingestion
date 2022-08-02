package com.netease.arctic.demo;

import com.netease.arctic.demo.function.CallContext;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.ververica.cdc.connectors.mysql.table.MySqlReadableMetadata.DATABASE_NAME;
import static com.ververica.cdc.connectors.mysql.table.MySqlReadableMetadata.TABLE_NAME;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * TODO: per table config. per table sql. multiple table merge into one
 */
@SuppressWarnings("unused")
public class SyncDbFunctionForDistrict implements Consumer<CallContext> {
    private static final ConfigOption<String> SRC_DB = ConfigOptions.key("custom.sync-db.source.db").stringType().noDefaultValue();
    private static final ConfigOption<String> DEST_DB = ConfigOptions.key("custom.sync-db.dest.db").stringType().noDefaultValue();

    private static Logger logger = Logger.getLogger(SyncDbFunctionForDistrict.class);

    private static String getKey(RowData r) {
        final JoinedRowData rowData = (JoinedRowData) r;
        return rowData.getString(rowData.getArity() - 3).toString() + "." + rowData.getString(rowData.getArity() - 2).toString();
    }

    /**
     * 1. create hudi table
     * 2. make db.table -> deserializerSchema map
     * 3. make db.table -> convert map
     * 4. start single cdc source
     * 1. deserializer SourceRecord to RowData
     * 5 key by db.table
     * 1. RowData to Row
     * 2. side out
     * 6 write hudi
     *
     * @param context 参数
     */
    @SneakyThrows
    @Override
    public void accept(final CallContext context) {
        final StreamTableEnvironment tEnv = context.getTEnv();
        final Configuration configuration = tEnv.getConfig().getConfiguration();

//        configuration.setString(Local.TOTAL_FLINK_MEMORY.key(), "2048");
//        configuration.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "1024");
//        context.getEnv().getCheckpointConfig().setCheckpointStorage(
//                new FileSystemCheckpointStorage("file:///Users/yuekelei/Documents/workspace/hudi_test"));

        final String[] srcCatalogDb = configuration.getString(SRC_DB).split("\\.");
        final MysqlCdcCatalog mysql = (MysqlCdcCatalog) tEnv.getCatalog(srcCatalogDb[0]).orElseThrow(() -> new RuntimeException(srcCatalogDb[0] + " catalog not exists"));
        final String mysqlDb = srcCatalogDb[1];

        final String[] destCatalogDb = configuration.getString(DEST_DB).split("\\.");
        final String hudiCatalogName = destCatalogDb[0];

        final Catalog hudi = tEnv.getCatalog(hudiCatalogName).orElseThrow(() -> new RuntimeException(hudiCatalogName + " catalog not exists"));
        final String hudiDb = destCatalogDb[1];

        final List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable = getPathAndTable(mysql, mysqlDb);
        List<Tuple2<ObjectPath, ResolvedCatalogTable>> s = pathAndTable.stream().filter(e -> e.f0.getObjectName().equals("district")).collect(toList());
        createHudiTable(hudi, hudiDb, s);
//        createHudiTable(hudi, hudiDb, pathAndTable);

        final MySqlSource<RowData> source = getMySqlSource(srcCatalogDb, mysql, getDebeziumDeserializeSchemas(s));
        final SingleOutputStreamOperator<Void> process = context.getEnv()
                .fromSource(source, WatermarkStrategy.noWatermarks(), "mysql").uid("mysql").setParallelism(8)
                .process(new RowDataVoidProcessFunction(getConverters(s))).uid("split stream").name("split stream").setParallelism(8);
        final StatementSet set = tEnv.createStatementSet();
        getParamsList(mysqlDb, s).forEach(p -> {
            tEnv.createTemporaryView(p.table, process.getSideOutput(p.tag), p.schema);
            String sql = String.format("INSERT INTO %s.%s.%s SELECT f0.* FROM %s", hudiCatalogName, hudiDb, p.path.getObjectName(), p.table);
            set.addInsertSql(sql);
        });
        set.execute();

    }

    private List<Tuple2<ObjectPath, ResolvedCatalogTable>> getPathAndTable(final MysqlCdcCatalog mysql, final String mysqlDb) throws DatabaseNotExistException {
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


    private void createHudiTable(final Catalog hudi, final String hudiDb, final List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {
        if (!hudi.databaseExists(hudiDb)) {
            try {
                hudi.createDatabase(hudiDb, new CatalogDatabaseImpl(new HashMap<>(), "new db"), false);
            } catch (DatabaseAlreadyExistException e) {
                e.printStackTrace();
            }
        }

        pathAndTable.forEach(e -> {
            try {
                // TODO: read external options hoodie-catalog.yml using context.getEnv().getCachedFiles()
                final Map<String, String> options = new HashMap<>();
                options.put("hive_sync.enable", "true");
                options.put("hive_sync.mode", "hms");
                options.put("hive_sync.metastore.uris", "thrift://hz11-trino-arctic-0.jd.163.org:9083");
//                options.put("hive_sync.skip_ro_suffix", "true");

                options.put("hive_sync.db", hudiDb);
                options.put("hive_sync.table", e.f0.getObjectName());
                options.put("hive_sync.support_timestamp","true");
                options.put("hoodie.datasource.hive_sync.support_timestamp","true");

                options.put("table.type", "MERGE_ON_READ");
                options.put("read.tasks", "8");
                options.put("write.bucket_assign.tasks", "8");
                options.put("write.tasks", "8");
                options.put("write.batch.size", "128");
//                options.put("write.log_block.size", "256");
//                options.put("write.rate.limit", "10000");

//                options.put("compaction.async.enabled","false");
//                options.put("compaction.schedule.enabled","false");
//                options.put("clean.async.enabled","true");


                options.put("compaction.tasks", "8");
                options.put("compaction.trigger.strategy", "num_or_time");
                options.put("compaction.delta_commits", "1");
                options.put("compaction.delta_seconds", "60");
                options.put("hoodie.embed.timeline.server", "false");

//                options.put("compaction.delta_seconds", "60");
//                options.put("hoodie.clean.async", "true");
//                options.put("hoodie.cleaner.parallelism", "2");
                ObjectPath objectPath = new ObjectPath(hudiDb, e.f0.getObjectName());

                if (hudi.tableExists(objectPath)) {
                    hudi.dropTable(objectPath, true);
                }
                hudi.createTable(objectPath, new ResolvedCatalogTable(e.f1.copy(options), e.f1.getResolvedSchema()), true);
            } catch (TableAlreadyExistException ex) {
                ex.printStackTrace();
            } catch (DatabaseNotExistException ex) {
                ex.printStackTrace();
            } catch (TableNotExistException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    private MySqlSource<RowData> getMySqlSource(final String[] srcCatalogDb, final MysqlCdcCatalog mysql, final Map<String, RowDataDebeziumDeserializeSchema> maps) {

        // TODO: extract hostname port from url
        return MySqlSource.<RowData>builder()
                .hostname(mysql.getHostname())
                .port(mysql.getPort())
                .username(mysql.getUsername())
                .password(mysql.getPassword())
//                .databaseList(srcCatalogDb)
                .databaseList(srcCatalogDb[1])
//                .tableList(".*")
                .tableList(srcCatalogDb[1] + ".district")
                .deserializer(new CompositeDebeziuDeserializationSchema(maps))
                .build();
    }

    private Map<String, RowDataDebeziumDeserializeSchema> getDebeziumDeserializeSchemas(final List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {
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

    private Map<String, RowRowConverter> getConverters(final List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {
        return pathAndTable.stream().collect(toMap(e -> e.f0.toString(), e -> RowRowConverter.create(e.f1.getResolvedSchema().toPhysicalRowDataType())));
    }

    private List<SyncDbParams> getParamsList(final String mysqlDb, final List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {
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

    private static class RowDataVoidProcessFunction extends ProcessFunction<RowData, Void> {

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

    public static void main(String[] args) throws DatabaseNotExistException, TableNotExistException {
        final MysqlCdcCatalog catalog = new MysqlCdcCatalog("mysql", "test2w", "rds",
                "123456", "sloth-commerce-test2.jd.163.org", 3332);
        catalog.listDatabases().forEach(System.out::println);
        catalog.listTables("test2w").forEach(System.out::println);
        System.out.println(catalog.getTable(new ObjectPath("test2w", "warehouse")).getOptions());
    }
}
