/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.benchmark.ingestion;

import com.netease.arctic.benchmark.ingestion.params.database.SyncDBParams;
import com.netease.arctic.benchmark.ingestion.source.MysqlCDCCatalog;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.MySqlDeserializationConverterFactory;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.MetadataConverter;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import static com.ververica.cdc.connectors.mysql.table.MySqlReadableMetadata.DATABASE_NAME;
import static com.ververica.cdc.connectors.mysql.table.MySqlReadableMetadata.TABLE_NAME;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * database synchronize function, includes getting data sources and converting data types
 */
@SuppressWarnings("unused")
public class SyncDbFunction {
  private static final Logger LOG = LoggerFactory.getLogger(SyncDbFunction.class);

  public static List<Tuple2<ObjectPath, ResolvedCatalogTable>> getPathAndTable(
      final MysqlCDCCatalog mysql, final String mysqlDb, List<String> tableList)
      throws DatabaseNotExistException {
    return mysql.listTables(mysqlDb).stream().filter(t -> !t.equals("heartbeat"))
        .filter(tableList::contains).map(t -> {
          final ObjectPath p = new ObjectPath(mysqlDb, t);
          try {
            return Tuple2.of(p, ((ResolvedCatalogTable) mysql.getTable(p)));
          } catch (TableNotExistException e) {
            e.printStackTrace();
          }
          return null;
        }).collect(toList());
  }

  public static MySqlSource<RowData> getMySqlSource(final MysqlCDCCatalog mysql,
      final String srcCatalogDb, List<String> tableList,
      final Map<String, RowDataDebeziumDeserializeSchema> maps, String startUpMode) {
    return MySqlSource.<RowData>builder().hostname(mysql.getHostname()).port(mysql.getPort())
        .username(mysql.getUsername()).password(mysql.getPassword()).databaseList(srcCatalogDb)
        .tableList(tableList.stream().map(e -> srcCatalogDb + "." + e).toArray(String[]::new))
        .startupOptions(getStartUpMode(startUpMode))
        .deserializer(new CompositeDebeziuDeserializationSchema(maps)).build();
  }

  public static Map<String, RowDataDebeziumDeserializeSchema> getDebeziumDeserializeSchemas(
      final List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {
    return pathAndTable.stream()
        .collect(toMap(e -> e.f0.toString(), e -> RowDataDebeziumDeserializeSchema.newBuilder()
            .setPhysicalRowType(
                (RowType) e.f1.getResolvedSchema().toPhysicalRowDataType().getLogicalType())
            .setUserDefinedConverterFactory(MySqlDeserializationConverterFactory.instance())
            .setMetadataConverters(
                new MetadataConverter[] {TABLE_NAME.getConverter(), DATABASE_NAME.getConverter()})
            .setResultTypeInfo(TypeInformation.of(RowData.class)).build()));
  }

  public static Map<String, RowRowConverter> getConverters(
      final List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {
    return pathAndTable.stream().collect(toMap(e -> e.f0.toString(),
        e -> RowRowConverter.create(e.f1.getResolvedSchema().toPhysicalRowDataType())));
  }

  public static List<SyncDBParams> getParamsList(final String mysqlDb,
      final List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {
    return pathAndTable.stream().map(e -> {
      final OutputTag<RowData> tag = new OutputTag<RowData>(e.f0.getFullName()) {};
      final List<Field> fields = e.f1.getResolvedSchema().getColumns().stream()
          .map(c -> DataTypes.FIELD(c.getName(), c.getDataType())).collect(toList());
      final Schema schema =
          Schema.newBuilder().fromResolvedSchema(e.f1.getResolvedSchema()).build();
      return SyncDBParams.builder().table(e.f0.getObjectName())
          .path(new ObjectPath(mysqlDb, e.f0.getObjectName())).tag(tag).schema(schema).build();
    }).collect(toList());
  }

  private static String getKey(RowData r) {
    final JoinedRowData rowData = (JoinedRowData) r;
    return rowData.getString(rowData.getArity() - 3).toString() + "." +
        rowData.getString(rowData.getArity() - 2).toString();
  }

  private static StartupOptions getStartUpMode(String startUpMode) {
    if (startUpMode.equals("initial")) {
      return StartupOptions.initial();
    } else if (startUpMode.equals("latest-offset")) {
      return StartupOptions.latest();
    }
    throw new RuntimeException("startUpMode is error, now only support initial/latest-offset");
  }

  private static class CompositeDebeziuDeserializationSchema
      implements DebeziumDeserializationSchema<RowData> {

    private final Map<String, RowDataDebeziumDeserializeSchema> deserializationSchemaMap;

    public CompositeDebeziuDeserializationSchema(
        final Map<String, RowDataDebeziumDeserializeSchema> deserializationSchemaMap) {
      this.deserializationSchemaMap = deserializationSchemaMap;
    }

    @Override
    public void deserialize(final SourceRecord record, final Collector<RowData> out)
        throws Exception {
      final Struct value = (Struct) record.value();
      final Struct source = value.getStruct("source");
      final String db = source.getString("db");
      final String table = source.getString("table");
      if (deserializationSchemaMap == null) {
        throw new IllegalStateException("deserializationSchemaMap can not be null!");
      }
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
    public void processElement(final RowData rowData,
        final ProcessFunction<RowData, Void>.Context ctx, final Collector<Void> out)
        throws Exception {
      final String key = rowData.getString(rowData.getArity() - 1).toString() + "." +
          rowData.getString(rowData.getArity() - 2).toString();
      ctx.output(new OutputTag<RowData>(key) {},
          getField(JoinedRowData.class, (JoinedRowData) rowData, "row1"));
    }

    private static <O, V> V getField(Class<O> clazz, O obj, String fieldName) {
      try {
        java.lang.reflect.Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        Object v = field.get(obj);
        return v == null ? null : (V) v;
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
