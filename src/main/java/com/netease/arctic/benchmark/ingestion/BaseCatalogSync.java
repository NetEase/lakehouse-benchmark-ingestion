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

import com.netease.arctic.benchmark.ingestion.SyncDbFunction.RowDataVoidProcessFunction;
import com.netease.arctic.benchmark.ingestion.params.CallContext;
import com.netease.arctic.benchmark.ingestion.params.catalog.CatalogParams;
import com.netease.arctic.benchmark.ingestion.params.BaseParameters;
import com.netease.arctic.benchmark.ingestion.source.MysqlCdcCatalog;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public abstract class BaseCatalogSync implements Consumer<CallContext> {

  private final BaseParameters baseParameters;

  private static final Logger LOG = LoggerFactory.getLogger(BaseCatalogSync.class);

  protected BaseCatalogSync(BaseParameters baseParameters) {
    this.baseParameters = baseParameters;
  }

  @Override
  public void accept(final CallContext context) {
    StreamExecutionEnvironment env = context.getEnv();
    final StreamTableEnvironment tableEnv = context.getTableEnv();
    final Configuration configuration = tableEnv.getConfig().getConfiguration();

    CatalogParams sourceCatalogParams = getSourceCatalogParam(configuration);
    CatalogParams destCatalogParams = getDestCatalogParam(configuration);

    final MysqlCdcCatalog mysqlCdcCatalog =
        (MysqlCdcCatalog) getCatalog(tableEnv, sourceCatalogParams);
    final Catalog destCatalog = getCatalog(tableEnv, destCatalogParams);
    String sourceDatabaseName = sourceCatalogParams.getDataBaseName();
    List<String> syncTableList =
        getSyncTableList(mysqlCdcCatalog, sourceDatabaseName, baseParameters);
    final List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable;
    final MySqlSource<RowData> source;

    try {
      pathAndTable = SyncDbFunction.getPathAndTable(mysqlCdcCatalog,
          sourceCatalogParams.getDataBaseName(), syncTableList);
      createTable(destCatalog, destCatalogParams.getDataBaseName(), pathAndTable);
      source = SyncDbFunction.getMySqlSource(mysqlCdcCatalog, sourceDatabaseName, syncTableList,
          SyncDbFunction.getDebeziumDeserializeSchemas(pathAndTable),
          baseParameters.getSourceScanStartupMode());
    } catch (DatabaseAlreadyExistException | DatabaseNotExistException e) {
      throw new RuntimeException(e);
    }

    final SingleOutputStreamOperator<Void> process = sideOutputHandler(env, source, pathAndTable);

    insertData(tableEnv, process, sourceCatalogParams, destCatalogParams, pathAndTable);
  }

  public Catalog getCatalog(StreamTableEnvironment tableEnv, CatalogParams catalogParams) {
    String catalogName = catalogParams.getCatalogName();
    return tableEnv.getCatalog(catalogName)
        .orElseThrow(() -> new RuntimeException(catalogName + " catalog not exists"));
  }

  private CatalogParams getSourceCatalogParam(Configuration configuration) {
    String catalogName = baseParameters.getSourceType().toLowerCase() + "_catalog";
    String databaseName = baseParameters.getSourceDatabaseName();
    return CatalogParams.builder().catalogName(catalogName).dataBaseName(databaseName).build();
  }

  private CatalogParams getDestCatalogParam(Configuration configuration) {
    String catalogName = baseParameters.getSinkType().toLowerCase() + "_catalog_ignore";
    String databaseName = baseParameters.getSinkDatabaseName();
    return CatalogParams.builder().catalogName(catalogName).dataBaseName(databaseName).build();
  }

  private SingleOutputStreamOperator<Void> sideOutputHandler(StreamExecutionEnvironment env,
      MySqlSource<RowData> source, List<Tuple2<ObjectPath, ResolvedCatalogTable>> s) {
    return env.fromSource(source, WatermarkStrategy.noWatermarks(), "mysql").uid("mysql")
        .setParallelism(baseParameters.getSourceParallelism())
        .process(new RowDataVoidProcessFunction(SyncDbFunction.getConverters(s)))
        .uid("split stream").name("split stream").setParallelism(4);
  }

  private List<String> getSyncTableList(MysqlCdcCatalog mysqlCdcCatalog, String sourceDatabaseName,
      BaseParameters baseParameters) {
    String tableListParam = baseParameters.getSourceTableName();
    List<String> tableList = new ArrayList<>();
    if (!tableListParam.equals("*")) {
      String[] tables = tableListParam.split("\\s*,\\s*");
      tableList = Arrays.asList(tables);
    } else {
      try {
        tableList = mysqlCdcCatalog.listTables(sourceDatabaseName);
      } catch (DatabaseNotExistException e) {
        throw new RuntimeException(e);
      }
    }
    return tableList;
  }

  public void insertData(StreamTableEnvironment tableEnv, SingleOutputStreamOperator<Void> process,
      CatalogParams sourceCatalogParams, CatalogParams destCatalogParams,
      List<Tuple2<ObjectPath, ResolvedCatalogTable>> s) {
    final StatementSet set = tableEnv.createStatementSet();
    SyncDbFunction.getParamsList(sourceCatalogParams.getDataBaseName(), s).forEach(p -> {
      tableEnv.createTemporaryView(p.getTable(), process.getSideOutput(p.getTag()), p.getSchema());
      String sql = String.format("INSERT INTO %s.%s.%s SELECT f0.* FROM %s",
          destCatalogParams.getCatalogName(), destCatalogParams.getDataBaseName(),
          p.getPath().getObjectName(), p.getTable());
      set.addInsertSql(sql);
    });
    set.execute();
  }

  public abstract void createTable(Catalog catalog, String dbName,
      List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable)
      throws DatabaseAlreadyExistException;
}
