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

package com.netease.arctic.benchmark.ingestion.sink;

import com.netease.arctic.ams.api.client.ArcticThriftUrl;
import com.netease.arctic.benchmark.ingestion.BaseCatalogSync;
import com.netease.arctic.benchmark.ingestion.SyncDbFunction;
import com.netease.arctic.benchmark.ingestion.params.catalog.CatalogParams;
import com.netease.arctic.benchmark.ingestion.params.database.BaseParameters;
import com.netease.arctic.benchmark.ingestion.params.table.ArcticParameters;
import com.netease.arctic.flink.InternalCatalogBuilder;
import com.netease.arctic.flink.catalog.ArcticCatalog;
import com.netease.arctic.flink.table.ArcticTableLoader;
import com.netease.arctic.flink.util.ArcticUtils;
import com.netease.arctic.flink.write.FlinkSink;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableIdentifier;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Arctic synchronize implementation of {@link BaseCatalogSync}, which customised operations for
 * building tables
 */
public class ArcticCatalogSync extends BaseCatalogSync {

  private final ArcticParameters arcticParameters;

  public ArcticCatalogSync(BaseParameters baseParameters, ArcticParameters arcticParameters) {
    super(baseParameters);
    this.arcticParameters = arcticParameters;
  }

  @Override
  public void createTable(Catalog catalog, String dbName,
      List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {
    Catalog arctic = catalog;
    if (!arctic.databaseExists(dbName)) {
      try {
        arctic.createDatabase(dbName, new CatalogDatabaseImpl(new HashMap<>(), "new db"), false);
      } catch (DatabaseAlreadyExistException e) {
        e.printStackTrace();
      }
    }

    final Map<String, String> options = new HashMap<>();
    fillArcticTableOptions(options);

    pathAndTable.forEach(e -> {
      try {
        ObjectPath objectPath = new ObjectPath(dbName, e.f0.getObjectName());

        if (arctic.tableExists(objectPath)) {
          arctic.dropTable(objectPath, true);
        }
        arctic.createTable(objectPath,
            new ResolvedCatalogTable(e.f1.copy(options), e.f1.getResolvedSchema()), false);
      } catch (TableAlreadyExistException | DatabaseNotExistException ex) {
        ex.printStackTrace();
      } catch (TableNotExistException ex) {
        throw new RuntimeException(ex);
      }
    });
  }

  @Override
  public void insertData(StreamTableEnvironment tableEnv, SingleOutputStreamOperator<Void> process,
      CatalogParams sourceCatalogParams, CatalogParams destCatalogParams,
      List<Tuple2<ObjectPath, ResolvedCatalogTable>> s) {
    SyncDbFunction.getParamsList(sourceCatalogParams.getDatabaseName(), s).forEach(p -> {
      ObjectPath objectPath = new ObjectPath(destCatalogParams.getDatabaseName(), p.getTable());
      ArcticCatalog catalog = (ArcticCatalog) destCatalogParams.getCatalog();
      CatalogBaseTable catalogTable;
      try {
        catalogTable = catalog.getTable(objectPath);
      } catch (TableNotExistException e) {
        throw new RuntimeException(e);
      }
      String url = arcticParameters.getMetastoreURL();
      InternalCatalogBuilder catalogBuilder = InternalCatalogBuilder.builder().metastoreUrl(url);
      ArcticThriftUrl arcticThriftUrl = ArcticThriftUrl.parse(url);
      TableIdentifier tableIdentifier = TableIdentifier.of(arcticThriftUrl.catalogName(),
          destCatalogParams.getDatabaseName(), p.getPath().getObjectName());

      ArcticTableLoader tableLoader = ArcticTableLoader.of(tableIdentifier, catalogBuilder);
      ArcticTable table = ArcticUtils.loadArcticTable(tableLoader);

      FlinkSink.forRowData(process.getSideOutput(p.getTag())).table(table).tableLoader(tableLoader)
          .flinkSchema(catalogTable.getSchema()).build();
    });
  }

  private void fillArcticTableOptions(Map<String, String> options) {
    if (arcticParameters.getOptimizeEnable()) {
      options.put("optimize.group", arcticParameters.getOptimizeGroupName());
    }
    options.put("sink.parallelism", arcticParameters.getSinkParallelism() + "");
    options.put("write.upsert.enabled", "true");
  }
}
