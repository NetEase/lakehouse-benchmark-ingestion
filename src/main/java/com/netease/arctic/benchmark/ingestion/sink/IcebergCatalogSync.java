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

import com.netease.arctic.benchmark.ingestion.BaseCatalogSync;
import com.netease.arctic.benchmark.ingestion.SyncDbFunction;
import com.netease.arctic.benchmark.ingestion.params.catalog.CatalogParams;
import com.netease.arctic.benchmark.ingestion.params.database.BaseParameters;
import com.netease.arctic.benchmark.ingestion.params.table.IcebergParameters;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Iceberg synchronize implementation of {@link BaseCatalogSync}，which customised operations for
 * building tables
 */
public class IcebergCatalogSync extends BaseCatalogSync {

  private final IcebergParameters icebergParameters;

  public IcebergCatalogSync(BaseParameters baseParameters, IcebergParameters icebergParameters) {
    super(baseParameters);
    this.icebergParameters = icebergParameters;
  }

  @Override
  public void insertData(StreamTableEnvironment tableEnv, SingleOutputStreamOperator<Void> process,
      CatalogParams sourceCatalogParams, CatalogParams destCatalogParams,
      List<Tuple2<ObjectPath, ResolvedCatalogTable>> s) {
    FlinkCatalog catalog = (FlinkCatalog) destCatalogParams.getCatalog();
    CatalogLoader catalogLoader = getField(FlinkCatalog.class, catalog, "catalogLoader");
    org.apache.iceberg.catalog.Catalog loadCatalog = catalogLoader.loadCatalog();
    SyncDbFunction.getParamsList(sourceCatalogParams.getDatabaseName(), s).forEach(p -> {
      TableIdentifier identifier =
          TableIdentifier.of(Namespace.of(destCatalogParams.getDatabaseName()), p.getTable());
      Table table = loadCatalog.loadTable(identifier);
      TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);

      FlinkSink.forRowData(process.getSideOutput(p.getTag())).table(table).tableLoader(tableLoader)
          .writeParallelism(4).append();

    });
  }

  @Override
  public void createTable(Catalog catalog, String dbName,
      List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {
    Catalog iceberg = catalog;
    boolean isDataBaseExist = iceberg.databaseExists(dbName);
    if (!isDataBaseExist) {
      try {
        iceberg.createDatabase(dbName, new CatalogDatabaseImpl(new HashMap<>(), "new db"), false);
      } catch (DatabaseAlreadyExistException e) {
        e.printStackTrace();
      }
    }

    pathAndTable.forEach(e -> {
      try {
        final Map<String, String> options = new HashMap<>();
        fillIcebergTableOptions(options);
        ObjectPath objectPath = new ObjectPath(dbName, e.f0.getObjectName());

        if (iceberg.tableExists(objectPath) ||
            (isDataBaseExist && objectPath.getObjectName().equals("history"))) {
          iceberg.dropTable(objectPath, true);
        }
        iceberg.createTable(objectPath,
            new ResolvedCatalogTable(e.f1.copy(options), e.f1.getResolvedSchema()), false);
      } catch (TableAlreadyExistException ex) {
        ex.printStackTrace();
      } catch (DatabaseNotExistException ex) {
        ex.printStackTrace();
      } catch (TableNotExistException ex) {
        throw new RuntimeException(ex);
      }
    });
  }

  private void fillIcebergTableOptions(Map<String, String> options) {
    options.put("format-version", "2");
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
