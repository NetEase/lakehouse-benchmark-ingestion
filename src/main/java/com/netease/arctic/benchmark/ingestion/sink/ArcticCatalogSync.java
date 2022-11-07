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
import com.netease.arctic.benchmark.ingestion.params.catalog.ArcticParameters;
import com.netease.arctic.benchmark.ingestion.params.BaseParameters;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.catalog.Catalog;
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
      } catch (TableAlreadyExistException ex) {
        ex.printStackTrace();
      } catch (DatabaseNotExistException ex) {
        ex.printStackTrace();
      } catch (TableNotExistException ex) {
        throw new RuntimeException(ex);
      }
    });
  }

  private void fillArcticTableOptions(Map<String, String> options) {
    options.put("optimize.group", arcticParameters.getOptimizeGroupName());
  }
}
