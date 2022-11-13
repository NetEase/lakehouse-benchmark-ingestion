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

package com.netease.arctic.benchmark.ingestion.config;

import com.netease.arctic.benchmark.ingestion.params.database.BaseParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * An util class of catalog config properties manager
 */
public class CatalogConfigUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogConfigUtil.class);

  public static void getSourceCatalogProps(BaseParameters baseParameters,
      Map<String, String> sourceProps) {
    sourceProps.put("type", "mysql-cdc");
    sourceProps.put("default-database", baseParameters.getSourceDatabaseName());
    sourceProps.put("username", baseParameters.getSourceUserName());
    sourceProps.put("password", baseParameters.getSourcePassword());
    sourceProps.put("hostname", baseParameters.getSourceHostName());
    sourceProps.put("port", baseParameters.getSourcePort());
  }

  public static void getSinkCatalogProps(String sinkType, Map<String, String> sinkProps) {
    switch (sinkType) {
      case "arctic":
        ArcticConfigOptions.fillRequiredOptions(sinkProps);
        break;
      case "iceberg":
        IcebergConfigOptions.fillRequiredOptions(sinkProps);
        break;
      case "hudi":
        HudiConfigOptions.fillRequiredOptions(sinkProps);
        break;
      default:
        LOG.error("sinkType:{} is not supported now", sinkType);
    }
  }

  public static Map<String, String> filterCatalogParams(String sinkType,
      Map<String, String> props) {
    switch (sinkType) {
      case "arctic":
        return ArcticConfigOptions.filterCatalogOptions(props);
      case "iceberg":
        return IcebergConfigOptions.filterCatalogOptions(props);
      case "hudi":
        return HudiConfigOptions.filterCatalogOptions(props);
      default:
        LOG.error("sinkType:{} is not supported now", sinkType);
        return null;
    }
  }

}
