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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import static com.netease.arctic.benchmark.ingestion.config.HudiConfigOptions.HUDI_CATALOG_PATH;

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

  public static void getSinkCatalogProps(String sinkType, Map<String, String> sinkProps,
      Map<String, String> props) throws URISyntaxException, IOException {
    switch (sinkType) {
      case "arctic":
        ArcticConfigOptions.fillRequiredOptions(sinkProps);
        break;
      case "iceberg":
        IcebergConfigOptions.fillRequiredOptions(sinkProps);
        break;
      case "hudi":
        createWarehouseDir(props.get(HUDI_CATALOG_PATH.key()));
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

  private static void createWarehouseDir(String path) throws URISyntaxException, IOException {
    checkValidatePath(path);
    org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
    if (isLocalPath(path)) {
      FileSystem fileSystem = FileSystem.get(configuration);
      Path localPath = new Path(path);
      LOG.info("Hadoop Directory for warehouse created result: {}", fileSystem.mkdirs(localPath));
    } else {
      String[] urlMsg = parseWarehousePath(path);
      FileSystem fileSystem = FileSystem.get(new URI(urlMsg[0]), configuration);
      Path remotePath = new Path(urlMsg[1]);
      LOG.info("Hadoop Directory for warehouse created result: {}", fileSystem.mkdirs(remotePath));
    }
  }

  private static String[] parseWarehousePath(String path) throws URISyntaxException {
    String[] result = new String[2];
    String port = new URI(path).getPort() + "";
    int portIdx = path.indexOf(port);
    result[0] = path.substring(0, portIdx + port.length());
    result[1] = path.substring(portIdx + port.length());
    return result;
  }

  private static boolean isLocalPath(String path) {
    return !path.startsWith("hdfs://");
  }

  private static void checkValidatePath(String path) {
    if (!(path.startsWith("hdfs://") || path.startsWith("/"))) {
      throw new RuntimeException("The path " + path + " for warehouse is not validate ");
    }
  }

}
