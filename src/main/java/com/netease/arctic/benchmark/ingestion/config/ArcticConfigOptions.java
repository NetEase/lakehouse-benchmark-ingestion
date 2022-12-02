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

import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@link ConfigOption}s for {@link com.netease.arctic.benchmark.ingestion.sink.ArcticCatalogSync}.
 */
public class ArcticConfigOptions {

  public static final String TYPE = "arctic";

  public static final ConfigOption<String> ARCTIC_METASTORE_URL =
      ConfigOptions.key("arctic.metastore.url").stringType().noDefaultValue();

  public static final ConfigOption<Boolean> ARCTIC_OPTIMIZE_ENABLE =
      ConfigOptions.key("arctic.optimize.enable").booleanType().defaultValue(true);

  public static final ConfigOption<String> ARCTIC_OPTIMIZE_GROUP_NAME =
      ConfigOptions.key("arctic.optimize.group.name").stringType().defaultValue("default");

  public static final ConfigOption<Map<String, String>> ARCTIC_OPTIMIZE_TABLE_QUOTA =
      ConfigOptions.key("arctic.optimize.table.quota").mapType().noDefaultValue();

  public static final ConfigOption<Boolean> ARCTIC_WRITE_UPSERT_ENABLE =
      ConfigOptions.key("arctic.write.upsert.enable").booleanType().defaultValue(false);

  public static final ConfigOption<Integer> ARCTIC_SINK_PARALLELISM =
      ConfigOptions.key("arctic.sink.parallelism").intType().defaultValue(4);

  public static void fillRequiredOptions(Map<String, String> props) {
    props.put("type", TYPE);
  }

  public static Map<String, String> filterCatalogOptions(Map<String, String> props) {
    Map<String, String> catalogProps = new HashedMap();
    List<String> catalogPropertyKeys = getCatalogPropertyKeys();
    for (String key : props.keySet()) {
      if (catalogPropertyKeys.contains(key)) {
        catalogProps.put(key, props.get(key));
      }
    }
    return catalogProps;
  }

  private static List<String> getCatalogPropertyKeys() {
    List<String> catalogPropertyKeys = new ArrayList<>();
    catalogPropertyKeys.add("arctic.metastore.url");
    return catalogPropertyKeys;
  }

}
