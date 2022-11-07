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

public class HudiConfigOptions {

  public static final String TYPE = "hudi";
  public static final ConfigOption<String> HUDI_CATALOG_PATH =
      ConfigOptions.key("hudi.catalog.path").stringType().noDefaultValue();
  public static final ConfigOption<String> HUDI_HIVE_METASTORE_URL =
      ConfigOptions.key("hudi.hive_sync.metastore.uris").stringType().noDefaultValue();
  public static final ConfigOption<String> HUDI_TABLE_TYPE =
      ConfigOptions.key("hudi.table.type").stringType().defaultValue("MERGE_ON_READ");
  public static final ConfigOption<Integer> HUDI_READ_TASKS =
      ConfigOptions.key("hudi.read.tasks").intType().defaultValue(2);
  public static final ConfigOption<Integer> HUDI_WRITE_TASKS =
      ConfigOptions.key("hudi.write.tasks").intType().defaultValue(2);
  public static final ConfigOption<Integer> HUDI_COMPACTION_TASKS =
      ConfigOptions.key("hudi.compaction.tasks").intType().defaultValue(2);
  public static final ConfigOption<String> HUDI_COMPACTION_TRIGGER_STRATEGY = ConfigOptions
      .key("hudi.compaction.trigger.strategy").stringType().defaultValue("num_or_time");


  public static void fillRequiredOptions(Map<String, String> props) {
    props.put("type", TYPE);
    props.put("default-database", "hudi_default_database");
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
    catalogPropertyKeys.add("hudi.catalog.path");
    return catalogPropertyKeys;
  }
}
