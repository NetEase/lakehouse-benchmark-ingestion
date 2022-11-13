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

package com.netease.arctic.benchmark.ingestion.params.table;

import com.netease.arctic.benchmark.ingestion.config.HudiConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

/**
 * A utility class helps parse and manage Hudi table parameters that are used for create hudi tables
 */
public class HudiParameters {

  protected final Configuration eduardConfig;

  public HudiParameters(Configuration eduardConfig) {
    this.eduardConfig = Preconditions.checkNotNull(eduardConfig);
  }

  public boolean getHiveSyncEnable() {
    return eduardConfig.getBoolean(HudiConfigOptions.HUDI_HIVE_SYNC_ENABLE);
  }

  public String getHiveMetastoreUri() {
    return eduardConfig.getString(HudiConfigOptions.HUDI_HIVE_METASTORE_URL);
  }

  public String getTableType() {
    return eduardConfig.getString(HudiConfigOptions.HUDI_TABLE_TYPE);
  }

  public int getReadTasks() {
    return eduardConfig.getInteger(HudiConfigOptions.HUDI_READ_TASKS);
  }

  public int getWriteTasks() {
    return eduardConfig.getInteger(HudiConfigOptions.HUDI_WRITE_TASKS);
  }

  public int getCompactionTasks() {
    return eduardConfig.getInteger(HudiConfigOptions.HUDI_COMPACTION_TASKS);
  }

  public String getCompactionStrategy() {
    return eduardConfig.getString(HudiConfigOptions.HUDI_COMPACTION_TRIGGER_STRATEGY);
  }
}
