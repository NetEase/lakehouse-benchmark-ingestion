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

import com.netease.arctic.benchmark.ingestion.config.ArcticConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import java.util.Collections;
import java.util.Map;

/**
 * A utility class helps parse and manage Arctic table parameters that are used for create Arctic
 * tables
 */
public class ArcticParameters {

  protected final Configuration eduardConfig;

  public ArcticParameters(Configuration eduardConfig) {
    this.eduardConfig = Preconditions.checkNotNull(eduardConfig);
  }

  public String getMetastoreURL() {
    return eduardConfig.getString(ArcticConfigOptions.ARCTIC_METASTORE_URL);
  }

  public boolean getOptimizeEnable() {
    return eduardConfig.getBoolean(ArcticConfigOptions.ARCTIC_OPTIMIZE_ENABLE);
  }

  public String getOptimizeGroupName() {
    return eduardConfig.getString(ArcticConfigOptions.ARCTIC_OPTIMIZE_GROUP_NAME);
  }

  public Map<String, String> getOptimizeTableQuota() {
    return eduardConfig.getOptional(ArcticConfigOptions.ARCTIC_OPTIMIZE_TABLE_QUOTA)
        .orElse(Collections.emptyMap());
  }

  public boolean getWriteUpsertEnable() {
    return eduardConfig.getBoolean(ArcticConfigOptions.ARCTIC_WRITE_UPSERT_ENABLE);
  }

  public int getSinkParallelism() {
    return eduardConfig.getInteger(ArcticConfigOptions.ARCTIC_SINK_PARALLELISM);
  }
}
