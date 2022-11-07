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

package com.netease.arctic.benchmark.ingestion.params;

import com.netease.arctic.benchmark.ingestion.config.BaseConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

public class BaseParameters {

  protected final Configuration eduardConfig;

  public BaseParameters(Configuration eduardConfig) {
    this.eduardConfig = Preconditions.checkNotNull(eduardConfig);
  }

  public Configuration getEduardConfig() {
    return eduardConfig;
  }

  public String getSourceType() {
    return eduardConfig.getString(BaseConfigOptions.SOURCE_TYPE);
  }

  public String getSourceDatabaseName() {
    return eduardConfig.getString(BaseConfigOptions.SOURCE_DATABASE_NAME);
  }

  public String getSourceUserName() {
    return eduardConfig.getString(BaseConfigOptions.SOURCE_USERNAME);
  }

  public String getSourcePassword() {
    return eduardConfig.getString(BaseConfigOptions.SOURCE_PASSWORD);
  }

  public String getSourceHostName() {
    return eduardConfig.getString(BaseConfigOptions.SOURCE_HOSTNAME);
  }

  public String getSourcePort() {
    return eduardConfig.getString(BaseConfigOptions.SOURCE_PORT);
  }

  public String getSourceTableName() {
    return eduardConfig.getString(BaseConfigOptions.SOURCE_TABLE_NAME);
  }

  public String getSourceScanStartupMode() {
    return eduardConfig.getString(BaseConfigOptions.SOURCE_SCAN_STARTUP_MODE);
  }

  public int getSourceParallelism() {
    return eduardConfig.getInteger(BaseConfigOptions.SOURCE_PARALLELISM);
  }

  public String getSinkType() {
    return eduardConfig.getString(BaseConfigOptions.SINK_TYPE);
  }

  public String getSinkDatabaseName() {
    return eduardConfig.getString(BaseConfigOptions.SINK_DATABASE_NAME);
  }

  public int getSinkParallelism() {
    return eduardConfig.getInteger(BaseConfigOptions.SINK_PARALLELISM);
  }

}
