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

import com.netease.arctic.benchmark.ingestion.BaseCatalogSync;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * {@link ConfigOption}s for {@link BaseCatalogSync}.
 */
public class BaseConfigOptions {
  public static final ConfigOption<String> SOURCE_TYPE =
      ConfigOptions.key("source.type").stringType().noDefaultValue();

  public static final ConfigOption<String> SOURCE_DATABASE_NAME =
      ConfigOptions.key("source.database.name").stringType().noDefaultValue();

  public static final ConfigOption<String> SOURCE_USERNAME =
      ConfigOptions.key("source.username").stringType().noDefaultValue();

  public static final ConfigOption<String> SOURCE_PASSWORD =
      ConfigOptions.key("source.password").stringType().noDefaultValue();

  public static final ConfigOption<String> SOURCE_HOSTNAME =
      ConfigOptions.key("source.hostname").stringType().noDefaultValue();

  public static final ConfigOption<String> SOURCE_PORT =
      ConfigOptions.key("source.port").stringType().noDefaultValue();

  public static final ConfigOption<String> SOURCE_TABLE_NAME =
      ConfigOptions.key("source.table.name").stringType().defaultValue("*");

  public static final ConfigOption<String> SOURCE_SCAN_STARTUP_MODE =
      ConfigOptions.key("source.scan.startup.mode").stringType().defaultValue("initial");

  public static final ConfigOption<String> SOURCE_SERVER_TIME_ZONE =
      ConfigOptions.key("source.server.timezone").stringType().defaultValue("");

  public static final ConfigOption<Integer> SOURCE_PARALLELISM =
      ConfigOptions.key("source.parallelism").intType().defaultValue(4);
}
