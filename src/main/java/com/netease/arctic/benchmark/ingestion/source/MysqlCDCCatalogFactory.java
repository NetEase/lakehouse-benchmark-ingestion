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

package com.netease.arctic.benchmark.ingestion.source;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashSet;
import java.util.Set;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.HOSTNAME;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.PASSWORD;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.PORT;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.USERNAME;
import static org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactoryOptions.DEFAULT_DATABASE;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

/**
 * Factory for {@link MysqlCDCCatalog}.
 */
public class MysqlCDCCatalogFactory implements CatalogFactory {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlCDCCatalogFactory.class);

  @Override
  public String factoryIdentifier() {
    return "mysql-cdc";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(HOSTNAME);
    options.add(USERNAME);
    options.add(PASSWORD);
    options.add(DEFAULT_DATABASE);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(PROPERTY_VERSION);
    options.add(PORT);
    return options;
  }

  @Override
  public Catalog createCatalog(Context context) {
    final FactoryUtil.CatalogFactoryHelper helper =
        FactoryUtil.createCatalogFactoryHelper(this, context);
    helper.validate();

    return new MysqlCDCCatalog(context.getName(), helper.getOptions().get(DEFAULT_DATABASE),
        helper.getOptions().get(USERNAME), helper.getOptions().get(PASSWORD),
        helper.getOptions().get(HOSTNAME), helper.getOptions().get(PORT));
  }
}
