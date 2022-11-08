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

package com.netease.arctic.benchmark.ingestion;

import com.netease.arctic.benchmark.ingestion.config.CatalogConfigUtil;
import com.netease.arctic.benchmark.ingestion.params.BaseParameters;
import com.netease.arctic.benchmark.ingestion.params.CallContext;
import com.netease.arctic.benchmark.ingestion.params.ParameterUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateCatalogOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MainRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MainRunner.class);
  private static StreamExecutionEnvironment env;
  private static StreamTableEnvironment tableEnv;
  public static final String EDUARD_CONF_FILENAME = "conf/eduard-conf.yaml";

  public static void main(String[] args)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Class.forName("com.mysql.jdbc.Driver");
    System.setProperty("HADOOP_USER_NAME", "sloth");

    Map<String, String> props = new HashMap<>();
    Configuration configuration = loadYAMLResource(
        MainRunner.class.getClassLoader().getResourceAsStream(EDUARD_CONF_FILENAME), props);
    BaseParameters baseParameters = new BaseParameters(configuration);

    env = StreamExecutionEnvironment.getExecutionEnvironment(setFlinkConf());
    tableEnv = StreamTableEnvironment.create(env);
    createSourceCatalog(baseParameters.getSourceType(), baseParameters);
    createSinkCatalog(baseParameters.getSinkType(), props);
    call(baseParameters.getSinkType(), configuration, CallContext.builder()
        .args(ParameterTool.fromArgs(args)).env(env).tableEnv(tableEnv).build());
  }

  private static void call(String sinkType, Configuration configuration, final CallContext context)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    final String prefix = "com.netease.arctic.benchmark.ingestion.sink.";
    final String suffix = "CatalogSync";
    Class<?> classz = Class.forName(prefix + sinkType + suffix);
    sinkType = sinkType.toLowerCase();
    Constructor<?> constructor;
    try {
      constructor =
          classz.getConstructor(BaseParameters.class, ParameterUtil.getParamsClass(sinkType));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
    try {
      ((Consumer<CallContext>) constructor.newInstance(new BaseParameters(configuration),
          ParameterUtil.getParamsClass(sinkType).getConstructor(Configuration.class)
              .newInstance(configuration))).accept(context);
    } catch (InvocationTargetException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  private static void createSourceCatalog(String sourceType, BaseParameters baseParameters) {
    sourceType = sourceType.toLowerCase();
    String prefix = "source." + sourceType;
    String catalogName = sourceType + "_catalog";
    Map<String, String> sourceProps = new HashMap<>();
    getSourceCatalogProps(baseParameters, sourceProps);
    Operation operation = new CreateCatalogOperation(catalogName, sourceProps);
    ((StreamTableEnvironmentImpl) tableEnv).executeInternal(operation);
  }

  private static void createSinkCatalog(String sinkType, Map<String, String> props) {
    sinkType = sinkType.toLowerCase();
    String catalogName = sinkType + "_catalog_ignore";
    Map<String, String> sinkProps = new HashMap<>();
    CatalogConfigUtil.getSinkCatalogProps(sinkType, sinkProps);
    for (String key : CatalogConfigUtil.filterCatalogParams(sinkType, props).keySet()) {
      if (key.startsWith(sinkType)) {
        sinkProps.put(key.substring(sinkType.length() + 1), props.get(key));
      }
    }
    Operation operation = new CreateCatalogOperation(catalogName, sinkProps);
    ((StreamTableEnvironmentImpl) tableEnv).executeInternal(operation);
  }

  private static Configuration loadYAMLResource(InputStream inputStream,
      Map<String, String> props) {
    final Configuration config = new Configuration();

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

      String line;
      int lineNo = 0;
      while ((line = reader.readLine()) != null) {
        lineNo++;
        // 1. check for comments
        String[] comments = line.split("#", 2);
        String conf = comments[0].trim();

        // 2. get key and value
        if (conf.length() > 0) {
          String[] kv = conf.split(": ", 2);

          // skip line with no valid key-value pair
          if (kv.length == 1) {
            LOG.warn("Error while trying to split key and value in configuration file " +
                EDUARD_CONF_FILENAME + ":" + lineNo + ": \"" + line + "\"");
            continue;
          }

          String key = kv[0].trim();
          String value = kv[1].trim();

          // sanity check
          if (key.length() == 0 || value.length() == 0) {
            LOG.warn("Error after splitting key and value in configuration file " +
                EDUARD_CONF_FILENAME + ":" + lineNo + ": \"" + line + "\"");
            continue;
          }

          LOG.info("Loading configuration property: {}, {}", key, value);
          config.setString(key, value);
          props.put(key, value);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error parsing YAML configuration.", e);
    }

    return config;
  }

  private static void getSourceCatalogProps(BaseParameters baseParameters,
      Map<String, String> sourceProps) {
    sourceProps.put("type", "mysql-cdc");
    sourceProps.put("default-database", baseParameters.getSourceDatabaseName());
    sourceProps.put("username", baseParameters.getSourceUserName());
    sourceProps.put("password", baseParameters.getSourcePassword());
    sourceProps.put("hostname", baseParameters.getSourceHostName());
    sourceProps.put("port", baseParameters.getSourcePort());
  }

  private static Configuration setFlinkConf() {
    Configuration configuration = new Configuration();
    configuration.setInteger(RestOptions.PORT, 8081);
    return configuration;
  }
}
