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
import com.netease.arctic.benchmark.ingestion.params.CallContext;
import com.netease.arctic.benchmark.ingestion.params.ParameterUtil;
import com.netease.arctic.benchmark.ingestion.params.database.BaseParameters;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateCatalogOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

/**
 * Start-up class, includes parsing configuration files, creating source and sink catalogs and
 * calling synchronisation functions
 */
@Slf4j
public class MainRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MainRunner.class);
  private static StreamExecutionEnvironment env;
  private static StreamTableEnvironment tableEnv;
  public static final String EDUARD_CONF_FILENAME = "ingestion-conf.yaml";

  public static void main(String[] args)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Class.forName("com.mysql.jdbc.Driver");

    String[] params = parseParams(args);
    String confDir = params[0];
    String sinkType = params[1];
    String sinkDatabase = params[2];
    int restPort = Integer.parseInt(params[3]);
    Map<String, String> props = new HashMap<>();
    Configuration configuration = loadConfiguration(confDir, props);

    BaseParameters baseParameters = new BaseParameters(configuration, sinkType, sinkDatabase);

    env = StreamExecutionEnvironment.getExecutionEnvironment(setFlinkConf(restPort));
    env.setStateBackend(new FsStateBackend("file:///tmp/benchmark-ingestion"));
    env.getCheckpointConfig().setCheckpointInterval(60 * 1000L);
    env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
    tableEnv = StreamTableEnvironment.create(env);
    createSourceCatalog(baseParameters.getSourceType(), baseParameters);
    createSinkCatalog(sinkType, props);
    call(sinkType, sinkDatabase, configuration, CallContext.builder()
        .args(ParameterTool.fromArgs(args)).env(env).tableEnv(tableEnv).build());
    // try {
    // env.execute();
    // } catch (Exception e) {
    // throw new RuntimeException(e);
    // }
  }

  private static void call(String sinkType, String sinkDatabase, Configuration configuration,
      final CallContext context)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    final String prefix = "com.netease.arctic.benchmark.ingestion.sink.";
    final String suffix = "CatalogSync";
    Class<?> classz = Class.forName(prefix + toUpperFirstCase(sinkType) + suffix);
    sinkType = sinkType.toLowerCase();
    Constructor<?> constructor;
    try {
      constructor =
          classz.getConstructor(BaseParameters.class, ParameterUtil.getParamsClass(sinkType));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
    try {
      ((Consumer<CallContext>) constructor.newInstance(
          new BaseParameters(configuration, sinkType, sinkDatabase),
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
    CatalogConfigUtil.getSourceCatalogProps(baseParameters, sourceProps);
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

  private static Configuration loadConfiguration(final String configDir,
      Map<String, String> props) {

    if (configDir == null) {
      throw new IllegalArgumentException(
          "Given configuration directory is null, cannot load configuration");
    }

    final File confDirFile = new File(configDir);
    if (!(confDirFile.exists())) {
      throw new IllegalConfigurationException(
          "The given configuration directory name '" + configDir + "' (" +
              confDirFile.getAbsolutePath() + ") does not describe an existing directory.");
    }

    // get Flink yaml configuration file
    final File yamlConfigFile = new File(confDirFile, EDUARD_CONF_FILENAME);

    if (!yamlConfigFile.exists()) {
      throw new IllegalConfigurationException("The Flink config file '" + yamlConfigFile + "' (" +
          yamlConfigFile.getAbsolutePath() + ") does not exist.");
    }

    Configuration configuration = loadYAMLResource(yamlConfigFile, props);

    return configuration;
  }

  private static Configuration loadYAMLResource(File file, Map<String, String> props) {
    final Configuration config = new Configuration();

    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(Files.newInputStream(file.toPath())))) {

      String line;
      int lineNo = 0;
      while ((line = reader.readLine()) != null) {
        lineNo++;
        String[] comments = line.split("#", 2);
        String conf = comments[0].trim();

        if (conf.length() > 0) {
          String[] kv = conf.split(": ", 2);

          if (kv.length == 1) {
            LOG.warn("Error while trying to split key and value in configuration file " +
                EDUARD_CONF_FILENAME + ":" + lineNo + ": \"" + line + "\"");
            continue;
          }

          String key = kv[0].trim();
          String value = kv[1].trim();

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

  private static Configuration setFlinkConf(int restPort) {
    Configuration configuration = new Configuration();
    configuration.setInteger(RestOptions.PORT, restPort);
    return configuration;
  }

  private static String toUpperFirstCase(String str) {
    return str.substring(0, 1).toUpperCase() + str.substring(1);
  }

  private static String[] parseParams(String[] args) {
    Options options = new Options();
    Option confDir = Option.builder("confDir").required(true).hasArg().argName("confDir")
        .desc("Specify the directory of ingestion-conf yaml").build();
    Option sinkType = Option.builder("sinkType").required(true).hasArg().argName("sinkType")
        .desc("Specify the type of target database").build();
    Option sinkDatabase = Option.builder("sinkDatabase").required(true).hasArg()
        .argName("sinkDatabase").desc("Specify the database name of target database").build();
    Option restPort = Option.builder("restPort").required(false).hasArg().argName("restPort")
        .desc("Specify the port of Flink Web UI").build();
    options.addOption(confDir);
    options.addOption(sinkType);
    options.addOption(sinkDatabase);
    options.addOption(restPort);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }

    String[] params = new String[4];

    if (cmd.hasOption("confDir")) {
      params[0] = cmd.getOptionValue("confDir");
    } else {
      throw new RuntimeException("parse Param 'confDir' fail");
    }
    if (cmd.hasOption("sinkType")) {
      params[1] = cmd.getOptionValue("sinkType");
    } else {
      throw new RuntimeException("parse Param 'sinkType' fail");
    }
    if (cmd.hasOption("sinkDatabase")) {
      params[2] = cmd.getOptionValue("sinkDatabase");
    } else {
      throw new RuntimeException("parse Param 'sinkDatabase' fail");
    }
    if (cmd.hasOption("restPort")) {
      params[3] = cmd.getOptionValue("restPort");
    } else {
      params[3] = "8081";
      LOG.info("No rest port specified, will bind to 8081");
    }
    return params;
  }
}
