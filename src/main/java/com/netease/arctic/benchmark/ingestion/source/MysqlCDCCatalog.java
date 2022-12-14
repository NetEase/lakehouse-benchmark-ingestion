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

import com.mysql.cj.MysqlType;
import com.ververica.cdc.connectors.mysql.table.MySqlTableSourceFactory;
import org.apache.flink.connector.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.DATABASE_NAME;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.HOSTNAME;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.PASSWORD;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.PORT;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.TABLE_NAME;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.USERNAME;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * Catalog for Mysql.
 */
public class MysqlCDCCatalog extends AbstractJdbcCatalog {

  private static final Set<String> builtinDatabases = new HashSet<String>() {
    {
      add("information_schema");
      add("performance_schema");
      add("sys");
      add("mysql");
    }
  };
  public static final String IDENTIFIER = "mysql-cdc";

  private final String hostname;

  public int getPort() {
    return port;
  }

  private final int port;

  public MysqlCDCCatalog(String catalogName, String defaultDatabase, String username, String pwd,
      final String hostname, final int port) {
    super(catalogName, defaultDatabase, username, pwd,
        String.format("jdbc:mysql://%s:%d", hostname, port));
    this.hostname = hostname;
    this.port = port;
  }

  public static void main(String[] args) throws DatabaseNotExistException, TableNotExistException {
    final MysqlCDCCatalog catalog =
        new MysqlCDCCatalog("mysql", "chbenchmark", "sys", "netease", "10.171.161.168", 3332);
    catalog.listDatabases().forEach(System.out::println);
    catalog.listTables("test").forEach(System.out::println);
    System.out.println(catalog.getTable(new ObjectPath("test", "test")).getOptions());
  }

  @Override
  public Optional<Factory> getFactory() {
    return Optional.of(new MySqlTableSourceFactory());
  }
  // ------ databases ------

  @Override
  public List<String> listDatabases() throws CatalogException {
    List<String> mysqlDatabases = new ArrayList<>();

    try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {

      PreparedStatement ps = conn.prepareStatement("show databases");

      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        String dbName = rs.getString(1);
        if (!builtinDatabases.contains(dbName)) {
          mysqlDatabases.add(rs.getString(1));
        }
      }

      return mysqlDatabases;
    } catch (Exception e) {
      throw new CatalogException(String.format("Failed listing database in catalog %s", getName()),
          e);
    }
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    if (listDatabases().contains(databaseName)) {
      return new CatalogDatabaseImpl(Collections.emptyMap(), null);
    } else {
      throw new DatabaseNotExistException(getName(), databaseName);
    }
  }

  // ------ tables ------

  @Override
  public List<String> listTables(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    if (!databaseExists(databaseName)) {
      throw new DatabaseNotExistException(getName(), databaseName);
    }

    // get all schemas
    try (Connection conn = DriverManager.getConnection(baseUrl + databaseName, username, pwd)) {
      PreparedStatement ps = conn.prepareStatement("show tables");

      ResultSet rs = ps.executeQuery();

      List<String> tables = new ArrayList<>();

      while (rs.next()) {
        tables.add(rs.getString(1));
      }

      return tables;
    } catch (Exception e) {
      throw new CatalogException(String.format("Failed listing database in catalog %s", getName()),
          e);
    }
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    if (!tableExists(tablePath)) {
      throw new TableNotExistException(getName(), tablePath);
    }

    String dbUrl = baseUrl + tablePath.getDatabaseName();
    try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd)) {
      DatabaseMetaData metaData = conn.getMetaData();
      final DatabaseMetaData infoSchema = metaData;
      final ResultSet rs = infoSchema.getColumns(tablePath.getDatabaseName(),
          tablePath.getDatabaseName(), tablePath.getObjectName(), null);
      final ResultSet primaryKeys = infoSchema.getPrimaryKeys(tablePath.getDatabaseName(),
          tablePath.getDatabaseName(), tablePath.getObjectName());
      final List<String> pkFields = new ArrayList<>();
      while (primaryKeys.next()) {
        pkFields.add(primaryKeys.getString("COLUMN_NAME"));
      }

      final Schema.Builder builder = Schema.newBuilder();
      List<Column> cols = new ArrayList<>();
      while (rs.next()) {
        final String columnName = rs.getString("COLUMN_NAME");
        final String typeName = rs.getString("TYPE_NAME");
        final AbstractDataType<?> dataType;
        if (pkFields.contains(columnName)) {
          dataType = getDataTypePrimary(typeName);
        } else {
          dataType = getDataType(typeName);
        }
        builder.column(columnName, dataType);
        cols.add(Column.physical(columnName, (DataType) dataType));
      }
      rs.close();

      builder.primaryKey(pkFields);
      primaryKeys.close();

      Map<String, String> props = makeCdcProps(tablePath);

      final Schema schema = builder.build();
      final Schema.UnresolvedPrimaryKey unresolvedPrimaryKey =
          schema.getPrimaryKey().orElseThrow(() -> new RuntimeException(tablePath + "no pk "));
      final ResolvedSchema resolvedSchema =
          new ResolvedSchema(cols, Collections.emptyList(), UniqueConstraint.primaryKey(
              unresolvedPrimaryKey.getConstraintName(), unresolvedPrimaryKey.getColumnNames()));
      return new ResolvedCatalogTable(CatalogTable.of(schema, "", Collections.emptyList(), props),
          resolvedSchema);
    } catch (Exception e) {
      throw new CatalogException(String.format("Failed getting table %s", tablePath.getFullName()),
          e);
    }
  }

  private Map<String, String> makeCdcProps(final ObjectPath tablePath) {
    Map<String, String> props = new HashMap<>();
    props.put(CONNECTOR.key(), IDENTIFIER);
    props.put(HOSTNAME.key(), hostname);
    props.put(DATABASE_NAME.key(), tablePath.getDatabaseName());
    props.put(TABLE_NAME.key(), tablePath.getObjectName());
    props.put(USERNAME.key(), username);
    props.put(PASSWORD.key(), pwd);
    props.put(PORT.key(), String.valueOf(port));
    return props;
  }

  private AbstractDataType<?> getDataType(final String typeName) {
    final MysqlType mysqlType = MysqlType.getByName(typeName);
    switch (mysqlType) {
      case BIGINT:
        return DataTypes.BIGINT();
      case INT:
        return DataTypes.INT();
      case TIMESTAMP:
        return DataTypes.TIMESTAMP(6);
      case VARCHAR:
        return DataTypes.STRING();
      case DECIMAL:
        return DataTypes.DECIMAL(16, 8);
      case CHAR:
        return DataTypes.CHAR(Integer.MAX_VALUE);
      case FLOAT:
        return DataTypes.FLOAT();
      default:
        throw new RuntimeException("unsupport type" + typeName);
    }
  }

  private AbstractDataType<?> getDataTypePrimary(final String typeName) {
    final MysqlType mysqlType = MysqlType.getByName(typeName);
    switch (mysqlType) {
      case BIGINT:
        return DataTypes.BIGINT().notNull();
      case INT:
        return DataTypes.INT().notNull();
      case TIMESTAMP:
        return DataTypes.TIMESTAMP(6);
      case VARCHAR:
        return DataTypes.STRING();
      case DECIMAL:
        return DataTypes.DECIMAL(16, 8);
      case CHAR:
        return DataTypes.CHAR(Integer.MAX_VALUE);
      case FLOAT:
        return DataTypes.FLOAT();
      default:
        throw new RuntimeException("unsupport type" + typeName);
    }
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {

    List<String> tables;
    try {
      tables = listTables(tablePath.getDatabaseName());
    } catch (DatabaseNotExistException e) {
      return false;
    }

    return tables.contains(tablePath.getObjectName());
  }

  public String getHostname() {
    return hostname;
  }

}
