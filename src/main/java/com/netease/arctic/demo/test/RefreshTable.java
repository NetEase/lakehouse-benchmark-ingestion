package com.netease.arctic.demo.test;

import org.apache.beam.sdk.options.Default;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import javax.print.DocFlavor;
import java.util.*;

import static java.util.stream.Collectors.toList;

public class RefreshTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(15000);
        env.setParallelism(8);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.dynamic-table-options.enabled", "true");
        String DEST_DB = "hudi.hudi100wstatic";

        final String[] destCatalogDb = DEST_DB.split("\\.");
        final String hudiCatalogName = destCatalogDb[0];

        final Catalog hudi = tableEnv.getCatalog(hudiCatalogName).orElseThrow(() -> new RuntimeException(hudiCatalogName + " catalog not exists"));
        final String hudiDb = destCatalogDb[1];

        String[] strs = {"order_line", "new_order","stock"};
        List<String> tableNameList = Arrays.asList(strs);
        createHudiTable(hudi, hudiDb, tableNameList);

    }

    private static void createHudiTable(final Catalog hudi, final String hudiDb, List<String> tableNameList) {
        if (hudi.databaseExists(hudiDb)) {
            try {
                hudi.createDatabase(hudiDb, new CatalogDatabaseImpl(new HashMap<>(), "new db"), false);
            } catch (DatabaseAlreadyExistException e) {
                e.printStackTrace();
            }
        }

        for(String tbName:tableNameList){
            try {
                // TODO: read external options hoodie-catalog.yml using context.getEnv().getCachedFiles()

                ObjectPath objectPath = new ObjectPath(hudiDb, tbName);

                if (hudi.tableExists(objectPath)) {
                    hudi.dropTable(objectPath, true);
                }
//                hudi.createTable(objectPath, new ResolvedCatalogTable(e.f1.copy(options), e.f1.getResolvedSchema()), true);
            } catch (TableNotExistException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
