package com.netease.arctic.demo.sink;

import com.netease.arctic.demo.BaseEduardCatalog;
import com.netease.arctic.demo.entity.CatalogParam;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.netease.arctic.demo.SyncDbFunction.getParamsList;

public class HudiCatalogSync extends BaseEduardCatalog {
    @Override
    public void createTable(Catalog catalog, String dbName, List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {
        Catalog hudi = catalog;
        if (!hudi.databaseExists(dbName)) {
            try {
                hudi.createDatabase(dbName, new CatalogDatabaseImpl(new HashMap<>(), "new db"), false);
            } catch (DatabaseAlreadyExistException e) {
                e.printStackTrace();
            }
        }

        pathAndTable.forEach(e -> {
            try {
                // TODO: read external options hoodie-catalog.yml using context.getEnv().getCachedFiles()
                final Map<String, String> options = new HashMap<>();
                options.put("hive_sync.support_timestamp","true");
//                options.put("hoodie.datasource.hive_sync.support_timestamp","true");
                options.put("hive_sync.enable", "true");
                options.put("hive_sync.mode", "hms");
                options.put("hive_sync.metastore.uris", "thrift://hz11-trino-arctic-0.jd.163.org:9083");
                options.put("hive_sync.db", dbName);
                options.put("hive_sync.table", e.f0.getObjectName());


                options.put("table.type", "MERGE_ON_READ");
                options.put("read.tasks", "8");
                options.put("write.bucket_assign.tasks", "8");
                options.put("write.tasks", "8");
                options.put("write.batch.size", "128");
//                options.put("write.log_block.size", "1");
//                options.put("compaction.async.enabled","false");
//                options.put("compaction.schedule.enabled","true");
//                options.put("clean.async.enabled","true");

                options.put("compaction.tasks", "8");
                options.put("compaction.trigger.strategy", "num_or_time");
                options.put("compaction.delta_commits", "3");
                options.put("compaction.delta_seconds", "180");
                options.put("compaction.max_memory", "1024");
                options.put("hoodie.embed.timeline.server", "false");

//                options.put("hoodie.clean.async", "true");
//                options.put("hoodie.cleaner.parallelism", "2");
                ObjectPath objectPath = new ObjectPath(dbName, e.f0.getObjectName());

                if (hudi.tableExists(objectPath)) {
                    hudi.dropTable(objectPath, true);
                }
                hudi.createTable(objectPath, new ResolvedCatalogTable(e.f1.copy(options), e.f1.getResolvedSchema()), true);
            } catch (TableAlreadyExistException ex) {
                ex.printStackTrace();
            } catch (DatabaseNotExistException ex) {
                ex.printStackTrace();
            } catch (TableNotExistException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    @Override
    public void insertData(StreamTableEnvironment tEnv, SingleOutputStreamOperator<Void> process, CatalogParam sourceCatalogParam, CatalogParam destCatalogParam, List<Tuple2<ObjectPath, ResolvedCatalogTable>> s) {
        final StatementSet set = tEnv.createStatementSet();
        getParamsList(sourceCatalogParam.getDataBaseName(), s).forEach(p -> {
            tEnv.createTemporaryView(p.getTable(), process.getSideOutput(p.getTag()), p.getSchema());
            String sql = String.format("INSERT INTO %s.%s.%s SELECT f0.* FROM %s", destCatalogParam.getCatalogName(), destCatalogParam.getDataBaseName(), p.getPath().getObjectName(), p.getTable());
            set.addInsertSql(sql);
        });
        set.execute();
    }
}
