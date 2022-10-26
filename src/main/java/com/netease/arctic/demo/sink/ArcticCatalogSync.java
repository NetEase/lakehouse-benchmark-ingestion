package com.netease.arctic.demo.sink;

import com.netease.arctic.demo.BaseEduardCatalog;
import com.netease.arctic.demo.entity.CatalogParam;
import com.netease.arctic.flink.InternalCatalogBuilder;
import com.netease.arctic.flink.catalog.ArcticCatalog;
import com.netease.arctic.flink.table.ArcticTableLoader;
import com.netease.arctic.flink.util.ArcticUtils;
import com.netease.arctic.flink.write.FlinkSink;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableIdentifier;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
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

public class ArcticCatalogSync extends BaseEduardCatalog {
    @Override
    public void createTable(Catalog catalog, String dbName, List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {
        Catalog arctic = catalog;
        if (!arctic.databaseExists(dbName)) {
            try {
                arctic.createDatabase(dbName, new CatalogDatabaseImpl(new HashMap<>(), "new db"), false);
            } catch (DatabaseAlreadyExistException e) {
                e.printStackTrace();
            }
        }

        pathAndTable.forEach(e -> {
            try {
                // TODO: read external options hoodie-catalog.yml using context.getEnv().getCachedFiles()
                final Map<String, String> options = new HashMap<>();
                ObjectPath objectPath = new ObjectPath(dbName, e.f0.getObjectName());

                if (arctic.tableExists(objectPath)) {
                    arctic.dropTable(objectPath, true);
                }
                arctic.createTable(objectPath, new ResolvedCatalogTable(e.f1.copy(options), e.f1.getResolvedSchema()), true);
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
