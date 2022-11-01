package com.netease.arctic.demo;

import com.netease.arctic.demo.SyncDbFunction.RowDataVoidProcessFunction;
import com.netease.arctic.demo.entity.CallContext;
import com.netease.arctic.demo.entity.CatalogParam;
import com.netease.arctic.demo.source.MysqlCdcCatalog;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.data.RowData;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.function.Consumer;

import static com.netease.arctic.demo.SyncDbFunction.getConverters;
import static com.netease.arctic.demo.SyncDbFunction.getDebeziumDeserializeSchemas;
import static com.netease.arctic.demo.SyncDbFunction.getMySqlSource;
import static com.netease.arctic.demo.SyncDbFunction.getPathAndTable;
import static java.util.stream.Collectors.toList;

public abstract class BaseEduardCatalog implements Consumer<CallContext> {

    private static final ConfigOption<String> SRC_DB = ConfigOptions.key("custom.sync-db.source.db").stringType().noDefaultValue();
    private static final ConfigOption<String> DEST_DB = ConfigOptions.key("custom.sync-db.dest.db").stringType().noDefaultValue();

    private static Logger logger = Logger.getLogger(SyncDbFunction.class);

    @Override
    public void accept(final CallContext context) {
        StreamExecutionEnvironment env = context.getEnv();
        final StreamTableEnvironment tEnv = context.getTEnv();
        final Configuration configuration = tEnv.getConfig().getConfiguration();

        // you can set configuration
        CatalogParam sourceCatalogParam = getSourceCatalogParam(configuration);
        CatalogParam destCatalogParam = getDestCatalogParam(configuration);

        final MysqlCdcCatalog mysql = (MysqlCdcCatalog) getCatalog(tEnv, sourceCatalogParam);

        final Catalog destCatalog = getCatalog(tEnv, destCatalogParam);

        final List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable;
        try {
            pathAndTable = getPathAndTable(mysql, sourceCatalogParam.getDataBaseName());
        } catch (DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }
        List<Tuple2<ObjectPath, ResolvedCatalogTable>> s = pathAndTable.stream().collect(toList());
        try {
            createTable(destCatalog, destCatalogParam.getDataBaseName(), s);
        } catch (DatabaseAlreadyExistException e) {
            throw new RuntimeException(e);
        }

        final MySqlSource<RowData> source;
        try {
            source = getMySqlSource(sourceCatalogParam.getDataBaseName(), mysql.listTables(sourceCatalogParam.getDataBaseName()), mysql, getDebeziumDeserializeSchemas(s));
        } catch (DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }
        final SingleOutputStreamOperator<Void> process = sideOutputHandler(env, source, s);

        insertData(tEnv, process, sourceCatalogParam,
            destCatalogParam, s);
    }

    public Catalog getCatalog(StreamTableEnvironment tEnv, CatalogParam catalogParam) {
        String catalogName = catalogParam.getCatalogName();
        return tEnv.getCatalog(catalogName).orElseThrow(() -> new RuntimeException(catalogName + " catalog not exists"));
    }

    private CatalogParam getSourceCatalogParam(Configuration configuration) {
        String[] srcCatalogParams = configuration.getString(SRC_DB).split("\\.");
        return CatalogParam.builder()
            .catalogName(srcCatalogParams[0])
            .dataBaseName(srcCatalogParams[1])
            .build();
    }

    private CatalogParam getDestCatalogParam(Configuration configuration) {
        String[] srcCatalogParams = configuration.getString(DEST_DB).split("\\.");
        return CatalogParam.builder()
            .catalogName(srcCatalogParams[0])
            .dataBaseName(srcCatalogParams[1])
            .build();
    }

    private SingleOutputStreamOperator<Void> sideOutputHandler(StreamExecutionEnvironment env, MySqlSource<RowData> source, List<Tuple2<ObjectPath, ResolvedCatalogTable>> s) {
        return env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "mysql").uid("mysql").setParallelism(2)
            .process(new RowDataVoidProcessFunction(getConverters(s))).uid("split stream").name("split stream").setParallelism(2);
    }

    public abstract void createTable(Catalog catalog, String dbName, List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) throws DatabaseAlreadyExistException;

    public abstract void insertData(StreamTableEnvironment tEnv, SingleOutputStreamOperator<Void> process, CatalogParam sourceCatalogParam, CatalogParam destCatalogParam, List<Tuple2<ObjectPath, ResolvedCatalogTable>> s);

//    public void insertData(StreamTableEnvironment tEnv,SingleOutputStreamOperator<Void> process,CatalogParam sourceCatalogParam,
//                           CatalogParam destCatalogParam       ,List<Tuple2<ObjectPath, ResolvedCatalogTable>> s) {
//        final StatementSet set = tEnv.createStatementSet();
//        set.execute();
//        getParamsList(sourceCatalogParam.getDataBaseName(), s).forEach(p -> {
//            tEnv.createTemporaryView(p.table, process.getSideOutput(p.tag), p.schema);
//            String sql = String.format("INSERT INTO %s.%s.%s SELECT f0.* FROM %s", destCatalogParam.getCatalogName(), destCatalogParam.getDataBaseName(), p.path.getObjectName(), p.table);
//            set.addInsertSql(sql);
//        });
}
