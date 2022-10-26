package com.netease.arctic.demo.sink;

import com.netease.arctic.demo.BaseEduardCatalog;
import com.netease.arctic.demo.entity.CatalogParam;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;

import java.util.List;

public class IcebergCatalogSync extends BaseEduardCatalog {
    @Override
    public void createTable(Catalog catalog, String dbName, List<Tuple2<ObjectPath, ResolvedCatalogTable>> pathAndTable) {

    }

    @Override
    public void insertData(StreamTableEnvironment tEnv, SingleOutputStreamOperator<Void> process, CatalogParam sourceCatalogParam, CatalogParam destCatalogParam, List<Tuple2<ObjectPath, ResolvedCatalogTable>> s) {

    }
}
