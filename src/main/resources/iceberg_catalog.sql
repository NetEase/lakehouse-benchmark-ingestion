--set table.exec.resource.default-parallelism=16;
-- set taskmanager.memory.process.size=10240m;

set execution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION;
set execution.checkpointing.interval=60s;
set execution.checkpointing.mode=EXACTLY_ONCE;
-- set execution.checkpointing.max-concurrent-checkpoints=1;
set execution.checkpointing.min-pause=100;
set execution.checkpointing.timeout=600s;
set execution.checkpointing.tolerable-failed-checkpoints=100;
-- set execution.checkpointing.tolerable-failed-checkpoints=50;
-- set state.backend=rocksdb;
-- set state.checkpoint-storage=filesystem;
-- set state.checkpoints.dir='';
-- set state.backend.incremental=true;
-- SET 'execution.checkpoint.path' = '/Users/yuekelei/Documents/workspace';

CREATE CATALOG mysql_catalog WITH(
    'type' = 'mysql-cdc',
    'default-database' = 'test2w',
    'username' = 'rds',
    'password' = '123456',
    'hostname' = 'sloth-commerce-test2.jd.163.org',
    'port'='3332'
);

CREATE CATALOG iceberg_catalog WITH(
    'type' = 'iceberg',
    'catalog-type' = 'hive',
    'uri'='thrift://hz11-trino-arctic-0.jd.163.org:9083',
    'property-version' = '1',
    'warehouse' = 'hdfs://hz11-trino-arctic-0.jd.163.org:8020/user/warehouse',
    'clients' = '5'
);

set custom.sync-db.source.db=mysql_catalog.test2w;
set custom.sync-db.dest.db=iceberg_catalog.test2w_iceberg8;
call com.netease.arctic.demo.sink.IcebergCatalogSync;