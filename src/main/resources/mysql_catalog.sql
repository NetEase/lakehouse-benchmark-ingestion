set table.exec.resource.default-parallelism=1;
set local.taskmanager.memory.managed.size=1024m;

set execution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION;
set execution.checkpointing.interval=30s;
set execution.checkpointing.mode=AT_LEAST_ONCE;
-- set execution.checkpointing.max-concurrent-checkpoints=1;
set execution.checkpointing.min-pause=100;
set execution.checkpointing.timeout=600s;
set execution.checkpointing.tolerable-failed-checkpoints=100;
-- set execution.checkpointing.tolerable-failed-checkpoints=50;
set state.backend=rocksdb;
-- set state.checkpoint-storage=filesystem;
-- set state.checkpoints.dir='';
-- set state.backend.incremental=true;
-- SET 'execution.savepoint.path' = '/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab';

CREATE CATALOG mysql WITH(
    'type' = 'mysql-cdc',
    'default-database' = 'chbenchmark',
    'username' = 'sys',
    'password' = 'netease',
    'hostname' = '10.171.161.168'
);

CREATE CATALOG hudi WITH(
    'type' = 'hudi',
    'default-database' = 'test',
    'catalog.path' = 'file:///Users/hzmajin/Documents/workspace/hudi_test'
);

CREATE TABLE print WITH('connector'='print') LIKE mysql.test.test(EXCLUDING ALL);

-- INSERT INTO print
-- SELECT * FROM mysql.test.test;
set custom.sync-db.source.db=mysql.chbenchmark;
set custom.sync-db.dest.db=hudi.chbenchmark;
call com.netease.arctic.demo.SyncDbFunction;