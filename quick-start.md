# 总览
欢迎使用eduard，eduard是网易开源的数据湖性能基准测试data-lake-benchmark项目下的数据同步工具，该工具基于Flink-CDC实现，能够实现将数据库中的数据实时同步到数据湖的功能。

## 快速开始
1. 下载项目代码 `git clone xxx`
2. 修改resource/eduard.yaml，填写配置项信息
3. 通过命令`mvn clean install -DskipTests `编译项目
4. 在Flink目录下，新建userLib目录，并将eduard/target目录下的eduard-1.0-SNAPSHOT.jar拷贝至$FLINK_HOME/userLib下
5. 进入Flink目录，通过`bin/start-cluster.sh`命令启动standalone模式下的Flink集群
6. 通过`bin/flink run  --detached ./userlib/eduard-1.0-SNAPSHOT.jar`命令启动数据同步工具
7. 通过`localhost:8081`打开Flink Web UI，观察数据同步的情况

## 支持的参数
以下参数均可以通过resource/eduard.yaml文件进行配置

| 参数项                            | 是否必须 | 默认值     | 描述                                              |
|--------------------------------|------|---------|-------------------------------------------------|
| source.username                | 是    | (none)  | 源端数据库用户名                                        |
| source.password                | 是    | (none)  | 源端数据库密码                                         |
| source.hostname                | 是    | (none)  | 源端数据库地址                                         |
| source.password                | 是    | (none)  | 源端数据库端口                                         |
| source.table.name              | 否    | *       | 指定需要同步的表名称，支持指定多张表，默认情况下同步整个数据库                 |
| source.scan.startup.mode       | 否    | initial | MySQL CDC消费binlog时的启动模式，支持initial/latest-offset |
| source.parallelism             | 否    | 4       | 读取源端数据时的任务并行度                                   |      |         |                                                       |
| source.side.output.parallelism | 否    | 4       | 对源端数据做分流操作时的任务并行度                               |      |         |                                                       |
| sink.type                      | 是    | (none)  | 目标端数据湖的format类型，支持Arctic/Iceberg/Hudi           |
| sink.database.name             | 是    | (none)  | 目标端数据库的名称                                       |

### Arctic相关

| 参数项                        | 是否必须 | 默认值    | 描述                  |
|----------------------------|------|--------|---------------------|
| arctic.metastore.url       | 是    | (none) | Arctic数据库URL地址      |
| arctic.optimize.group.name | 否    | (none) | Arctic Optimizer资源组 |
 
### Iceberg相关

| 参数项                  | 是否必须 | 默认值    | 描述                               |
|----------------------|------|--------|----------------------------------|
| iceberg.uri          | 是    | (none) | Hive metastore的thrift URI        |
| iceberg.warehouse    | 是    | (none) | Hive warehouse的地址                |
| iceberg.catalog.type | 否    | hive   | Iceberg catalog的类型，支持hive/hadoop |

### Hudi相关

| 参数项                              | 是否必须 | 默认值           | 描述                                   |
|----------------------------------|------|---------------|--------------------------------------|
| hudi.catalog.path                | 是    | (none)        | Hudi catalog的地址                      |
| hudi.table.type                  | 否    | MERGE_ON_READ | 表操作的类型，支持MERGE_ON_READ/COPY_ON_WRITE |
| hudi.read.tasks                  | 否    | 2             | 读算子的并行度                              |
| hudi.compaction.tasks            | 否    | 2             | 在线 compaction 的并行度                   |
| hudi.compaction.trigger.strategy | 否    | num_or_time   | 压缩策略                                 |



## 已支持的数据库与数据湖format
### 源端数据库
1. MySQL
### 目标端数据湖format
1. Arctic
2. Iceberg
3. Hudi
