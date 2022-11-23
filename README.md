# 总览
欢迎使用lakehouse-benchmark-ingestion。lakehouse-benchmark-ingestion 是网易开源的数据湖性能基准测试 lakehouse-benchmark 项目下的数据同步工具，该工具基于 Flink-CDC 实现，能够将数据库中的数据实时同步到数据湖。

## 快速开始
1. 下载项目代码 `git clone https://github.com/NetEase/lakehouse-benchmark-ingestion.git`
2. 参考相关说明部分，构建项目所需的依赖 
3. 通过命令`mvn clean install -DskipTests`编译项目。进入 target 目录，通过`tar -zxvf lakehouse_benchmark_ingestion.tar.gz`命令解压得到 lakehouse-benchmark-ingestion-1.0-SNAPSHOT.jar 和 conf 目录
4. 修改 conf 目录下的 ingestion-conf.yaml ，填写配置项信息 
5. 通过`java -cp lakehouse-benchmark-ingestion-1.0-SNAPSHOT.jar com.netease.arctic.benchmark.ingestion.MainRunner -confDir [confDir] -sinkType [arctic/iceberg/hudi] -sinkDatabase [dbName]`命令启动数据同步工具 
6. 通过`localhost:8081`打开 Flink Web UI ，观察数据同步的情况

## 支持的参数
### 命令行参数

| 参数项          | 是否必须 | 默认值    | 描述                                                                  |
|--------------|------|--------|---------------------------------------------------------------------|
| confDir      | 是    | (none) | 配置文件 ingestion-conf.yaml 所在目录的绝对路径。基于快速开始的步骤，confDir为解压后conf目录所在的路径 |
| sinkType     | 是    | (none) | 目标端数据湖 Format 的类型，支持 Arctic/Iceberg/Hudi                            |
| sinkDatabase | 是    | (none) | 目标端数据库的名称                                                           |
| restPort     | 否    | 8081   | Flink Web UI的端口                                                     |

### 配置文件参数
以下参数均可以通过 conf/ingestion-conf.yaml 文件进行配置。

| 参数项                      | 是否必须 | 默认值     | 描述                                                            |
|--------------------------|------|---------|---------------------------------------------------------------|
| source.type              | 是    | (none)  | 源端数据库的类型，目前仅支持 MySQL                                          |
| source.username          | 是    | (none)  | 源端数据库用户名                                                      |
| source.password          | 是    | (none)  | 源端数据库密码                                                       |
| source.hostname          | 是    | (none)  | 源端数据库地址                                                       |
| source.port              | 是    | (none)  | 源端数据库端口                                                       |
| source.table.name        | 否    | *       | 指定需要同步的表名称，支持指定多张表，默认情况下同步整个数据库                               |
| source.scan.startup.mode | 否    | initial | MySQL CDC connector 消费 binlog 时的启动模式，支持 initial/latest-offset |
| source.parallelism       | 否    | 4       | 读取源端数据时的任务并行度                                                 |      |         |                                                       |

**Arctic相关**

| 参数项                        | 是否必须 | 默认值     | 描述                        |
|----------------------------|------|---------|---------------------------|
| arctic.metastore.url       | 是    | (none)  | Arctic metastore 的 URL 地址 |
| arctic.optimize.group.name | 否    | default | Arctic Optimizer 资源组      |
 
**Iceberg相关**

| 参数项                  | 是否必须 | 默认值    | 描述                                 |
|----------------------|------|--------|------------------------------------|
| iceberg.uri          | 是    | (none) | Hive metastore 的thrift URI         |
| iceberg.warehouse    | 是    | (none) | Hive warehouse 的地址                 |
| iceberg.catalog-type | 否    | hive   | Iceberg catalog 的类型，支持 hive/hadoop |

**Hudi相关**

| 参数项                                   | 是否必须 | 默认值           | 描述                                       |
|---------------------------------------|------|---------------|------------------------------------------|
| hudi.catalog.path                     | 是    | (none)        | Hudi Catalog 的地址                         |
| hudi.hive_sync.enable                 | 否    | true          | 是否开启 hive 同步功能                           |
| hudi.hive_sync.metastore.uris         | 否    | (none)        | Hive Metastore URL，当开启 hive 同步功能时需要填写该参数 |
| hudi.table.type                       | 否    | MERGE_ON_READ | 表操作的类型，支持 MERGE_ON_READ/COPY_ON_WRITE    |
| hudi.read.tasks                       | 否    | 4             | 读算子的并行度                                  |
| hudi.compaction.tasks                 | 否    | 4             | 在线 compaction 的并行度                       |
| hudi.compaction.trigger.strategy      | 否    | num_or_time   | 压缩策略                                     |

## 需要的环境
1. hadoop，

## 已支持的数据库与数据湖Format
### 源端数据库
1. MySQL
### 目标端数据湖Format
1. Arctic
2. Iceberg
3. Hudi

## 相关说明
* 本项目使用的arctic-flink-runtime-1.14依赖需要基于Arctic工程进行源码编译，请下载[Arctic工程](https://github.com/NetEase/arctic)的代码，然后切换到master分支，执行命令`mvn clean install -DskipTests`进行构建
* 本项目使用的hudi-flink1.14-bundle_2.12依赖需要基于Hudi工程进行源码编译，请下载[Hudi工程](https://github.com/apache/hudi)的代码，然后切换到release-0.11.1，执行命令`mvn clean install -DskipTests -Dflink1.14 -Dscala-2.12`进行构建