# 总览
欢迎使用eduard，eduard是网易开源的数据湖性能基准测试data-lake-benchmark下的数据同步工具，能够将数据库中的数据实时同步到常见数据湖format的表中。


## 快速开始
1. 下载项目代码 `git clone xxx`
2. 修改数据库与数据湖表的连接信息，可参考resource/arctic_catalog.sql
3. 通过命令`mvn clean install -DskipTests `编译项目
4. 在Flink目录下，新建userLib目录，并将eduard/target目录下的eduard-1.0-SNAPSHOT.jar拷贝至$FLINK_HOME/userLib下
5. 进入Flink目录，通过`bin/start-cluster.sh`命令启动standalone模式下的Flink集群
6. 通过`bin/flink run  --detached ./userlib/eduard-1.0-SNAPSHOT.jar`启动数据同步工具
7. 通过`localhost:8081`打开Flink Web UI，观察数据同步的情况

## 支持的参数
| 参数项                        | 是否必须 | 默认值     | 描述                                                    |
|----------------------------|------|---------|-------------------------------------------------------|
| source.table.name          | 否    | *       | 指定需要同步的表名称，支持逗号分割，默认情况下同步整个数据库                        |
| source.scan.startup.mode   | 否    | initial | MySQL CDC消费binlog时的启动模式，支持initial/latest-offset       |
| sink.type                  | 是    | (none)  | 目标端的数据湖format类型，支持Arctic/Iceberg/Hudi                 |
| arctic.optimize.group.name | 否    | (none)  | 仅当数据湖format设定为Arctic时生效，用于指定Arctic optimze group name |
| source.parallelism         | 否    | 4       | 读取source端数据的任务并行度                                     |
| side.output.parallelism    | 否    | 4       | 对source端数据做分流操作的任务并行度                                 |

## 已支持的数据库与数据湖format
### 源端数据库
1. mysql
### 目标端数据湖format
1. Arctic
2. Iceberg
3. Hudi
