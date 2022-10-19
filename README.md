# 基于数栈的trino自定义函数模板

# Installation
1. 将工程在idea中打开，参照工程中的案例或官网文档，开发自己的udxf
   1. 参考案例：https://github.com/wgzhao/presto-udfs https://github.com/archongum/trino-udf.git
   2. 参考官网：https://trino.io/docs/current/develop/functions.html
2. 打包工程，生存对应的jar包
3. 将工程路径下的target目录中的jar包如：`trino-udf-*.jar` 拷贝到trino集群上的每个节点的对应路径 `${TRINO_HOME}/plugin/custom-functions/`(如果custom-functions不存在，则新建)
4. 重启集群，下面Environment中有对应环境操作

# Versions
- JDK-11
- Trino-359

# Functions
## Scalar Functions
| Function              | Return Type | Argument Types | Description | Usage                                   |
|-----------------------|-------------|----------------|-------------|-----------------------------------------|
| getStrX               | varchar        | varchar           |             | getStrX('current_date')                 |
| get_full_acct_no      | varchar        | varchar           |             | get_full_acct_no('current_date')                  |

## Aggregate Functions
| Function                   | Return Type | Argument Types | Description                                                                          | Usage                   |
|----------------------------| ----------- |----------------| ------------------------------------------------------------------------------------ | ----------------------- |

## Environment
开发完成后可以在本地trino环境验证
1. ssh root@172.16.8.89   p:_dtstack@xxx
2. 重启集群：
   1. cd /data/trino/trino-server-359/cli
   2. stopTri.sh
   ```
   停止 coordinator：/data/trino/trino-server-359/bin/dtlauncher stop --etc-dir /data/trino/trino-server-359/etc/cluster/coordinator
   停止 worker 1：/data/trino/trino-server-359/bin/dtlauncher stop --etc-dir /data/trino/trino-server-359/etc/cluster/worker-1
   停止 worker 2：/data/trino/trino-server-359/bin/dtlauncher stop --etc-dir /data/trino/trino-server-359/etc/cluster/worker-2
   ```
   3. startTri.sh
   ```
   启动 coordinator：/data/trino/trino-server-359/bin/dtlauncher start --etc-dir /data/trino/trino-server-359/etc/cluster/coordinator
   启动 worker 1：/data/trino/trino-server-359/bin/dtlauncher start --etc-dir /data/trino/trino-server-359/etc/cluster/worker-1
   启动 worker 2：/data/trino/trino-server-359/bin/dtlauncher start --etc-dir /data/trino/trino-server-359/etc/cluster/worker-2
   ```
3. sql验证
   1. 下载对应版本的jar，放在随意目录下：https://repo1.maven.org/maven2/io/trino/trino-cli/
   2. cd到jar目录下执行重命名操作：mv xxx.jar trino
   3. 进入终端：./trino --server http://ip:port --catalog tpch --schema tiny
   4. 执行udf函数：select getStrX('1212121211112212121'); select get_full_acct_no('1111111111111111');  即可验证
