# 硬件配置与参数调优（按实机规格落地）

| 项目 | 内容 |
|---|---|
| 文档版本 | **v1.12（2026-10-25，含二轮定稿）**· 与手册/设计/技术方案 v1.12 同步：① Flink 首次大表全量期 TM 4096m→8192m 的内存账本与恢复要求；② SILO 三桶配额 `flink 100 + exports 160 + backup 90 = 350GiB`（EC 可用 ≈450G），**backup 按每周全量 × 3 份 ≈75G 规划**；③ checkpoint 配置键迁移到 Flink 1.20 新键、`high-availability.jobmanager.port` 固定 6123；④ 容量水位统一为 53%/70% 三档口径；⑤ **数据节点新增 keepalived 0.02G**（SILO VIP `.207`，合计仍 43.8G） |
| 日期 | 2026-10-25 |
| 依据 | 用户提供的实际 VM 规格（2026-10-22）+ `DBAPI_Doris组件版本基线.md` 的版本锁定 |
| 配套 | `DBAPI_Doris数据平台部署实施手册.md`（**v1.12**）、`DBAPI_Doris数据平台详细设计.md`（**v1.12**）、`DBAPI_Doris组件版本基线.md` |

> **账本口径**：本文的账本按实机 16C64G×3 + **8C32G×3**（v1.11 应用层三台同构）计算，组件版本升级不改变常驻内存量级（Kafka/ZK 仍是 2G/1G 堆，SILO 仍是 1.2G）。只有 Doris 4.1.5 的 `enable_file_cache` 默认值翻转会影响磁盘，已在 §2 的 `be.conf` 里显式关掉。

---

## 0. 实机规格与旧假设的差异（这是本次参数必须重算的原因）

| 项 | 手册 v1.4 的假设 | **实机规格** | 差异后果 |
|---|---|---|---|
| 数据节点 CPU | 16C 起，推荐 32C | **16C** | 可接受；并发参数按 16C 收窄（§4.1） |
| 数据节点内存 | **128G**（64G 为"降级"） | **64G** | **BE 不能再按 32G 算**，全栈常驻必须压缩到 ≈45G 以内 |
| 数据节点磁盘 | 系统盘 + BE 盘 + Kafka 盘 + MinIO 盘（4 块，≥2.5TB） | **系统盘 200G + 数据盘 500G + 数据盘 500G（共 1200G，3 块）** | 分盘**可保留**：BE 独占一块 500G；Kafka/SILO/Flink/ZK 共第二块 500G。BE 容量 480G，容量余量 1.6× |
| 应用节点 | 8C 64G / 500G | **8C 32G / 500G** | PG `shared_buffers`、Redis `maxmemory` 减半（§7） |
| 网络 | 10GbE | 未提供（按 1GbE 保守估） | Kafka/Doris 副本带宽受限，见 §9 扩容阈值 |

> **一句话结论**：这台机器不是"128G 降级跑"，而是**必须按 64G 重新分配**。手册里 `be.conf JAVA_OPTS="-Xmx32g"` 这行同时犯两个错——① BE 是 C++ 进程，根本没有 JVM 堆（`JAVA_OPTS` 对它无效）；② 即便有效，32G 也会把 64G 机器挤爆。正确旋钮是 `mem_limit` 百分比。

---

## 1. 内存账本（先算清，再配参数）

### 1.1 数据节点 doris-dw01/02/03（16C / 64G）

同构部署：Doris FE + BE + Kafka + Flink(JM+TM) + SILO + ZooKeeper。**v1.11 起不再有任何应用层组件**（etcd 迁至 gw01/gw02/gw03，Redis 哨兵同理），三台完全同构、可用同一份模板。

| 组件 | 内存上限设定 | 预估常驻 RSS | 依据 |
|---|---|---|---|
| OS + 内核 + systemd + 日志 | — | 2.0 GB | 含 ssd/网络栈、dockerd |
| **Doris BE**（C++） | `mem_limit=40%` → **25.6 GB** | 20~25 GB | 默认 90% 是给独占机的；共置必须压到 40% |
| **Doris FE**（JVM） | `-Xmx4g -Xms4g` | 5.0 GB | 元数据规模小（<100 表），4G 足够；8G 是浪费 |
| **Kafka** broker | `-Xmx2g -Xms2g` | 3.0 GB | heap 2G + 页缓存(可回收) |
| **Flink JobManager** | `jobmanager.memory.process.size: 2048m` | 2.0 GB | Standalone HA 候选，仅协调 |
| **Flink TaskManager** | `taskmanager.memory.process.size: 4096m` | 4.0 GB | 4 slot，跑 CDC 作业充裕 |
| **SILO** | `GOMEMLIMIT=1GiB` | 1.2 GB | Go 运行时软上限 |
| **ZooKeeper** | `ZOO_HEAP_SIZE=1024` | 1.0 GB | 仅 Flink 选主，负载极轻 |
| **keepalived**（v1.12） | 原生进程，无 JVM | 0.02 GB | 只做 SILO VIP `.207` 的 VRRP，内存可忽略，**但要占一个 systemd 单元与 VRRP 112 端口** |
| **合计承诺** | — | **≈43.8 GB** | v1.11 起 etcd 迁出（−0.5G），v1.12 起新增 keepalived（+0.02G，计入后仍为 43.8G） |
| **剩余余量** | — | **≈20.2 GB** | 留给页缓存与突发（BE 导入峰值、Compaction） |

> 余量 20.2G 中约 3.2G 是 BE 的 LowWaterMark（`mem_limit` 越低，这个水位越安全）。**不要**把 `mem_limit` 提到 50% 以上：50% 时 BE 上限 32G，全栈承诺逼近 51G，一旦 SILO/Kafka 页缓存上涨就会触发 BE Full GC，反而拖慢查询。

### 1.2 应用节点 dbapi-gw01/02/03（8C / 32G，三台同构）

| 组件 | 内存上限设定 | 预估常驻 RSS | 依据 |
|---|---|---|---|
| OS + 内核 + dockerd | — | 2.0 GB | |
| **DBAPI**（JVM，单实例） | `-Xmx3g -Xms3g`，`MaxDirectMemorySize=512m` | 3.5 GB | Spring Boot + Hikari 20 连接 |
| **PostgreSQL**（Patroni 容器） | `shared_buffers=6GB`，`work_mem=16MB` | 9.0 GB | 6G 共享缓存 + 连接/排序/WAL |
| **Redis** | `maxmemory 2gb`（`allkeys-lru`） | 2.5 GB | 热查询缓存，2G 足够 |
| **APISIX** | `worker_processes: 8`，`worker_connections: 10240` | 1.0 GB | |
| **etcd** | `GOMEMLIMIT=512MiB` + `--quota-backend-bytes=2GiB` | 0.5 GB | 三台各一成员，同集群（v1.11 从数据节点收回） |
| **keepalived** + **Sentinel**×1 | — | 0.2 GB | 每台各 1 哨兵，共 3 个，quorum=2 |
| **合计承诺** | — | **≈18.7 GB** | 三台完全同构，单台账本不变 |
| **剩余余量** | — | **≈13.3 GB** | PG checkpoint / Redis fork 峰值用 |

> **三台都是 PG 候选节点（1 主 2 从），所以 `shared_buffers=6GB` 要按"三台都可能是主库"来配**，不能只给 gw01 配大、从库配小——故障转移后从库升主，参数不会跟着变大，小 `shared_buffers` 会立刻成为写入瓶颈。三台同规格同参数是本方案的选择。
>
> 32G 上 PG 与 DBAPI 抢内存的典型事故是 `work_mem × max_connections` 失控。`work_mem=16MB` + `max_connections=300` 理论最坏 4.8G，已包含在上表；**不要**为了跑大报表把 `work_mem` 调到 64MB 以上。

### 1.3 磁盘账本（数据节点：200G 系统盘 + 500G + 500G）

实机是**三块盘**（200 + 500 + 500 = 1200G），这比"单块 1200G"好得多——**BE 可以独占一块物理盘**，把最重的随机读 + Compaction 与 Kafka/SILO 的顺序写隔开。

**挂载规划**

| 物理盘 | 容量 | 挂载点 | 承载 |
|---|---|---|---|
| `/dev/sda` | 200 GB | `/` | OS、Docker 镜像、各组件日志 |
| `/dev/sdb` | 500 GB | `/data/doris` | Doris FE meta + BE storage（独占） |
| `/dev/sdc` | 500 GB | `/data/store` | Kafka、SILO、Flink 本地状态、ZK（etcd 已于 v1.11 迁至应用层） |

> **落地命令见部署手册 §2.4**（认盘 → GPT 分区 → `mkfs.xfs` → UUID/fstab → 挂载验证 → SELinux 标签 → 扩容）。本节只给账本与依据，不重复写命令，避免两处不一致。

**容量分配**

| 用途 | 路径 | 配额 | 说明 |
|---|---|---|---|
| OS + 软件 + 日志 | `/` | 200 GB（整盘） | docker 日志轮转 100m×5、系统日志 logrotate |
| Doris FE meta | `/data/doris/fe/meta` | ≤ 20 GB | BDBJE 三副本，量极小 |
| **Doris BE** | `/data/doris/be/storage` | **480 GB（整盘留 20G 缓冲）** | `replication_num=3`，唯一大额占用 |
| **Kafka** | `/data/store/kafka/logs` | **120 GB** | `log.retention.bytes` 硬上限保护 |
| **SILO** | `/data/store/silo/d1` + `d2` | **300 GB**（150×2） | 同盘划两个目录凑 EC 最少 4 盘（§6.1） |
| **Flink 本地状态** | `/data/store/flink/tmp` | **50 GB** | RocksDB localdir + spill |
| **ZK / etcd** | `/data/store/zookeeper`、`/data/store/etcd` | **10 GB** | 事务日志量极小 |

**容量换算（v1.10 修正：区分"装得下"与"坏了能自愈"）**：BE 480G × 3 节点 = 1440G 裸盘 ÷ 3 副本 = **480G 单副本可用**，这是稳态上限。

但**单节点故障时，剩余 2 台要各扛 1.5 份副本**（3 份摊到 2 台）。留 20% 给 compaction 与元数据后每台可用 384G，故：

> **故障自愈上限 = 384G ÷ 1.5 ≈ 256G/节点（53%）**，对应单副本总量 **≈768G**

按设计文档 §2.8 的 500G 原始量测算（三副本合计 ≈885G，即每节点 ≈295G）——**这个量稳态能跑，但已经越过自愈线**：一旦坏一块盘，副本补齐会因 `Disk usage exceeds` 反复失败，集群长期停在 2 副本。若要求单盘故障可自愈，原始数据量应控制在 **≈430GB** 以内。

落在 256G~336G 之间时准备好应急脚本（故障即刻降副本、缩 ODS 保留、腾空间后再回 3 副本），超过 336G（70%）则稳态本身危险。

> ⚠️ **第二块盘（`/data/store`）上仍有 I/O 争抢**：Kafka 追加 + SILO checkpoint/导出 + Flink RocksDB 全在 `/dev/sdc`。缓解手段是 Kafka 保留上限 + checkpoint 间隔放宽（§4.1）+ SILO 桶配额（§6.3）。好在这些都是**大块顺序写**，且最关键的是它们已经与 BE 的随机读/Compaction 彻底分开——这比"全塞一块盘"好一个量级。
>
> **扩容优先级**：① 先给 `/data/store` 加盘（Kafka/SILO 扩容最频繁）；② 再给 BE 加盘（`storage_root_path` 支持多路径，逗号分隔追加即可，无需重建集群）。

---

## 2. Doris 参数（FE / BE）

### 2.1 `fe.conf`（三台 FE 完全一致，dw01 为 Leader）

```properties
# ---- 基础 ----
meta_dir = /data/doris/fe/meta
http_port = 8030
rpc_port  = 9020
query_port = 9030
edit_log_port = 9010
JAVA_HOME = /usr/lib/jvm/temurin-17-jdk

# ---- JVM：4G（原 8G 对 64G 节点是浪费）----
# Doris 3.x/4.x 在 JDK17 下优先读 JAVA_OPTS_FOR_JDK_17，两个都写避免版本差异失效
JAVA_OPTS = "-Xmx4g -Xms4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Dfile.encoding=UTF-8"
JAVA_OPTS_FOR_JDK_17 = "-Xmx4g -Xms4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Dfile.encoding=UTF-8"

# ---- 连接与事务（DBAPI 3 实例 × 25 连接 + DS + 运维，1024 足够）----
qe_max_connection = 1024
max_running_txn_num_per_db = 1000
stream_load_default_timeout_second = 600

# ---- 动态分区 ----
dynamic_partition_enable = true
```

### 2.2 `be.conf`（三台 BE 一致，**关键改动**）

```properties
# ---- 存储 ----
# BE 独占 /data/doris（/dev/sdb 500G），单路径即可；
# 后续扩盘时用逗号追加第二个路径（storage_root_path=/p1;/p2），Doris 会按盘均衡，无需重建集群
storage_root_path = /data/doris/be/storage
be_port = 9060
webserver_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060
JAVA_HOME = /usr/lib/jvm/temurin-17-jdk   # 仅供 Java UDF 使用，BE 本体是 C++

# ---- 内存（核心：取代原手册错误的 JAVA_OPTS -Xmx32g）----
mem_limit = 40%                    # 64G × 40% = 25.6G，与 Kafka/Flink/SILO/ZK 共存的唯一可行值
soft_mem_limit_frac = 0.8          # 软限 20.5G，提前触发 Minor GC，避免撞硬限
storage_page_cache_limit = 20%     # 页缓存 ≈5G（相对 mem_limit）
disable_storage_page_cache = false
# 4.1.5 起 enable_file_cache 默认 false→true，会占本地盘做 file cache 吃掉 480G 余量 → 显式关
enable_file_cache = false

# ---- 导入 / Compaction（64G 小内存必须限速）----
load_process_max_memory_limit_percent = 40
compaction_max_memory_limit_percent = 20
write_buffer_size = 100M
max_routine_load_task_num_per_be = 3      # 原默认 5，16C 共置机收到 3
routine_load_thread_pool_size = 6
max_consumer_num_per_group = 3
streaming_load_max_mb = 10240
max_tablet_version_num = 500
```

### 2.3 与硬件相关的三个 Doris 运行时参数（建库后执行一次）

```sql
-- 16C：pipeline 并发取核数一半，避免单查询吃满所有核导致并发查询排队
SET GLOBAL parallel_pipeline_task_num = 8;

-- 25.6G BE 内存下，单查询给 4G；再大容易触发 BE Full GC
SET GLOBAL exec_mem_limit = 4294967296;

-- 内存偏小 + 磁盘有限：开启自动落盘（Doris 2.1+），大聚合/排序溢出到磁盘而不是 OOM
SET GLOBAL enable_spill = true;
SET GLOBAL spill_mode = "auto";

-- 查询超时与审计：慢查询 300s 切断，避免占住内存
SET GLOBAL query_timeout = 300;
```

> **为什么不是 `mem_limit=90%`（默认值）**：Doris 内存控制策略里 `MemLimit = mem_limit × 物理内存`，默认 90% 意味着 BE 可以涨到 57.6G。共置机上 Kafka(3G)+Flink(6G)+SILO(1.2G)+ZK(1G)+FE(5G) 已经占了 16G，BE 再涨到 57G 必然 OOM Kill（通常先被杀的是 SILO 或 Kafka，表现为"checkpoint 写失败"）。40% 是这台机器的稳态上限。

### 2.4 验证命令

```sql
-- 内存上限是否生效（Value 应为 40%）
SHOW BACKEND CONFIG LIKE "mem_limit";
SHOW BACKEND CONFIG LIKE "soft_mem_limit_frac";
-- 各 BE 实时内存水位（MemTracker，重点看 Query/Load/Compaction 三项）
SHOW PROC "/backends";                       -- 看 MemUsed / MaxMemory
curl -s http://192.168.110.201:8040/mem_tracker | head -50
```

---

## 3. Kafka 参数（每台 1 broker，KRaft）

### 3.1 启动命令（dw01 示例，dw02/03 只改 NODE_ID 与 ADVERTISED_LISTENERS）

```bash
# v1.6：镜像换官方 apache/kafka，环境变量前缀 KAFKA_CFG_* → KAFKA_*，并新增 CLUSTER_ID（三台一致）
docker run -d --name kafka --restart unless-stopped --net host \
  -v /data/store/kafka/data:/var/lib/kafka/data \
  -e CLUSTER_ID=<kafka-storage.sh random-uuid 生成的集群ID> \
  -e KAFKA_HEAP_OPTS="-Xmx2g -Xms2g" \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@192.168.110.201:9093,2@192.168.110.202:9093,3@192.168.110.203:9093 \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://192.168.110.201:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_LOG_DIRS=/data/store/kafka/logs \
  -e KAFKA_NUM_PARTITIONS=3 \
  -e KAFKA_DEFAULT_REPLICATION_FACTOR=3 \
  -e KAFKA_MIN_INSYNC_REPLICAS=2 \
  -e KAFKA_LOG_RETENTION_HOURS=48 \
  -e KAFKA_LOG_RETENTION_BYTES=5368709120 \
  -e KAFKA_LOG_SEGMENT_BYTES=536870912 \
  -e KAFKA_LOG_CLEANUP_POLICY=delete \
  -e KAFKA_MESSAGE_MAX_BYTES=10485760 \
  -e KAFKA_REPLICA_FETCH_MAX_BYTES=10485760 \
  -e KAFKA_COMPRESSION_TYPE=lz4 \
  -e KAFKA_NUM_IO_THREADS=8 \
  -e KAFKA_NUM_NETWORK_THREADS=4 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
  -e KAFKA_SOCKET_SEND_BUFFER_BYTES=1048576 \
  -e KAFKA_SOCKET_RECEIVE_BUFFER_BYTES=1048576 \
  apache/kafka:4.3.2
```

### 3.2 参数取舍说明

| 参数 | 取值 | 为什么 |
|---|---|---|
| `KAFKA_HEAP_OPTS` | 2G | 64G 共置机给 2G 足够（单 broker 峰值 <5MB/s）；heap 大了只会拖长 GC |
| `LOG_RETENTION_HOURS` | 48 | 原方案默认 168h（7 天），在 100G 配额下必然写满 |
| `LOG_RETENTION_BYTES` | 5G/分区 | 配合 3 分区 × 3 topic ≈45G，留出余量；**这是磁盘保护的第一道闸** |
| `LOG_SEGMENT_BYTES` | 512M | 段文件小一点，删除更及时，不会因大段滞留占盘 |
| `MIN_INSYNC_REPLICAS` | 2 | 3 副本允许 1 个落后，兼顾写入可用性与不丢数据 |
| `COMPRESSION_TYPE` | lz4 | CDC JSON 冗余度高，lz4 压缩比 ~3-5×，直接换算成磁盘和网络收益 |
| `AUTO_CREATE_TOPICS_ENABLE` | false | 防止拼错 topic 名静默建出 1 分区 1 副本的坑 topic |
| `NUM_IO_THREADS` | 8 | 16C 机器取一半；默认 8 亦可，不要超过核数 |

> **镜像与变量前缀（v1.6 修正）**：Bitnami 的版本化 tag 已全部迁到 `bitnamilegacy` 并从公共目录移除，**`bitnami/kafka:3.7` 已拉不到**；Kafka 3.x 也已 EOL。改用官方 `apache/kafka:4.3.2`（2026-10-20，最新补丁版；4.4.0 太新、无补丁），并注意两件事：① 环境变量前缀是 **`KAFKA_*`**，不是 Bitnami 的 `KAFKA_CFG_*`——沿用旧名会被**静默忽略**，副本因子与 KRaft 配置全部回落默认值；② KRaft 必须显式给 `CLUSTER_ID`，**三台必须完全一致**。上一版这里写"KAFKA_CFG_* 在官方镜像里同样支持"是错的。

---

## 4. Flink 参数（Standalone HA，三台各 1 JM + 1 TM）

### 4.1 `flink-conf.yaml`（dw01 示例；dw02/03 改 `jobmanager.rpc.address`、`rest.address`、`taskmanager.host`）

```yaml
# ---- 进程与地址 ----
jobmanager.rpc.address: doris-dw01
jobmanager.rpc.port: 6123
jobmanager.bind-address: 0.0.0.0
taskmanager.bind-address: 0.0.0.0
taskmanager.host: doris-dw01          # 关键：TM 向 JM 注册时上报的主机名，缺省会用容器/反查名
rest.bind-address: 0.0.0.0            # 原手册写成 rest.address: 0.0.0.0，语义错误
rest.address: doris-dw01
rest.port: 8081

# ---- 内存（按 2G JM + 4G TM 分配，对应 §1.1 账本）----
jobmanager.memory.process.size: 2048m
taskmanager.memory.process.size: 4096m
taskmanager.memory.managed.fraction: 0.4
taskmanager.numberOfTaskSlots: 4
parallelism.default: 4

# ---- 状态与 checkpoint（小内存 + 共享盘的关键调优）----
state.backend: rocksdb
state.backend.incremental: true
state.backend.rocksdb.memory.managed: true
state.backend.rocksdb.localdir: /data/store/flink/tmp
io.tmp.dirs: /data/store/flink/tmp
state.checkpoints.dir: s3://flink/checkpoints/
state.savepoints.dir: s3://flink/savepoints/
state.checkpoints.num-retained: 3
execution.checkpointing.interval: 30s
execution.checkpointing.min-pause: 10s
execution.checkpointing.timeout: 10min
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

# ---- HA（ZooKeeper 选主 + SILO 存元数据）----
high-availability: zookeeper
high-availability.cluster-id: dbapi-flink
high-availability.zookeeper.quorum: 192.168.110.201:2181,192.168.110.202:2181,192.168.110.203:2181
high-availability.zookeeper.path.root: /flink
high-availability.storageDir: s3://flink/ha/

# ---- SILO（S3 兼容）----
s3.endpoint: http://192.168.110.201:9000
s3.path.style.access: "true"
s3.access.key: <强口令>
s3.secret.key: <强口令>
```

### 4.2 说明

- **`taskmanager.host` 必须显式写**：Standalone 模式下如果缺省，TM 会用主机名反查结果注册；一旦反查到容器网段或短名，JM 连不回来，表现为"作业一直 INITIALIZING / TM 显示 0"。
- **checkpoint 间隔 30s 而不是 10s**：CDC 位点靠 checkpoint 落盘，10s 间隔在共享盘上会产生持续的小文件写放大（3 个 TM 同时写 RocksDB + 上传 SILO，还和 Kafka 抢 sdc）。30s 意味着故障恢复后**最多重放 30s 的 binlog**（ODS 为 Duplicate 模型，重放会产生重复行，靠 DWD `UNIQUE KEY` 去重）。
- **`state.checkpoints.num-retained: 3`**：SILO 只有 3 节点 6 目录的弱冗余（§6），多留 2 份历史 checkpoint 是性价比最高的兜底。
- **本地状态放 `/data/store/flink/tmp` 且限 50G**：RocksDB 本地目录不能放系统盘，否则写满 `/` 会连带 docker 与 Doris 日志一起挂；也不要放 BE 盘，避免与 Compaction 抢 I/O。

---

## 5. ZooKeeper / etcd 参数

### 5.1 ZooKeeper（三台，仅服务 Flink HA）

```bash
# v1.6：Bitnami 镜像已下架，改用官方 zookeeper:3.9.6；变量名 ZOO_SERVER_ID → ZOO_MY_ID
docker run -d --name zk --restart unless-stopped \
  -p 2181:2181 -p 2888:2888 -p 3888:3888 \
  -v /data/store/zookeeper/data:/data \
  -v /data/store/zookeeper/log:/datalog \
  -e ZOO_MY_ID=1 \
  -e JVMFLAGS="-Xms1g -Xmx1g" \
  -e ZOO_TICK_TIME=2000 \
  -e ZOO_INIT_LIMIT=10 \
  -e ZOO_SYNC_LIMIT=5 \
  -e ZOO_MAX_CLIENT_CNXNS=200 \
  -e ZOO_AUTOPURGE_SNAPRETAINCOUNT=3 \
  -e ZOO_AUTOPURGE_PURGEINTERVAL=24 \
  -e ZOO_4LW_COMMANDS_WHITELIST="srvr,ruok" \
  -e ZOO_ADMINSERVER_ENABLED=false \
  -e ZOO_SERVERS="server.1=192.168.110.201:2888:3888;2181 server.2=192.168.110.202:2888:3888;2181 server.3=192.168.110.203:2888:3888;2181" \
  zookeeper:3.9.6
```

- `JVMFLAGS="-Xms1g -Xmx1g"`：官方镜像不认 Bitnami 的 `ZOO_HEAP_SIZE`，堆要通过 ZK 自身的 `JVMFLAGS` 传，64G 共置机上收成 1G。
- `ZOO_ADMINSERVER_ENABLED=false`：关掉内置 AdminServer（默认占 8080），省内存也避端口冲突。
- `AUTOPURGE`：ZK 的 snapshot/事务日志不清会慢涨，在第二块盘（500G）的小配额里必须开自动清理（保留 3 份、每 24h 清一次）。
- `TICK_TIME=2000`：LAN 内默认 2000ms 即可；调小会增加选主抖动。

### 5.2 etcd（三成员：gw01 / gw02 / gw03，**全在应用层**）

三成员全部在应用层（v1.11 从"gw01+gw02+dw01 跨层投票"改为"gw01+gw02+gw03"）：**故障域与应用层完全重合**，数据节点怎么折腾都不影响 APISIX 读配置与 Patroni 选主，且数据节点少一个常驻容器。`--name` 分别为 `etcd-gw01/gw02/gw03`，`--initial-cluster` 三台逐字一致。

在原命令上补三项（对 APISIX 配置中心 + Patroni DCS 双用户都必要）：

```bash
  --quota-backend-bytes=2147483648 \
  --auto-compaction-mode=periodic \
  --auto-compaction-retention=1h \
  --snapshot-count=10000 \
  --heartbeat-interval=100 \
  --election-timeout=1000
```

| 参数 | 作用 |
|---|---|
| `quota-backend-bytes=2GiB` | 默认 2GiB 本身够用，但**写满会直接拒绝写入**（APISIX 改不了路由、Patroni 抢不到 lease）；显式声明便于监控 |
| `auto-compaction-retention=1h` | APISIX 每次路由变更都写 revision，不压缩会导致 db 体积持续增长；1h 压缩对 500G 盘成本可忽略 |
| `heartbeat-interval/election-timeout` | LAN 内降低选主耗时（默认 100/1000 已是推荐值，显式写出便于复制） |
| `GOMEMLIMIT=512MiB`（环境变量） | Go 运行时软上限，防止 etcd 在异常时吃满内存 |

---

## 6. SILO 参数（重点：3 节点 1 盘不满足 EC）

### 6.1 硬约束

SILO 沿用 MinIO 的纠删码实现，**启用纠删码至少需要 4 个 drive**。若每节点只在第二块 500G 盘上划 1 个目录，3 节点 = 3 个 drive，**不具备纠删码冗余**——Flink 的 checkpoint 与 HA 元数据恰好存在这里，一旦某台节点磁盘损坏，checkpoint 就可能整体不可读，HA 恢复失效。

三块盘（200+500+500）的条件下，SILO 可以用"同一块盘划两个目录"的办法凑够 drive 数，**不需要加硬件**：

**方案 A（推荐，当前可落地）**：每节点在第二块数据盘上划 **2 个目录**（`/data/store/silo/d1`、`/data/store/silo/d2`，各 150G），3 节点 × 2 目录 = **6 个 drive**，满足 EC 最低要求；**可用容量按 1/2 算（≈450G），不是 2/3（600G）**——SILO/MinIO 的默认 parity 表在 6~7 drive 时取 **EC:3**，即 6 份数据里 3 份是校验，可用 = 裸容量 × 3/6。实际值用 `docker exec silo mcli admin info local` 复核。

**方案 B（不推荐）**：退化为单实例 SILO（1 节点），无冗余但部署简单——只适合验收演示环境。

> ⚠️ 方案 A 属于上游 MinIO 明确"允许但不推荐"的用法（同一物理卷多目录，失去磁盘级故障隔离）。它的真实收益是**解决"对象被 EC 切分后单盘损坏导致对象不可读"的问题**，但**不能**解决"整块 500G 数据盘损坏"。因此必须同时做 §6.3 的桶配额 + checkpoint 多份保留。生产最终态建议给第二块盘扩容时按独立物理盘规划。

### 6.2 启动命令（三台完全一致）

```bash
mkdir -p /data/store/minio/d1 /data/store/minio/d2

# v1.6：上游 minio/minio 镜像已被删除，改用 Pigsty 维护的 MinIO 分支 SILO
#       参数契约不变（MINIO_ROOT_*、/minio/* 路由、.minio.sys 存储格式）
docker run -d --name silo --restart unless-stopped \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=<强用户名> \
  -e MINIO_ROOT_PASSWORD=<强口令，≥16位> \
  -e GOMEMLIMIT=1GiB \
  -v /data/store/minio/d1:/d1 -v /data/store/minio/d2:/d2 \
  docker.io/pgsty/silo:RELEASE.2026-10-18T00-00-00Z server \
    http://192.168.110.201:9000/d1 http://192.168.110.201:9000/d2 \
    http://192.168.110.202:9000/d1 http://192.168.110.202:9000/d2 \
    http://192.168.110.203:9000/d1 http://192.168.110.203:9000/d2 \
    --console-address ":9001"

docker exec silo silo healthcheck ready      # 返回 0 即就绪
```

### 6.3 桶与配额（必做，防对象存储吃满 BE 的盘）

```bash
# SILO 镜像自带 mcli，直接在容器内执行（不再需要 minio/mc 容器）
docker exec silo mcli alias set local http://127.0.0.1:9000 <user> <pass>
docker exec silo mcli mb --ignore-existing local/flink
docker exec silo mcli mb --ignore-existing local/exports
docker exec silo mcli quota set local/flink   --size 80GiB
docker exec silo mcli quota set local/exports --size 160GiB
docker exec silo mcli admin info local
```

- `flink` 桶：checkpoint / savepoint / HA 元数据，80GiB 上限（3 份 checkpoint × 小作业通常 <20G）。
- `exports` 桶：DBAPI 异步导出 CSV，160GiB 上限 + 生命周期（`docker exec silo mcli ilm add --expiry-days 7 local/exports`），否则导出文件永久堆积。
- **两个配额合计 240GiB，远小于 EC 可用容量（≈450G）**：这是有意的——对象存储与 Kafka/Flink 共用第二块 500G 盘，配额就是"不许长胖"的硬闸。
- **SILO 使用注意**：钉死 release tag；运维客户端统一 `mcli`（与旧 `mc` 只保证 best-effort 兼容）；**禁止旧 MinIO 与 SILO 混部滚动升级**，要么全停全起同版本，要么不动。

---

## 7. 应用节点参数（32G / 8C）

### 7.1 PostgreSQL（Patroni 托管，`patroni.yml` 的 `postgresql.parameters`）

```yaml
postgresql:
  parameters:
    # 内存：6G 共享缓存（32G × 19%），配合 2 主从双写
    shared_buffers: 6GB
    effective_cache_size: 12GB
    work_mem: 16MB
    maintenance_work_mem: 1GB
    wal_buffers: 16MB
    # 连接：DBAPI 3 实例 × 25 + Patroni 2 条复制连接 + DS + 运维监控
    max_connections: 300
    # 8C 并行度
    max_worker_processes: 8
    max_parallel_workers: 8
    max_parallel_workers_per_gather: 2
    # 写放大控制（500G 盘 + 调用日志持续写）
    checkpoint_completion_target: 0.9
    max_wal_size: 4GB
    min_wal_size: 1GB
    # SSD
    random_page_cost: 1.1
    effective_io_concurrency: 200
    # 复制（Patroni synchronous_mode 会自行管理 synchronous_commit）
    wal_level: replica
    max_wal_senders: 10
    max_replication_slots: 10
    wal_keep_size: 1GB
    hot_standby: on
    # 观测
    shared_preload_libraries: pg_stat_statements
    log_min_duration_statement: 1000
```

> `synchronous_mode: true` 时 Patroni 会把 `synchronous_standby_names` 指向同步从库；**不要**在 `parameters` 里手写该项，否则与 Patroni 冲突。

### 7.2 DBAPI（systemd，JVM 3G）

```ini
[Service]
User=dbapi
Environment="JAVA_HOME=/usr/lib/jvm/java-21-openjdk"
ExecStart=/usr/lib/jvm/java-21-openjdk/bin/java \
  -Xms3g -Xmx3g \
  -XX:+UseG1GC -XX:MaxGCPauseMillis=200 \
  -XX:MaxDirectMemorySize=512m \
  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/var/log/dbapi \
  -jar /opt/dbapi/dbapi.jar --spring.profiles.active=prod
Restart=on-failure
RestartSec=10
LimitNOFILE=655350
```

- `-Xmx3g`：原手册 2G 偏紧（Doris 结果集反序列化 + 脱敏 + 导出），32G 机器给 3G 合理。
- `MaxDirectMemorySize=512m`：Netty/S3 SDK（SILO）直连内存，显式限制防堆外泄漏。
- `LimitNOFILE`：systemd 不读 `limits.conf` 的 `*` 通配，必须显式写。

### 7.3 Redis（主从 + **3 哨兵**）

```bash
# gw01 主 / gw02、gw03 从（replicaof 192.168.110.204 6379）：
# 必须 --net host（否则切换后哨兵通告容器内网 IP，DBAPI 连不上新主）
docker run -d --name redis-master --restart unless-stopped --net host \
  -v /data/redis:/data \
  redis:7.4 redis-server \
  --bind 0.0.0.0 --port 6379 \
  --requirepass <强口令> \
  --masterauth <强口令> \
  --maxmemory 2gb --maxmemory-policy allkeys-lru \
  --appendonly yes --appendfsync everysec \
  --save 900 1 \
  --replica-announce-ip 192.168.110.204
```

**哨兵必须 3 个**（gw01:26379、gw02:26379、**gw03:26379**，全在应用层），`quorum=2`。

> ⚠️ **v1.11 修正：哨兵配置必须落成 `sentinel.conf` 挂载，不能用 `SENTINEL MONITOR/SET` 运行时下发**。运行时命令只存在于内存，容器或主机重启后全部丢失，哨兵变成"不知道要监控谁"的空进程（表现为 `num-other-sentinels=0`、主库真挂也不切）。本文 v1.10 及以前给的是运行时命令写法，**已作废**，正确写法见部署手册 §3.4 步骤 5。

```bash
mkdir -p /data/redis-sentinel
cat > /data/redis-sentinel/sentinel.conf <<EOF
port 26379
bind 0.0.0.0                      # 默认 127.0.0.1 时跨节点通告被拒，3 哨兵互相发现不了
protected-mode no                 # 同理：默认 yes 会拒绝其它哨兵/客户端
sentinel monitor mymaster 192.168.110.204 6379 2
sentinel auth-pass mymaster <强口令>
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 60000
sentinel announce-ip <本机IP>       # gw01=.204 / gw02=.205 / gw03=.206
EOF
chmod 666 /data/redis-sentinel/sentinel.conf    # 容器内 redis(uid 999) 要能回写 known-replica

docker run -d --name redis-sentinel --restart unless-stopped --net host \
  -v /data/redis-sentinel/sentinel.conf:/usr/local/etc/redis/sentinel.conf:rw,z \
  redis:7.4 redis-server /usr/local/etc/redis/sentinel.conf --sentinel
```

> 原方案 2 哨兵 + `quorum=1` 是**假高可用**：哨兵的客观下线判定需要 quorum 个哨兵同意，但**执行故障转移需要多数派授权（majority of all sentinels）**。2 个哨兵时多数派 = 2，任一台宕机就永远无法切换。3 哨兵 quorum=2 才是标准配置；3 台应用节点还带来一个额外好处——**一台维护时剩 2 哨兵仍能完成切换**，2 台时代只要一台离线，自动切换能力就等于零。

### 7.4 APISIX（8C）

`config.yaml` 关键段（**`host` 不是 `endpoints`**，且需显式声明 role 与 config_provider）：

```yaml
apisix:
  node_listen: 9080
  enable_ipv6: false
  worker_processes: 8
  worker_rlimit_nofile: 655350

nginx_config:
  worker_connections: 10240
  event:
    use: epoll

deployment:
  role: traditional
  role_traditional:
    config_provider: etcd
  etcd:
    host:
      - "http://192.168.110.204:2379"
      - "http://192.168.110.205:2379"
      - "http://192.168.110.206:2379"      # v1.11：第三成员改在 gw03（原 .201/dw01 已迁出）
    prefix: /apisix
    timeout: 30
  admin:
    allow_admin:
      - 127.0.0.0/24
      - 127.0.0.1/32          # v1.9 收窄：原为 192.168.110.0/24，而 DS 集群与办公终端同在此网段
      - 192.168.110.204/32
      - 192.168.110.205/32
      - 192.168.110.206/32      # v1.11 增补第三台应用节点
    admin_key:
      - name: admin
        key: <强随机串>
        role: admin
```

> `worker_connections 10240` + `worker_processes 8` 是 8C 下的常规值；同时要把系统 `nofile`（655350，§8）与 docker `default-ulimits` 一起落实，否则 10240 会被容器限制截断。

---

## 8. 系统层参数（按 64G / 32G 区分）

### 8.1 sysctl（数据节点 64G）

```conf
vm.swappiness=10
vm.zone_reclaim_mode=0
kernel.numa_balancing=0
vm.max_map_count=2000000          # Doris BE 硬性要求，不可省
vm.overcommit_memory=1            # Redis 硬性要求
vm.min_free_kbytes=1048576        # 64G 机器 1G 低水位
# 共享盘写密集：用绝对字节控制脏页，默认比例制在 64G 上会攒到 12.8G 才刷盘
vm.dirty_background_bytes=268435456   # 256MB 起刷
vm.dirty_bytes=1073741824             # 1GB 硬顶
net.core.somaxconn=65535
net.ipv4.tcp_max_syn_backlog=65535
net.ipv4.ip_local_port_range=1024 65535
net.core.rmem_max=16777216
net.core.wmem_max=16777216
net.ipv4.tcp_rmem=4096 87380 16777216
net.ipv4.tcp_wmem=4096 65536 16777216
```

### 8.2 sysctl（应用节点 32G，差异项）

```conf
vm.min_free_kbytes=524288         # 32G 机器 512M 足够，1G 会挤压 PG
vm.dirty_background_bytes=134217728   # 128MB
vm.dirty_bytes=536870912              # 512MB
# 其余同上
```

> **注意**：`vm.dirty_ratio`（比例）与 `vm.dirty_bytes`（字节）是互斥的，设置后者会把前者自动置 0。原手册只配了比例，在 64G 机器上等于"允许 12.8G 脏页"，与 Kafka/SILO/Flink 共享第二块盘的组合冲突。

### 8.3 THP 固化单元（原手册有个顺序问题）

```ini
[Unit]
Description=Disable Transparent Huge Pages (THP)
DefaultDependencies=no
After=sysinit.target
Before=multi-user.target

[Service]
Type=oneshot
ExecStart=/bin/sh -c 'echo never > /sys/kernel/mm/transparent_hugepage/enabled; echo never > /sys/kernel/mm/transparent_hugepage/defrag'

[Install]
WantedBy=multi-user.target
```

原来写 `Before=sysinit.target` 时，`/sys` 可能还没挂载完，`ExecStart` 会因路径不存在而静默失败（oneshot 失败不阻塞启动，也不易发现）。改成 `After=sysinit.target` 才能保证 sysfs 就绪。

### 8.4 其他必配

```bash
# chrony 必须显式安装（最小化安装的 AlmaLinux 10 不带）
yum install -y chrony && systemctl enable --now chronyd

# 文件句柄：limits.conf 对容器无效，必须配 docker default-ulimits（手册 §2.8 已有，保留）
# 日志轮转：`/` 只有 200G，Kafka/SILO/容器日志靠 docker max-size 100m×5 兜底
```

---

## 9. 容量与扩容触发阈值（照这个看，不要凭感觉）

| 指标 | 观察命令 | 阈值 | 触发动作 |
|---|---|---|---|
| BE 磁盘使用率 | `df -h /data/doris`、`du -sh /data/doris/be/storage` | **> 53%（≈256G）→ 越过故障自愈线**；**> 70%（≈336G）→ 稳态危险** | 53% 起进入扩容流程；70% 必须立即加盘或缩保留期（3 副本需 ≥3 台，`storage_root_path` 逗号追加） |
| BE 内存水位 | `curl :8040/mem_tracker` | 持续 **> 85% of mem_limit** | 先降 Flink TM 到 3G；再考虑把 Flink/Kafka 迁出（需 128G 或独立节点） |
| Kafka 磁盘 | `du -sh /data/store/kafka/logs` | **> 100G（配额 120G）** | 降 `log.retention.hours` 到 24h，或按 topic 设 `retention.bytes` |
| 第二块盘剩余 | `df -h /data/store` | **< 50G** | 清 `exports` 桶、降 Kafka 保留、收紧 SILO 配额 |
| Routine Load 延迟 | `SHOW ROUTINE LOAD\G` 的 `Lag` | 持续 **> 10 万** | 加 Kafka 分区（≤9）并提 `desired_concurrent_number`；仍不够则独立部署 Kafka |
| PG 库体积 | `SELECT pg_database_size('dbapi_db')` | **> 200G** | 缩短日志保留到 60 天；`api_call_log` 归档到 SILO。**注意三台各有一份全量数据**（1主2从），规划磁盘时按"单份 × 3 台"看 |
| PG 主库切换耗时 | `patronictl list` / 演练 | **> 60s** | 调 `ttl:30/loop_wait:10`（已设）；检查 etcd 延迟（三成员全在应用层，应用层一台维护时延迟会升高） |
| **PG 复制从库数**（v1.11） | 主库 `SELECT count(*) FROM pg_stat_replication;` | **< 2** | 说明少了一台从库（gw03 未加入或 `pg_hba` 漏 `.206`），此时主库宕机后 RPO 降级窗口会回到小时级 |
| 磁盘 iowait | `iostat -x 1`（分别看 sdb / sdc） | 持续 **`%util > 80%`** | **加盘优先于加节点**；sdc 更敏感（Kafka+SILO+Flink 同盘） |
| 数据量 | `SHOW DATA` 的单副本总容量 | **> 768G（自愈线）** | 规划第 4 个 BE 或给每节点加盘扩容；未越线但已 > 400G 时应先验证故障演练能否完成副本补齐 |

**必须升级到 128G 的场景**（供立项参考）：Flink CDC 同步表数 > 50、Kafka 峰值 > 20MB/s、或需要在数据节点上再叠 Prometheus + Grafana + Doris 审计插件。届时 BE `mem_limit` 可放到 60~70%，Kafka/Flink 各加 2G。

---

## 10. 上线前一次性核对清单

```bash
# 1) 内存是否真的没被写爆（各节点）
free -g && ps -eo pid,rss,comm --sort=-rss | head -12

# 2) Doris 参数是否生效
mysql -h 192.168.110.201 -P 9030 -u root -e "SHOW BACKEND CONFIG LIKE 'mem_limit';"
mysql -h 192.168.110.201 -P 9030 -u root -e "SHOW FRONTENDS\G" | grep -E 'Xmx|Jvm'

# 3) 磁盘配额是否按 §1.3 落地（三块盘分别核对）
df -h / /data/doris /data/store
du -sh /data/doris/be/storage /data/store/kafka /data/store/minio /data/store/flink

# 4) Kafka 保留是否生效
docker exec kafka kafka-configs.sh --bootstrap-server 192.168.110.201:9092 \
  --entity-type topics --entity-name ods_trade_order --describe

# 5) SILO EC 是否真的启用（输出里应出现 6 drives / EC 集合）
docker logs minio 2>&1 | grep -iE "erasure|drive|pool" | tail -20

# 6) THP 是否开着（应为 [never]）
cat /sys/kernel/mm/transparent_hugepage/enabled
```

---

## 11. 修订说明

| 版本 | 日期 | 内容 |
|---|---|---|
| **v1.12（二轮）** | **2026-10-25** | ① 数据节点账本新增 **keepalived 0.02G**（承载 SILO VIP `.207`），合计仍 43.8G；② SILO 桶配额重述为 `flink 100 + exports 160 + backup 90 = 350GiB`，并明确 **backup 按"每周全量 × 3 份 ≈75G"规划**（原"每日 14 天 350G"在 90GiB 配额下不成立）；③ 容量判据不变（53%/70% 三档 + 768G 自愈线），与详细设计 §2.8 一致 |
| v1.11 | 2026-10-23 | 应用服务层 2 台 → 3 台（`dbapi-gw01/02/03`，8C/32G/500G 同构）：① 数据节点账本 44.3→**43.8G**（etcd 迁出，余量 20.2G）；② 应用节点账本明确三台同构、每台 1 哨兵 + 1 etcd 成员（合计仍 18.7G）；③ **PG 三台都用 `shared_buffers=6GB`**（从库升主后参数不会变大，不能只给 gw01 配大）；④ 连接数口径 2×25→**3×25**；⑤ **修正 v1.10 遗留缺陷**：哨兵仍用 `SENTINEL MONITOR/SET` 运行时命令下发（重启即丢），改为 `sentinel.conf` 挂载 + `bind 0.0.0.0` + `protected-mode no`；镜像由浮动 `redis:7` 钉为 `redis:7.4`；⑥ §9 新增"PG 复制从库数 < 2"告警项 |
| v1.5 | 2026-10-22 | 按实机规格（8C32G×2 + 16C64G×3 + **200G 系统盘 + 2×500G 数据盘**）重算全栈内存/磁盘账本；BE 独占 500G 盘、Kafka/MinIO/Flink 共第二块 500G 盘；给出 Doris/Kafka/Flink/MinIO/ZK/etcd/PG/Redis/APISIX 逐组件参数；修正 BE 用 `JAVA_OPTS` 控内存的错误、MinIO 3 盘无 EC、Redis 2 哨兵假高可用、THP 单元顺序、脏页比例制等问题；新增容量扩缩容阈值 |
