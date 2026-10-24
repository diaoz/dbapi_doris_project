# DBAPI + Doris 数据平台部署实施手册（AlmaLinux 10）

| 项目   | 内容                                                                                                                                                                                                                                                                                                                                                         |
| ---- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 文档版本 | **v1.12（生产整改版，2026-10-25 二轮定稿）**｜**二轮新增**：§3.1 步骤 7b「SILO 入口 VIP `.207`（数据层 keepalived）」、`s3.endpoint` 与 DBAPI 导出改指 `.207`、§4 步骤 6c/6d/6e「备份仓库 + 每周全量保留 3 份 + 恢复验证」、§0.5「口令与占位符约定」、§6.3 放行 VRRP 112、§5 增 SILO VIP/备份可恢复两项、§7 增第 57/58 条｜**一轮**：按外部评审清单 v1.12 关闭 P0-01~P0-07 与 P1-01~P1-19（**仅 P1-07 Secret 环境变量化未采纳**，改为占位符 + `chmod 600` 最低要求）：Flink 补 `conf/masters`+`conf/workers`+三台免密 SSH、启动改 `start-cluster.sh`；固定 `high-availability.jobmanager.port: 6123`；S3 插件改 `plugins/s3-fs-hadoop/`；**checkpoint 配置键全面迁移到 Flink 1.20 新键**；RocksDB 与 io.tmp 分目录；补 4G→8G 全量期策略；SILO 三桶配额 + checkpoint/savepoint 清理；Doris 最小权限账号 + 多 FE 连接；新增 JM/TM/SILO 三类故障演练；｜PG 改 1 主 2 从、etcd 与 Redis 哨兵收回应用层、keepalived 150/120/100、§3.6 节点 gw03、§2.4 磁盘分区挂载（原 §2.4~§2.7 顺延为 §2.5~§2.8）、**VIP 号段 `.210/.211` → `.199/.200` + 新增 `.207`**；组件版本以 `DBAPI_Doris组件版本基线.md` 为准 |
| 日期   | 2026-10-25                                                                                                                                                                                                                                                                                                                                                 |
| 适用方案 | 本方案（最优方案）：DBAPI 数据 API 发布平台 + Doris 数仓底座                                                                                                                                                                                                                                                                                                                   |
| 目标系统 | AlmaLinux 10（RHEL 10 / 兼容内核 6.12）                                                                                                                                                                                                                                                                                                                          |
| 组件基线 | Doris 4.1.5 · Kafka 4.3.2 · Flink 1.20.5 LTS + CDC 3.6.0-1.20 · ZooKeeper 3.9.6 · etcd 3.7.2 · SILO RELEASE.2026-10-18 · Patroni 4.1.5 + PG 17 · APISIX 3.19.0 · Redis 7.4 · JDK **Temurin 17.0.21+9**（DBAPI 用系统 21）                                                                                                                                       |
| 部署形态 | **两层各 3 台**：数据/入湖层 3 台（Doris + Kafka + SILO + Flink HA 集群 + ZooKeeper）；应用服务层 3 台（APISIX + DBAPI + PG/Patroni 1 主 2 从 + Redis + etcd 3 成员 + 3 哨兵）；入口为 APISIX + keepalived 双 VIP（`.199` API / `.200` PG），**不前置 Nginx**                                                                                                                                       |
| 入湖范式 | **混合**：实时链路 Flink CDC(HA) → Kafka → Doris Routine Load → ODS；批处理链路 DS（独立集群）定时 DataX + Doris SQL 做 ODS→DWD→DWS→ADS 加工与对账                                                                                                                                                                                                                                    |

> 与本方案配套的架构设计、Doris 建表 DDL、DBAPI 元数据库 DDL、Routine Load 与 CDC SQL 见 `DBAPI_Doris数据平台详细设计.md`。本文档聚焦 AlmaLinux 10 上的落地命令、按节点拆分后的执行顺序，以及每一步的解释。架构总览见同目录 `DBAPI_Doris架构图.svg`。

---

## 0. 文档定位与架构边界

- **本方案**是交付方案；用户提供的"方案一"（Nginx 负载均衡 + DBAPI 节点 + 共享 MySQL/PG 三层集群）仅作对比，不进入本交付物。
- **入口层**：APISIX（3 实例，keepalived priority 150/120/100）+ VIP `192.168.110.199`（API 入口），统一对外提供 HTTP/HTTPS；不在 APISIX 前再加 Nginx。
- **数据面**：APISIX → 数据服务层（DBAPI ×3 实例）→ Doris ADS 层。
- **辅助组件**：PostgreSQL（元数据/授权库，Patroni 自动故障转移）、Redis（热查询缓存）、SILO（异步导出，S3 兼容对象存储）、Kafka（入湖缓冲）、Flink HA 集群（变更捕获）、ZooKeeper（Flink HA 协调）、etcd（APISIX 路由配置中心 + Patroni 选主仲裁）。
- **实时入湖链路**：Flink CDC（HA 集群捕获业务库变更）→ Kafka → Doris Routine Load 入仓 → ODS（见设计文档 2.7 节）。
- **批处理链路**：独立 DS 集群（3 节点 + 自有 MySQL）定时调度 DataX 抽数/对账 + Doris SQL 做层间加工（ODS→DWD→DWS→ADS）。
- **两层拆分原则**：数据/入湖层只承载"写+存"，应用服务层只承载"读+出口"，中间用防火墙把 Doris（9030/9050 等）仅对应用层开放。

### 0.1 节点命名与角色总览（两层 6 台 + 独立 DS 集群，IP 为实际规划）

| 层      | 主机名                      | IP                                | 角色定位                                                                                                 |
| ------ | ------------------------ | --------------------------------- | ---------------------------------------------------------------------------------------------------- |
| 数据/入湖层 | `doris-dw01`             | 192.168.110.201                   | Doris FE(Leader) + BE + Kafka broker1 + SILO(节点1) + Flink HA(JM+TM) + ZooKeeper(1)                   |
| 数据/入湖层 | `doris-dw02`             | 192.168.110.202                   | Doris FE(Follower) + BE + Kafka broker2 + SILO(节点2) + Flink HA(JM+TM) + ZooKeeper(2)                 |
| 数据/入湖层 | `doris-dw03`             | 192.168.110.203                   | Doris FE(Follower) + BE + Kafka broker3 + SILO(节点3) + Flink HA(JM+TM) + ZooKeeper(3)                 |
| 应用服务层  | `dbapi-gw01`（平台节点1）      | 192.168.110.204                   | APISIX + DBAPI + **PG 主(Patroni)** + Redis(主) + **etcd(1)** + 哨兵(1) + keepalived(VIP 主，priority 150) |
| 应用服务层  | `dbapi-gw02`（平台节点2）      | 192.168.110.205                   | APISIX + DBAPI + **PG 从(Patroni 同步从库)** + Redis(从) + **etcd(2)** + 哨兵(2) + keepalived(备，120)         |
| 应用服务层  | `dbapi-gw03`（平台节点3）      | 192.168.110.206                   | APISIX + DBAPI + **PG 从(Patroni 第二从库)** + Redis(从) + **etcd(3)** + 哨兵(3) + keepalived(备，100)         |
| 独立调度集群 | `DolphinScheduler-1/2/3` | **192.168.110.131 / .132 / .133** | DolphinScheduler Master/Worker 混合（3 节点）+ 自有 MySQL（不计入平台拓扑，通过网络对接 Doris / 源库 / DataX）                 |

> **v1.11 变更：应用层 2 台 → 3 台，连带三处结构性调整（不是简单加机器）**
>
> 1. **PG 改 1 主 2 从**：2 台时主库一宕，唯一幸存的从库升主，集群里**再没有任何从库**能确认同步复制 → Patroni 只能把 `synchronous_standby_names` 置空、退化为异步，**从切换那一刻起到旧主被修回之前，所有新提交的 RPO 都不再为 0**；若这台新主随后再坏一次，数据全丢（无副本可恢复）。3 台时主库宕机后**另有一台从库仍在**，Patroni 立即把它挂为新的同步从库，追平后即恢复 RPO=0——**降级窗口从"小时级（等旧主修回）"压到"秒级（追平差量）"**。附带收益：滚动维护（逐台重启打补丁）时始终保有 1 主 1 从，不必像 2 台那样先把集群退化成单机裸奔。
> 2. **etcd 从跨层投票改为全应用层**：原 `gw01+gw02+dw01` 三成员是"5 台约束下的最优解"，代价是 dw01 故障同时带走 Doris 与 1/3 仲裁。现在 3 台应用节点可自建奇数成员，**故障域与应用层完全一致**，dw01 宕机不再影响 APISIX 配置与 Patroni 选主。
> 3. **Redis 第 3 哨兵从 dw03 撤回应用层**：同理，dw03 那个哨兵只为凑 `quorum=2` 而跨层，现由 gw03 承担。数据节点因此少跑 2 个容器（etcd + 哨兵），防火墙少 3 条跨层规则。

**VIP 规划（2026-10-23 已确认与现网无冲突，定稿）**

| VIP               | 用途                                      |
| ----------------- | --------------------------------------- |
| `192.168.110.199` | API 服务入口（keepalived VI_1，随 APISIX 健康）   |
| `192.168.110.200` | PG 主库入口（keepalived VI_2，随 Patroni 主库漂移） |

> **VIP 与节点 IP 是两套地址，别混用**：`.199/.200` 只由 keepalived 以 `/24` 别在网卡上（`virtual_ipaddress`），**不属于任何一台主机的静态配置**——`/etc/hosts`、Patroni `connect_address`、APISIX 上游、`pg_hba` 的复制行**一律写节点真实 IP（`.204/.205/.206`）**，只有"外部调用方连 DBAPI"和"DBAPI 连 PG"这两处写 VIP。判断依据：`ip a` 里 VIP 带 `secondary` 标记，且跟着服务漂移；节点 IP 永远不动。
>
> 地址段刻意错开：**VIP 在低段 `.199/.200`、节点在 `.204~.206`**，现场一眼能分清"这是浮动地址还是主机地址"。v1.11 之前节点占用 `.199/.200`，这两个地址现已整体让给 VIP（因此 `.199/.200` 在文档其他位置**不应再出现为节点 IP**）。

上线前仍建议在 gw01 上做一次无冲突复检（三台网关同时在线时执行，避免误判）：

```bash
# 预期全部无响应；若任一 IP 有回包，说明已被占用，必须先改 VIP 再继续
arping -c 3 -I <业务网卡名> 192.168.110.199     # API 入口（应用层 keepalived）
arping -c 3 -I <业务网卡名> 192.168.110.200     # PG 主库（应用层 keepalived）
```

```bash
# SILO 入口 VIP 在**数据节点**上，须在 dw01/02/03 各执行一次
arping -c 3 -I <业务网卡名> 192.168.110.207     # v1.12 新增
```

- 六台同处 `192.168.110.0/24` 单网段；"两层"隔离不靠子网而靠 **firewalld ipset 按主机 IP 分组**（数据节点集 `data-nodes`、应用节点集 `app-nodes`、**调度节点集 `ds-nodes`**，见第 6 章）。
- **DS 集群也在本网段（2026-10-23 确认）**：这意味着**不能**用 `source address=192.168.110.0/24` 为 DS 放行——那会把 Doris 9030 / 8030 / 8040 与 SILO 9000 对网段内所有机器敞开，隔离设计即作废。正确做法是建 `ds-nodes` ipset，只放 DS 的 3 个节点 IP（§6.1，已确认为 `192.168.110.131/.132/.133`）。
- 数据层 3 台同构跑 Doris + Kafka + SILO + Flink HA + ZooKeeper；应用层 3 台同构跑 APISIX + DBAPI + PG + Redis + etcd + 哨兵。**两层各 3 台、都满足"奇数 + 容忍单台故障"**，数据节点不再承载任何应用层组件。
- 入湖层（Kafka/SILO/Flink/ZK）**并入数据层，不单独成机**——Kafka→Doris Routine Load 需近网络，二者同属数据面。Flink 以 HA 集群形态跨 3 数据节点部署，消除单实例单点。
- DS 集群独立存在，仅通过网络对接本平台，不共享元数据库、不计入拓扑。**DS 三台的 `/etc/hosts` 建议追加平台 6 台的主机名映射**（或 DataX/SQL 任务直接写 IP），避免 DNS 不可用时抽取任务失败。

> 小数据量起步可先将 PG/Redis/SILO 缩到单节点，FE/BE/Flink/APISIX/DBAPI 仍保持最小高可用数量。

### 0.2 组件职责对照（谁用、干什么、存什么）

**核心原则**：**Doris 才存业务/BI 数据；PostgreSQL 不存业务数据**，它是 DBAPI 自己的"账本"。

| 组件                   | 给谁用                                  | 角色             | 存的内容                                                          | 部署位置                                                                                                                       |
| -------------------- | ------------------------------------ | -------------- | ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| **PostgreSQL**       | DBAPI（唯一使用者）                         | 元数据库 / 授权库     | API 定义、数据源配置、账号权限、调用日志、发布记录                                   | dbapi-gw01 主 + **gw02/gw03 两台从**，**Patroni 托管自动故障转移**（synchronous_mode 开启；主库宕机后剩余从库立即接任同步，RPO 降级窗口仅"追平差量"的秒级）              |
| **Redis**            | DBAPI（唯一使用者）                         | 热查询缓存          | 高频查询结果集（带 TTL），避免重复回源 Doris                                   | dbapi-gw01 主 + gw02/gw03 从 + **3 哨兵**（gw01/gw02/gw03，quorum=2，全在应用层）                                                       |
| **etcd**             | **APISIX + Patroni（双用户）**            | 分布式配置中心 + 选主仲裁 | APISIX 全部路由/插件配置（key 前缀 `/apisix`）；Patroni leader lease 与集群状态 | **3 成员集群**：dbapi-gw01 + gw02 + gw03（全应用层，v1.11 起不再跨层），quorum=2                                                             |
| **keepalived**       | 外部调用方 / Flink / DBAPI                | VIP 浮动网关       | 无（只持有 VIP）                                                     | **两组互相独立**：① 应用层 3 台 VI_1=`.199`（API 入口，探 APISIX）、VI_2=`.200`（PG 主库，探 Patroni `/primary`）；② **数据层 3 台 VI_1=`.207`（SILO 入口，探本机 SILO 就绪，v1.12 新增，VRID 53）**；两组均 priority 150/120/100 + `weight -80` |
| **Kafka**            | 入湖链路（Flink CDC + Doris Routine Load） | 入湖缓冲队列         | 业务库 CDC 变更流（**不是** DBAPI 直接读写）                                | 数据层 3 台各 1 broker                                                                                                          |
| **SILO**（S3 兼容对象存储）  | DBAPI（导出）+ Flink（checkpoint/HA 元数据）  | 对象存储           | 大结果集导出文件 + Flink 状态/HA 存储                                     | 数据层 3 台分布式，**每台 2 个 drive（共 6 drive，满足 EC）**；上游 `minio/minio` 镜像已被删除，改用 `pgsty/silo`                                       |
| **Flink HA 集群**      | 入湖链路                                 | 实时变更捕获与转发      | CDC 作业运行时状态（落 SILO）                                           | 数据层 3 台各 1 JM+TM                                                                                                           |
| **ZooKeeper**        | Flink HA                             | 选主与元数据协调       | Flink HA 仲裁（quorum=2）                                         | 数据层 3 台各 1 实例                                                                                                              |
| **Doris**            | DBAPI（查询 ADS 层）                      | 数仓底座           | 业务/BI 数据（ODS→DWD→DWS→ADS）                                     | 数据层 3 台                                                                                                                    |
| **DolphinScheduler** | 批处理链路（独立集群）                          | 调度编排           | 工作流定义与执行历史（自有 MySQL）                                          | 独立 3 节点集群                                                                                                                  |


**etcd 与 ZooKeeper 的区别（易混，说明一下）**：两者都是"分布式协调"，但在本方案里**各管一摊、互不复用**——**etcd** 服务应用层：APISIX 把"有哪些 API、怎么转发、怎么鉴权"存在 etcd 里（三台 APISIX 读同一份，发布一次全网生效），同时给 Patroni 当仲裁（谁持有 lease 谁是 PG 主库）；**ZooKeeper** 只服务数据层的 Flink HA（JobManager 选主）。功能类似但协议、API、生态不同（etcd=gRPC/KV，ZK=ZAB/树形节点），不合并使用。

**一句话归属总结**

- DBAPI 的三大依赖 **PG（账本）、Redis（缓存）、SILO（导出）**——都只服务于 DBAPI（应用层）。
- **Kafka / Flink / ZooKeeper / Doris** 属于数据/入湖层，服务于"写+存"实时管道；DS 独立集群服务于"定时批处理"管道；二者与 DBAPI 的查询服务是独立链路。

### 0.2.1 DBAPI 调用日志保留策略（已确认）

调用日志全量写 PG，**不设保留策略会无限增长撑爆 PG 数据盘**。定版方案：

| 项    | 决策                                                                                                    |
| ---- | ----------------------------------------------------------------------------------------------------- |
| 表名   | `api_call_log`（与设计文档 3.4 节 DDL 一致；**不要**写成 `call_log`）                                                |
| 分区方式 | 按月 **RANGE 分区**（`PARTITION BY RANGE (called_at)`，每月一个分区）                                              |
| 保留时长 | **最近 90 天**（3 个整月分区；审计要求变化只改数字，结构不动）                                                                  |
| 清理方式 | 每月 1 日 DS 定时任务执行 `DROP TABLE api_call_log_yyyymm`——**秒级完成、不锁主表、不产生 VACUUM 压力**（这是分区相对 `DELETE` 的核心优势） |
| 建分区  | 每月 1 日同一任务预建下月分区（`CREATE TABLE ... PARTITION OF ... FOR VALUES FROM ... TO ...`）                      |

> 落地位置：`api_ddl.sql` 中 `api_call_log` 按分区表建（**主键必须是 `(id, called_at)`，把分区列包含进主键**，否则 PG 报 "unique constraint on partitioned table must include all partitioning columns"）+ DS 建"月度日志维护"工作流（两步：建下月分区 → 删 90 天前分区）。归档需要时可在 DROP 前先 `COPY (SELECT...) TO SILO 导出`。

### 0.3 资源规划（两层）

#### 0.3.1 节点硬件规格总表

| 节点类型                      | CPU      | 内存        | 系统盘                         | 数据盘                                                          | 网络                                             |
| ------------------------- | -------- | --------- | --------------------------- | ------------------------------------------------------------ | ---------------------------------------------- |
| 数据节点 ×3（doris-dw01/02/03） | **16 核** | **64 GB** | 200 GB（OS+软件+日志）            | **500 GB + 500 GB（两块独立数据盘）**                                 | **10 GbE**（数据层内部 Doris/Kafka/Flink 流量大，千兆会成瓶颈） |
| 应用节点 ×3（dbapi-gw01/02/03） | **8 核**  | **32 GB** | **500 GB（OS 与 `/data` 同盘）** | **—（无独立数据盘）** `/data/pg`、`/data/redis`、`/data/etcd` 都在根文件系统内 | 1 GbE 即可（VIP 需同二层；建议双网卡做管理/业务分离可选）             |
| DS 集群 ×3（独立，不计入本平台）       | 8 核      | 16~32 GB  | 100 GB                      | 500 GB（工作流日志/临时文件）                                           | 1 GbE，须放通到 Doris 9030 / 业务源库 / SILO            |

> **本表为实机规格（2026-10-22 用户确认），所有组件参数按此表重算**，逐组件推导见 `DBAPI_Doris硬件配置与参数调优.md` §1。旧版"数据节点 128G + 四块盘"的口径已作废。

#### 0.3.2 数据节点磁盘划分（关键：BE 独占一块物理盘）

数据节点同时跑 Doris BE（随机读 + Compaction 顺序写）、Kafka（高吞吐顺序写）、SILO（对象存储）、Flink 本地状态，**必须把 BE 单独放一块盘**，其余共享第二块盘：

| 盘          | 容量     | 挂载点           | 分配                                                                                 |
| ---------- | ------ | ------------- | ---------------------------------------------------------------------------------- |
| `/dev/sda` | 200 GB | `/`           | OS、Docker 镜像、各组件日志（约剩 120G）                                                        |
| `/dev/sdb` | 500 GB | `/data/doris` | FE meta ≤20G + **BE 480G（整盘，留 20G 缓冲）**                                            |
| `/dev/sdc` | 500 GB | `/data/store` | Kafka 120G、SILO 300G（d1/d2 各 150G）、Flink 本地状态 50G、ZK 10G（**etcd 已于 v1.11 迁出数据节点**） |

> **为什么这样分**：BE 的 Compaction 是持续的随机读+顺序写，一旦与 Kafka 追加、SILO checkpoint 抢同一块盘，`%util` 会长期打满（表现为 Routine Load Lag 抖动、查询 P99 飙升）。把 BE 单独隔离到 sdb 后，第二块盘上只剩"大块顺序写"，冲突量级低一档。**这是本方案唯一的物理 I/O 隔离手段，务必按此挂载**，不要图省事把 `/data` 做成一个目录。**逐条落地命令（认盘 → 分区 → 格式化 → fstab → 挂载验证 → SELinux 标签）见 §2.4。**

#### 0.3.3 内存账本（按 64G / 32G 实算）

| 节点类型                  | 规格        | 组件常驻承诺                                                                                                                    | 余量                    |
| --------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------- | --------------------- |
| 数据节点 doris-dw01/02/03 | 16C / 64G | **≈43.8 GB**：BE 25.6（`mem_limit=40%`）+ FE 5 + Kafka 3 + Flink 6（JM 2 + TM 4）+ SILO 1.2 + ZK 1 + OS 2（**etcd 已迁出，省 0.5G**） | ≈20.2 GB              |
| 应用节点 dbapi-gw01/02/03 | 8C / 32G  | **≈18.7 GB**：PG 9（从库同规格）+ DBAPI 3.5 + Redis 2.5 + APISIX 1 + etcd 0.5 + OS 2                                              | ≈13.3 GB（三台同构，单台账本不变） |

- **BE 的 `mem_limit` 必须压到 40%**：默认 90% 意味着 BE 可涨到 57.6G，而共置机上 Kafka/Flink/SILO/ZK/FE 已占 16G，一旦导入峰值就会 OOM Kill（通常先被杀的是 SILO/Kafka，表现为"checkpoint 写失败"这种看似无关的故障）。40% = 25.6G 是这台机器的稳态上限。**注意：BE 是 C++ 进程，`JAVA_OPTS -Xmx` 对它无效**，详见 §3.1 步骤 3。
- **容量换算**：BE 480G × 3 节点 = 1440G 裸盘 ÷ 3 副本 = **480G 单副本可用**（稳态上限）。但**单 BE 故障后剩 2 台各扛 1.5 份副本**，留 20% 余量后每台可用 384G，故**故障自愈线 = 384 ÷ 1.5 ≈ 256G/节点（53%）**，对应单副本总量 **768G**。按设计文档 §2.8 的 500G 原始量测算（三副本 ≈885G、每节点 ≈295G）**稳态能跑但已越自愈线**：坏一块盘副本补不回来。扩容判据见《硬件配置与参数调优》§9。
- **Kafka 留存**：按峰值写入 × 留存时长估算，本方案取 48h + 每分区 5G 硬上限（§3.1 步骤 6），避免吃满 120G 配额。
- **Flink checkpoint 落 SILO**；`state.backend=RocksDB` 的本地目录放 `/data/store/flink/tmp`（限 50G），不要放系统盘（写满 `/` 会连带 docker 与 Doris 日志一起挂）。

#### 0.3.4 内核与固件注意

- **关闭 THP（Transparent Huge Pages）**：Doris BE / Redis 在 THP 下易现延迟抖动，AlmaLinux 10 默认开启。**落地：§2.5**（立即关闭 + `disable-thp.service` 开机固化）。
- **NUMA**：BE 大内存下避免跨节点内存访问抖动。**落地：§2.3**（`vm.zone_reclaim_mode=0`）+ **§3.1 步骤 5**（多路服务器可选 `numactl --interleave=all` 启动 BE）。
- **时间同步（已完成，无需处理）**：六台已配置时间同步，满足 CDC binlog 位点、Kafka 时间戳对时要求；新增节点时须同样接入（§2.2）。
- **SWAP**：关闭 swap（`swapoff -a` + 清 fstab，§2.3），并设 `vm.swappiness=10` 兜底——若后续因故恢复 swap 分区，内核也极少换出。
- **内核内存/网络参数**：`vm.max_map_count=2000000`（Doris BE 硬性要求，默认值会导致 BE 起不来）、`vm.overcommit_memory=1`（Redis 硬性要求）、连接队列/临时端口/TCP 缓冲扩容（APISIX/Kafka/Flink 高并发与吞吐）。**落地：§2.3**。
- **文件句柄 655350**：Doris 官方要求；且 `limits.conf` 对容器无效，须配 Docker `default-ulimits`。**落地：§2.6 + §2.8**。
- **容器日志轮转**：docker 默认 json-file 不限大小，Kafka/PG 数周可写满系统盘。**落地：§2.8**（`max-size 100m × 5`）。
- **数据盘挂载**：XFS + `noatime`，SSD/NVMe 调度器 none/mq-deadline、`fstrim.timer` 周期 TRIM。**落地：§2.4**（数据节点两挂载点 `/data/doris`+`/data/store`；应用节点单盘同盘，只建目录不挂载，见 2.4.12）。
- **CPU 频率与低延迟（固件/虚拟化层为主）**：宿主机 BIOS 电源策略设 **Performance**（勿用 balanced/power-saving），关闭深度 C-state/C1E——深 C-state 唤醒延迟（数十~数百 µs）对 BE 查询、APISIX 转发的 P99 影响远大于内核参数；虚拟机对应宿主机侧 CPU 预留/穿透（§0.4.2）。客户机内验证：`cat /sys/devices/system/clocksource/clocksource0/current_clocksource` 应为 **tsc**（hpet 说明时钟源退化，需检查宿主机配置）。
- **RAID 卡/NVMe 固件**：若数据盘走 RAID 卡，须 write-back + BBU（掉电保护）模式，write-through 会把 Kafka/BE 顺序写吞吐打到地板；NVMe 关注厂商固件更新。虚拟化环境此层由宿主机承担。
- **数据节点内存是共置场景的核心约束**：BE + FE + Kafka + Flink + SILO + ZK 常驻约 43.8G/64G（etcd 已于 v1.11 迁出）。控制手段**不是**给 BE 设 JVM 堆（BE 是 C++，`JAVA_OPTS` 无效），而是 `be.conf` 的 `mem_limit = 40%`，并配合 Kafka 保留上限、Flink checkpoint 间隔放宽（详见 `DBAPI_Doris硬件配置与参数调优.md` §1/§4）。

### 0.4 虚拟化环境部署要求

若平台运行在虚拟机（VMware vSphere / KVM / 云主机），除满足 §0.3 硬件规格外，还须满足以下虚拟化专项要求。**两层拓扑的 HA 价值建立在"副本不共物理故障域"之上，虚拟化层若配置不当，HA 假设会直接破产。**

#### 0.4.1 反亲和（最关键）

- **两层各 3 台都必须落在不同物理宿主机**：数据节点（doris-dw01/02/03）、应用节点（dbapi-gw01/02/03）各自分散，且**应用层三台不要与数据层三台共用同一批宿主机**（否则一次宿主机故障同时带走 etcd 多数派与 Doris 副本）。
- 否则单台宿主机宕机会同时带走多个副本，FE 选举组 / BE 3 副本 / keepalived VIP 全部失效。
- 云环境用可用区或放置组（anti-affinity rule）保证；VMware 用 DRS 反亲和规则（separate virtual machines）。

#### 0.4.2 计算与内存

- **CPU 超分受限**：数据节点 BE / Flink 对 CPU 敏感，建议 vCPU 与物理核 **1:1 或 ≤2:1**，并设置 **CPU 预留（reservation）**，避免邻居争抢导致 GC 停顿。
- **内存预留**：数据节点 64G 须设 **100% 内存预留**，禁用内存超分（oversubscription）与气球驱动（balloon），防止 BE 被回收内存触发 OOM。
- **vNUMA**：数据节点开启 vNUMA 并让 Guest 感知 NUMA，等价于裸机 §0.3.4 的 NUMA 绑核。

#### 0.4.3 磁盘

- **磁盘格式**：Doris BE / Kafka / SILO 所在数据盘须为**厚置备急零（eager zeroed thick）**&#x6216; SSD 虚拟盘，**禁用精简置备（thin）**——薄盘在写入高峰突发扩容会卡顿甚至 stall。
- **独立虚拟盘 + 独立数据存储**：三块盘分别挂载不同 datastore / 物理磁盘，避免同物理盘 I/O 互踩（对应裸机 §0.3.2 分盘）。
- **PVSCSI 控制器**：数据盘用准虚拟化 SCSI 控制器，提升 I/O 吞吐。
- **避免 VM 快照**：Doris 运行期间不要对 BE 盘做虚拟机快照（写放大 + 合并风暴）；备份用 Doris 自身备份/导出，而非 VM 快照。

#### 0.4.4 网络

- **半虚拟化网卡**：虚拟网卡用 VirtIO / VMXNET3 并开启多队列（multi-queue），提升吞吐、降低延迟。
- **keepalived VIP（VRRP）放行**：虚拟交换机须允许 VRRP，**不过滤虚拟 MAC**——否则 VIP 漂移失败。VMware 标准交换机需开"混杂模式 + MAC 地址更改 + 伪传递"（promiscuous / MAC change / forged transmits），或改用 **单播 VRRP（unicast_peer）** 规避。
- **同二层**：应用节点须在同一 L2 广播域，VIP 才能漂移；跨子网需 L2 扩展或改单播。
- **带宽**：数据层 10 GbE 须由虚拟网络上联提供 ≥10G，否则内部 Doris / Kafka 流量成瓶颈。

#### 0.4.5 时钟与运维

- **时钟漂移**：VM 时钟易漂，chrony 必修并启用 `rtcsync`；禁用宿主机对 Guest 的周期性时钟回写；避免 `suspend` 虚拟机。
- **避免热迁移重负载节点**：vMotion / 跨机迁移会让 BE / Flink 瞬间停顿（STUN），触发 checkpoint / 副本切换；确需迁移须挑低峰并暂停写入。
- **模板化部署**：可用同一 gold image 克隆（改 hostname / IP 即可），但须重新生成 Doris / etcd / ZooKeeper 的 cluster_id 与 UUID，避免冲突；且**数据节点与应用节点的模板不能混用**（磁盘布局、JDK 双版本、SELinux 上下文都不同）。

### 0.5 口令与占位符约定（v1.12 新增，必读）

**本文所有 `<强口令>`、`<SILO 用户名>`、`<VRRP认证口令>`、`<强用户名>` 之类尖括号内容都是占位符，不是可直接使用的默认值。** 严禁照抄成生产密码，也不要把填好的真实密码提交进 Git、贴进工单或留在 shell history 里。

| 凭据 | 本文出现位置 | 生产来源建议 |
|---|---|---|
| SILO `MINIO_ROOT_USER/PASSWORD` | §3.1 步骤 7 | 密码库；同时写入 Flink `s3.access.key/secret.key` 与 DBAPI S3 配置，三处必须一致 |
| Flink `s3.access.key/secret.key` | §3.1 步骤 9 | 同上（**同一套凭据**，不要另建） |
| keepalived `auth_pass` | §3.4 / §3.1 步骤 7b | 密码库；同一 VRRP 实例组三台必须一致 |
| PG `replicator` / `patroni` 口令 | §3.4 步骤 4 | Patroni 配置文件 `chmod 600`，属主 `postgres` |
| Doris `root` 口令 + `dbapi_ro`/`ds_rw` | §4 步骤 10 | 密码库；**DBAPI 只给 `dbapi_ro`（SELECT on ADS），DS 只给 `ds_rw`** |

**落地要求（最低限度）**：

```bash
# 1) 含口令的配置文件一律收权限，禁止 world-readable
chown postgres:postgres /data/pg/patroni.yml && chmod 600 /data/pg/patroni.yml
chmod 600 /etc/keepalived/keepalived.conf

# 2) 命令里不写明文口令：改用 mysql 的选项文件（-p 后接空格的写法仍会被 ps 看到）
cat > /root/.my.cnf <<'EOF'
[client]
user=root
password=<强口令>
EOF
chmod 600 /root/.my.cnf
mysql -h 192.168.110.201 -P 9030 -e "SELECT 1"    # 不再需要 -p

# 3) 交付前扫一遍有没有把真实口令写成默认值
grep -nE "(dbapi123|dbapi456|minioadmin|Password123|123456)" *.md || echo "OK: 无示例口令残留"
```

> Flink 侧如需环境变量注入（`s3.access.key: ${FLINK_S3_ACCESS_KEY}`），**1.20 的 `flink-conf.yaml` 支持 `${VAR}` 展开**，但要求变量在 **JM/TM 启动环境**里存在（systemd 单元或 `flink-conf.yaml` 同目录的 `flink-conf.env`）；只 export 在当前 shell 里，用 `start-cluster.sh` 远程拉起远端时会读不到。**本版未强制改为环境变量（用户决策），但必须做到"占位符不落真实值 + 配置文件 600"**。

---

## 1. AlmaLinux 10 特有关注点（概念速览，先读后做）

1. **firewalld 默认开启，且不得关闭**：所有端口放行用 `firewall-cmd` 持久化规则（第 6 章统一处理）。**不要图省事 `systemctl stop firewalld`，更不要 `systemctl disable firewalld`（开机不自启 = 永久关）**——本方案六台与 DS 三台同处一个 `192.168.110.0/24` 单网段，层间隔离全靠 ipset 按主机分组实现；一旦关闭，Doris 9030/8030/8040、SILO 9000、Kafka 9092/9093、PG 5432、Redis 6379、etcd 2379 全部对网段内所有机器裸奔，且叠加 Doris `root` 空口令（§4 步骤 10 设口令之前）即等于内网任意主机可直连删库。**排查连通性用 `nc -vz` 即可；确需临时放开，可 `systemctl stop firewalld` 测完立刻 `systemctl start firewalld` 恢复，但绝不 `disable`**（详见 §7-48）。
2. **SELinux 默认 enforcing**：Docker/Podman bind mount 写不进时按 §7-2 处理（`chcon`），不要直接 `setenforce 0`。
3. **`yum` 即 dnf5 别名**：EL10 的 `yum` 命令底层是 dnf5，本手册统一用 `yum`；仅包名 `dnf5-plugins-core` 保留原名。
4. **podman 默认存在**：与本手册 Docker 命令并存无冲突；若不用 Docker 可全量替换为 podman。
5. **THP 默认开启、Java 17 已从仓库移除**：前者按 §2.5 关闭固化，后者按 §2.7 双 JDK 方案处理。

---

## 2. 通用系统准备（6 台平台节点分别执行，命令完全一致）

> **执行说明**：本章命令在 `doris-dw01/2/3` 与 `dbapi-gw01/2/3` 六台上**各执行一遍**（除 2.1 主机名按各台改）。这是部署前的"地基"阶段，先在全台都做完，再做第 3 章的逐节点组件部署。DS 集群节点不属本平台，按 DS 官方文档准备即可。

### 2.1 设置主机名与 hosts 解析

```bash
# 各台把主机名换成对应值：doris-dw01 / doris-dw02 / doris-dw03 / dbapi-gw01 / dbapi-gw02 / dbapi-gw03
hostnamectl set-hostname doris-dw01

# 六台都写入同一个 /etc/hosts（跨子网也能用主机名互访）
cat >> /etc/hosts <<'EOF'
192.168.110.201 doris-dw01
192.168.110.202 doris-dw02
192.168.110.203 doris-dw03
192.168.110.204 dbapi-gw01
192.168.110.205 dbapi-gw02
192.168.110.206 dbapi-gw03
EOF
```

**解释**：`hostnamectl set-hostname` 改本机标识，重启后依然生效。`/etc/hosts` 让六台用主机名互访——后面 Doris/PG/Redis/Flink 配置写主机名比写 IP 更易维护。**Flink HA 尤其依赖此 hosts**：各节点 `jobmanager.rpc.address` 写自己主机名，ZK 选出的 Leader 地址就是该主机名，靠 /etc/hosts 全节点可解析。六台 hosts 内容必须一致（六台同在 192.168.110.0/24）。


### 2.2 时钟同步

> ✅ **本环境已完成时间同步配置，本节无需执行**。命令保留，供后续新增节点（扩 BE、加 DS 节点）时参照。

```bash
timedatectl set-ntp true
timedatectl status
```

**解释**：`set-ntp true` 启用 chrony 校时。`timedatectl status` 应显示 `System clock synchronized: yes`。六台时间必须一致，否则 Doris 选主异常、ZK 会话抖动、CDC 乱序（binlog 位点与事件时间都依赖时钟）。

### 2.3 关闭 swap、调低 swappiness 与内核参数（内存 / 网络 / NUMA）

```bash
swapoff -a
sed -i '/swap/d' /etc/fstab
cat >> /etc/sysctl.conf <<'EOF'
vm.swappiness=10
vm.zone_reclaim_mode=0
kernel.numa_balancing=0
vm.max_map_count=2000000
vm.overcommit_memory=1
vm.min_free_kbytes=1048576
vm.dirty_background_bytes=268435456
vm.dirty_bytes=1073741824
net.core.somaxconn=65535
net.ipv4.tcp_max_syn_backlog=65535
net.ipv4.ip_local_port_range=1024 65535
net.core.rmem_max=16777216
net.core.wmem_max=16777216
net.ipv4.tcp_rmem=4096 87380 16777216
net.ipv4.tcp_wmem=4096 65536 16777216
EOF
sysctl -p
```

**解释**（逐项对应组件的硬性要求或实测收益）：

- `vm.max_map_count=2000000`：**Doris BE 硬性要求**。BE 内大量 mmap（RocksDB/ Tablet 存储、堆外内存映射），默认 65530 会导致 BE **直接启动失败**或运行中 crash——这是 Doris 部署最高频的"起不来"根因。
- `vm.overcommit_memory=1`：**Redis 官方要求**。默认 overcommit=2 时 fork（RDB/AOF 重写）可能因内存保守策略失败；Redis 日志出现 "WARNING overcommit_memory is set to 0" 即为此项未配。
- `vm.min_free_kbytes=1048576`：预留 1GB 最低空闲内存，避免大流量导入/查询时内核因原子内存分配失败（"page allocation failure"）丢包或进程被 OOM。
- `kernel.numa_balancing=0`：关闭内核自动 NUMA 页迁移。自动 balancing 以缺页中断为代价换"局部性"，对 BE/APISIX 这类低延迟服务是纯抖动源；配合 `zone_reclaim_mode=0` 与可选 `numactl`（§3.1 步骤 5）构成完整 NUMA 策略。
- `vm.dirty_background_bytes=256MB` / `vm.dirty_bytes=1GB`：**用绝对字节控制脏页，不用比例**。比例制在 64G 机器上等于"background 起刷阈值 3.2G、硬顶 12.8G"，而第二块盘上 Kafka 追加 + SILO checkpoint + Flink RocksDB 同时写，脏页会先在内存里堆到 GB 级再集中刷盘，造成周期性写毛刺（Routine Load Lag 抖动、查询 P99 飙升）。**注意比例与字节互斥**——设了 `dirty_bytes` 会把 `dirty_ratio` 自动置 0，两者不要同时写。**应用节点差异项**：`vm.min_free_kbytes=524288`、`vm.dirty_background_bytes=134217728`（128MB）、`vm.dirty_bytes=536870912`（512MB）。
- `net.core.somaxconn=65535 / tcp_max_syn_backlog=65535`：全连接/半连接队列上限。APISIX 对外入口与 Redis 高并发短连接场景，默认 128/1024 会在突发流量下丢连接（Redis 启动会打 "TCP backlog" 警告）。
- `net.ipv4.ip_local_port_range=1024 65535`：扩大本地临时端口范围。DBAPI→FE×3、Flink→Kafka×3、Routine Load 等大量 outbound 长连接 + 频繁新建时，默认范围（32768~60999）可能耗尽临时端口（"Cannot assign requested address"）。
- `net.core.rmem_max/wmem_max` 与 `tcp_rmem/tcp_wmem`：TCP 收发缓冲上限提到 16MB。Kafka 跨节点复制、Flink shuffle、Doris BE 间副本同步都吃大缓冲——缓冲过小会限制单连接吞吐，万兆网下尤其明显。

**数据盘**：数据节点必须把两块 500G 盘挂成**两个独立文件系统**（`/data/doris` 给 BE 独占、`/data/store` 给 Kafka/SILO/Flink/ZK），应用节点只有一块 500G 盘、OS 与 `/data` 同盘，**不分区不挂载**（见 2.4.12）。分区、格式化、UUID/fstab、SELinux 标签的完整命令见 **§2.4**——本节不再重复写，避免两处写法打架。


### 2.4 数据节点磁盘分区与挂载（doris-dw01/02/03 逐台执行；应用节点差异见 2.4.12）

> **本节是 §3.1 步骤 2 的硬前置**。`/data/doris`、`/data/store` 必须是**已挂载的独立文件系统**；若只是 `mkdir` 出来的普通目录，所有数据会写进 200G 系统盘——通常第 3 章装到一半 `/` 就满，Doris、docker、系统日志一起挂，而此时数据已经写进去，返工要从清库重来。**三台逐台执行，命令只在盘符上不同。**
>
> 挂载规划与容量分配的依据见 §0.3.2（`sdb`→`/data/doris` 给 BE 独占，`sdc`→`/data/store` 给其余组件）。

#### 2.4.1 认盘：先确认哪块是数据盘

`sda/sdb/sdc` 的编号来自内核枚举顺序，**不保证与物理槽位对应**；NVMe 机器是 `nvme0n1/nvme1n1/nvme2n1`，挂过 RAID 卡又可能整体后移一位。挂错的代价是格掉系统盘，所以这步不能跳。

```bash
lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT,MODEL   # 看容量、文件系统、挂载点
findmnt -no SOURCE /                             # 根分区到底在哪个设备
blkid                                            # 现有分区/文件系统签名
```

判定：两块 **≈500G 且 `MOUNTPOINT` 为空**的就是数据盘；根分区所在设备（多为 `sda2`/`sda3`，或整块 `sda`）**绝对不能碰**。

> 两块 500G 容量相同，谁做 `/data/doris`、谁做 `/data/store` 在功能上无差别——**决定身份的是 fstab 里的 UUID，不是盘符**（2.4.5）。但请把"哪块盘给了哪个挂载点"记进台账，将来换盘要按台账补回。
>
> 设备名拿不准时用物理路径反查：
>
> ```bash
> ls -l /dev/disk/by-path/ | grep -v part
> udevadm info --query=property --name=/dev/sdb | grep -E 'ID_PATH|ID_MODEL'
> ```

**能不能把盘符固定下来？不能，也不必。**

| 想法                        | 可行性     | 说明                                                          |
| ------------------------- | ------- | ----------------------------------------------------------- |
| 用 udev 把 `sdc` 重命名成 `sda` | **做不到** | 块设备的 `NAME=` 已被 udev 禁用——内核里的设备名与用户空间必须一一对应，改了会破坏 `/sys` 映射 |
| 调驱动加载顺序 / BIOS 启动顺序       | 勉强能做到   | 升级内核、换 PCIe 槽、加一块盘、插个 U 盘就会变，等于把脆弱性藏起来                      |
| 只装一块盘                     | 不成立     | 本方案就是三块盘                                                    |
| **让系统不依赖 `sdX`**          | **正解**  | fstab 用 UUID（跟文件系统走），人读路径用 `by-id`（跟盘走）                     |

`/dev/disk/` 下有三套稳定视图，按用途选：

```bash
ls -l /dev/disk/by-uuid/     # 跟文件系统走：重新分区不变，重新 mkfs 会变
ls -l /dev/disk/by-id/       # 跟盘走：含型号+序列号（或 WWN），换槽位也不变 ← 认盘用它
ls -l /dev/disk/by-path/     # 跟 PCI 路径走：槽位固定时最稳，换槽就变
```

**fstab 用 `by-uuid`，认盘核对用 `by-id`**，两者不混用。如果确实想要一个短名（比如写脚本时），用 udev 按 WWN 建**符号链接**（注意是 `SYMLINK+=`，不是改名）：

```bash
# 先取 WWN（值形如 0x5000c500a1b2c3d4）
udevadm info --query=property --name=/dev/sdb | grep -E 'ID_WWN|ID_SERIAL'

# /etc/udev/rules.d/70-data-disks.rules
SUBSYSTEM=="block", ENV{ID_WWN}=="0x5000c500a1b2c3d4", ENV{DEVTYPE}=="disk", SYMLINK+="doris-data"
SUBSYSTEM=="block", ENV{ID_WWN}=="0x5000c500a1b2c3d4", ENV{DEVTYPE}=="partition", ENV{ID_PART_ENTRY_NUMBER}=="1", SYMLINK+="doris-data1"
```

```bash
udevadm control --reload && udevadm trigger
ls -l /dev/doris-data /dev/doris-data1        # 无论盘序怎么漂，这两个名字永远指向同一块盘
```

规则**按 WWN 匹配、绝不按 `sdX` 匹配**——按 `KERNEL=="sdb"` 写的规则，盘序一漂就指向别的盘，比不写更危险。

#### 2.4.2 格盘前的最后一道闸

```bash
wipefs -n /dev/sdb /dev/sdc        # -n 只探测，不擦除
```

- **输出为空** → 全新裸盘，继续。
- **有输出**（`xfs`/`ext4`/`gpt`/`dos` 等）→ 盘上已有签名。**先停下来确认它是不是复用盘**；确认可覆盖后执行 `wipefs -a /dev/sdb` 清干净再分区。不先清会出现"分区表是新的、文件系统签名还是旧的"混合状态，`mkfs` 与 `mount` 都会报错。

> 再核对一次：`findmnt -no SOURCE /` 的输出**不等于** `/dev/sdb`、`/dev/sdc` 及其分区。这一条比任何检查都直接。

#### 2.4.3 建 GPT 分区表 + 单分区

```bash
parted -s /dev/sdb mklabel gpt
parted -s /dev/sdb mkpart primary xfs 0% 100%
parted -s /dev/sdb name 1 data_doris       # GPT 才支持分区名，便于 lsblk 识别
parted -s /dev/sdc mklabel gpt
parted -s /dev/sdc mkpart primary xfs 0% 100%
parted -s /dev/sdc name 1 data_store

partprobe /dev/sdb /dev/sdc                # 让内核重读分区表
lsblk /dev/sdb /dev/sdc                    # 应出现 sdb1 / sdc1
```

- **为什么不直接格式化整盘**：① `lsblk` 一眼看出用途；② 以后扩容只改分区表（`growpart`）即可，不必重装；③ 监控/备份工具对"裸盘无分区表"支持普遍更差。
- **用 `0% 100%` 而不是写死字节数**：parted 会按 1MiB 自动对齐（默认 `opt` 策略）。手写 `0 500GB` 容易落在 512B 边界造成 4K 不对齐，XFS 写入性能直接掉一档。
- `parted -s` 是批处理模式（不交互）。若报 `unrecognised disk label`，多为旧盘残留分区表 → 回 2.4.2 用 `wipefs -a` 清理。

#### 2.4.4 格式化 XFS

```bash
mkfs.xfs -L data_doris /dev/sdb1
mkfs.xfs -L data_store /dev/sdc1
```

- **选 XFS 不选 ext4**：Doris 官方推荐。BE 的 Segment 是"写一次、读多次"的大文件，XFS 的 extent 分配与并行写更好、元数据开销更轻；本方案不需要缩容，XFS 只能扩不能缩的短板不影响。
- **第一次故意不加 `-f`**：盘上若有残留文件系统，`mkfs.xfs` 会报 `appears to contain an existing filesystem` 并中止——**这正是我们要的保护**。人工确认后可覆盖时，再加 `-f` 重跑。
- **不要传 `-d stripe=` / `-l` 等条带参数**：虚拟化环境（vSphere/KVM）后端 RAID 条带对客户机不可见，凭猜测填只会造成写放大；直通 NVMe 也保持默认。
- `-L` 打标签后 `lsblk -f` 能直接看出 `data_doris` / `data_store`，换盘排障省事。

#### 2.4.5 取 UUID 并写入 fstab

> **不要手抄 UUID**。UUID 是 36 个字符的 `8-4-4-4-12` 十六进制串，手工摘录必然漏段——2026-10-23 现场就因为把 `blkid` 的整行输出摘成了 `sdb1-ae19c0bf-c580--418792dee760`（带了设备名前缀、中间两段丢失），开机直接掉进 emergency mode 等满 90 秒。**下面全部用变量自动写入**。

```bash
# 1) 取值（只输出 UUID 本身，不带 /dev/xxx: 前缀）
UD=$(blkid -s UUID -o value /dev/sdb1)
US=$(blkid -s UUID -o value /dev/sdc1)
echo "doris=[$UD]"; echo "store=[$US]"

# 2) 当场校验：必须是标准 8-4-4-4-12，取空或格式不符立即停
for v in "$UD" "$US"; do
  [[ $v =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] \
    && echo "OK  $v" || echo "BAD [$v] —— 回 2.4.3/2.4.4 补分区或格式化"
done

# 3) 自动写入（heredoc 不带引号，变量才会展开；带了引号就得手抄，正是出错根源）
cp /etc/fstab /etc/fstab.bak.$(date +%F)
cat >> /etc/fstab <<EOF
# ---- Doris 数据盘（部署手册 §2.4）----
UUID=$UD  /data/doris  xfs  defaults,noatime  0 0
UUID=$US  /data/store  xfs  defaults,noatime  0 0
EOF

# 4) 落盘后复核一眼，确认写进去的是 36 字符而不是别的东西
grep -E '/data/(doris|store)' /etc/fstab
```

> **`BAD []`（值为空）时先别急着重新格式化**——如果这台机器之前已经配过 fstab 且写进去过 UUID，空值通常只是"`sdX` 这个名字不存在了"（盘序漂移），而不是文件系统没了。用 `blkid | grep <UUID前8位>` 确认文件系统还在，然后走 2.4.10 的「emergency 抢救流程」去掉前缀即可。

校验必须通过再往下。`BAD` 的三种典型来源：① 分区刚建、`mkfs` 还没跑完就 `blkid`，取到空值；② 把 `blkid /dev/sdb1` 的整行（`/dev/sdb1: LABEL="..." UUID="..."`）当值抄；③ 重新格式化过一次，`blkid` 缓存还是旧 UUID（`blkid -p` 或 `udevadm settle` 后再取）。

挂载选项逐项取舍（每项都有代价，不要随手加）：

| 选项                                | 用 / 不用 | 原因                                                                                  |
| --------------------------------- | ------ | ----------------------------------------------------------------------------------- |
| `noatime`                         | **用**  | 读文件不更新访问时间，省一次元数据写。BE 频繁遍历 tablet 目录，收益直接。                                          |
| `discard`                         | **不用** | 在线 TRIM 会把"删除"变成同步 IO，删大 tablet 时出现秒级卡顿。改用 `fstrim.timer` 周期回收（2.4.8）。              |
| `nofail`                          | **不用** | 故意不加。加了之后数据盘挂不上系统照常启动、Doris 在空目录上照常写，**故障被藏进系统盘**；不加则 mount 失败直接进 emergency，问题当场暴露。 |
| 额外 XFS 调优（`allocsize`/`logbsize`） | **不加** | 默认值对本场景已足够，调错反而增碎片。                                                                 |

> **fstab 写错会让机器起不来**（UUID 打错、字段少一个、多一个空格都会进 emergency mode）。所以写完**先 `mount -a` 验证，通过后再重启**；`mount -a` 报错就立刻改，别带着错重启。

#### 2.4.6 挂载并验证（含重启复验）

```bash
mkdir -p /data/doris /data/store
mount -a
findmnt /data/doris /data/store
df -hT /data/doris /data/store
```

期望输出（`SOURCE` 必须是 `sdb1`/`sdc1`；500 GB 十进制 = 465.7 GiB，`df -h` 显示 **≈466G**）：

```
TARGET        SOURCE    FSTYPE OPTIONS
/data/doris   /dev/sdb1 xfs    rw,noatime,attr2,inode64,logbufs=8,logbsize=32k
/data/store   /dev/sdc1 xfs    rw,noatime,attr2,inode64,logbufs=8,logbsize=32k
```

**判定只有一条**：`SOURCE` 不是 `sdb1`/`sdc1`（比如显示 `sda3`，或容量是 200G/几十 G），说明挂载没生效——**此时绝不能继续装 Doris**，否则数据全部落进系统盘。

**必须重启复验一次**：`reboot` 后重新执行 `findmnt /data/doris /data/store`，两个挂载点仍在、`df` 仍是 ≈466G，才算 fstab 真写对。`mount -a` 能过但 fstab 有隐性错误（UUID 大小写、末列少 `0 0`）的情况，只在重启时才暴露。

#### 2.4.7 建目录结构并打 SELinux 标签

目录一次建全，避免后面各步骤零散 `mkdir` 漏建；**标签必须在容器启动前打好**，否则第一次起 Kafka/SILO/ZK 就是 permission denied。

```bash
# Doris：FE/BE 是宿主机进程 → var_lib_t（标成 container_file_t 会直接读不了自己的目录，见 §7-2）
mkdir -p /data/doris/fe/meta /data/doris/be/storage
chcon -Rt var_lib_t /data/doris

# 容器挂载目录 → container_file_t
mkdir -p /data/store/kafka/data \
         /data/store/silo/d1 /data/store/silo/d2 \
         /data/store/zookeeper/data /data/store/zookeeper/log \
         /data/store/flink/tmp
chcon -Rt container_file_t /data/store
```

| 目录                                 | 承载                            | 配额（§0.3.2） |
| ---------------------------------- | ----------------------------- | ---------- |
| `/data/doris/fe/meta`              | FE 元数据（BDBJE）                 | ≤20G       |
| `/data/doris/be/storage`           | BE 数据（唯一大额占用）                 | 480G       |
| `/data/store/kafka/data`           | Kafka 日志段                     | 120G（双闸控制） |
| `/data/store/silo/d1`、`d2`         | SILO 对象存储（两目录凑 EC 最少 4 drive） | 各 150G     |
| `/data/store/zookeeper/data`、`log` | ZK 快照 / 事务日志                  | 10G        |
| `/data/store/flink/tmp`            | RocksDB localdir + spill      | 50G        |

> 以后新增目录时**建完要重跑一次 `chcon`**——`chcon` 只作用于执行当时的已有文件，不会给未来子目录自动打标。同理，§3.1 步骤 2 用 `cp -a` 并入 Doris 二进制后（`cp -a` 保留归档里的旧上下文），还要再执行一次 `chcon -Rt var_lib_t /data/doris`。

#### 2.4.8 SSD/NVMe 调度器与周期性 TRIM（含虚拟机判定）

**先判环境，再决定做不做**——这一步在虚拟机上结论经常是"什么都不用做"：

```bash
systemd-detect-virt                              # kvm / vmware / microsoft / none
lsblk -d -o NAME,SIZE,ROTA,SCHED,MODEL           # ROTA=0 非旋转；1 旋转
cat /sys/block/sdb/queue/scheduler               # 看方括号落在哪个值上
```

| 现象                             | 含义                               | 动作               |
| ------------------------------ | -------------------------------- | ---------------- |
| `ROTA=0` 且 `[none]`            | 已是最优（AlmaLinux 10 对非旋转设备默认如此）    | **什么都不做**        |
| `ROTA=1`                       | 被当成旋转盘（虚拟盘很常见，标志由 hypervisor 决定） | 见下方"要不要改"        |
| `systemd-detect-virt` 非 `none` | 盘是 qcow2/vmdk/vhdx 虚拟盘           | 调度器调优收益极小，瓶颈在宿主机 |

**调度器要不要改**：`none` 的含义是"直接使用硬件队列"，对 SSD/NVMe 就是最优解；`mq-deadline` 的价值在保证请求延迟上界，SSD 本身没有寻道，收益很小；`bfq` 是为旋转盘设计的公平队列，SSD 上反而增加开销。**已经是 `[none]` 就别动**，换成任何有名字的调度器都只会更慢。

虚拟机里尤其不要折腾：I/O 穿透到宿主机后还要再排一次队，guest 里改调度器几乎无感知。真要优化，去调宿主机的存储（缓存策略、队列深度、后端是否全闪）。

> 只有一种情况值得写 udev 规则：`ROTA=0` 但调度器显示 `[mq-deadline]` 或 `[bfq]`。规则要按 `rotational=0` 匹配，别按 `sdX` 写死（盘序会漂，见易错点 45）：
>
> ```bash
> cat > /etc/udev/rules.d/60-iosched.rules <<'EOF'
> ACTION=="add|change", KERNEL=="sd[b-z]", ATTR{queue/rotational}=="0", ATTR{queue/scheduler}="none"
> EOF
> udevadm control --reload && udevadm trigger
> ```

**周期性 TRIM：必须先测，虚拟化层经常不透传**

```bash
fstrim -v /data/doris
```

| 返回                                                  | 判定          | 动作                     |
| --------------------------------------------------- | ----------- | ---------------------- |
| `/data/doris: 12.3 GiB (13204897792 bytes) trimmed` | 支持          | 启用定时器（见下）              |
| `the discard operation is not supported`            | **不透传**     | **什么都别配**——配了每周白跑一次并报错 |
| 卡住超过 10 秒                                           | unmap 实现有问题 | Ctrl-C，放弃，别用           |

确认支持后再开：

```bash
systemctl enable --now fstrim.timer
systemctl list-timers fstrim.timer        # 默认每周一次，带随机延迟
```

**为什么虚拟机里 TRIM 反而更要紧**：qcow2/vmdk 是稀疏文件，guest 删了数据但不发 TRIM，宿主机的镜像文件只涨不缩。Doris 的 compaction 会持续删写 segment，几个月后虚拟盘涨到物理峰值就再也不会降下来，thin provision 的容量规划直接失效。前提是 hypervisor 支持透传：

- **KVM/qcow2**：虚拟盘要带 `discard=unmap`，guest 里才有得谈
- **VMware**：硬件版本 11+，且需要 `open-vm-tools` 才能把 UNMAP 传下去
- **Hyper-V**：VHDX 原生支持
- **宿主后端是机械盘**：TRIM 没有意义，直接跳过

> **`discard` 挂载选项一律不用**（物理机虚拟机的结论相同）：XFS 上每次删除文件都可能触发同步 TRIM，Doris 删大 tablet 时会出现秒级卡顿。用 `fstrim.timer` 批量离线回收是唯一合理的取舍。

#### 2.4.9 三台一致性核对

**在每台本机执行**，把三份输出并排比对（不依赖 ssh 免密）：

```bash
findmnt -no TARGET,SOURCE,FSTYPE,OPTIONS /data/doris /data/store
df -hT /data/doris /data/store | tail -2
ls -Zd /data/doris /data/store
```

| 检查项            | 期望                                                             | 不通过的处理                                                 |
| -------------- | -------------------------------------------------------------- | ------------------------------------------------------ |
| 挂载点数           | 每台 2 个（`/data/doris`、`/data/store`）                            | 少了 → 该台 fstab 漏写                                       |
| 文件系统           | 三台都 `xfs`                                                      | 混入 ext4 → 重做该台，三台保持一致                                  |
| 容量             | 各 **≈466G**                                                    | 显示 200G 或几十 G → 挂载没生效，实际写在系统盘                          |
| 挂载选项           | 都含 `noatime`                                                   | 缺失 → fstab 选项写漏                                        |
| SELinux 类型     | `/data/doris` = `var_lib_t`；`/data/store` = `container_file_t` | 标错 → 按 2.4.7 重打                                        |
| TRIM 支持（2.4.8） | 三台结论一致：要么都能 `fstrim`，要么都不支持                                    | 一台支持一台不支持 → 存储后端不同，**按最保守的一台统一**（都不开 timer），否则容量增长不可预测 |

#### 2.4.10 常见故障

| 现象                                                  | 根因                                   | 处置                                                                                                                                 |
| --------------------------------------------------- | ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------- |
| 装到一半 `/` 满，Doris 与 docker 一起异常                      | **`/data` 只是目录不是挂载点**，数据全写进 200G 系统盘 | 停 Doris 与 docker → `umount /data/doris /data/store` → `rm -rf /data/doris /data/store`（清掉误写入系统盘的那份）→ 重做 2.4.5/2.4.6 → 重新挂载后再装第 3 章 |
| 重启进 `emergency mode`                                | fstab 有错行（UUID 错、字段缺、多空格）            | 控制台输 root 密码 → 走下&#x65B9;**「emergency 抢救流程」**，别从头重做 2.4.3~2.4.5                                                                    |
| `mount: wrong fs type, bad option`                  | 分区没格式化，或 fstab 的 fstype 与实盘不符        | `blkid /dev/sdb1` 看有无 `TYPE="xfs"`；没有就回 2.4.4                                                                                      |
| `mkfs.xfs: cannot open ... Device or resource busy` | 分区已被挂载（可能被自动挂上）                      | `findmnt /dev/sdb1` → `umount` 后再格式化                                                                                               |
| 换盘/扩容后开机挂不上                                         | 新盘 UUID 变了，fstab 里的旧 UUID 失效         | `blkid` 取新 UUID 替换 fstab 对应行                                                                                                       |
| `XFS (...): Structure needs cleaning`               | 上次非正常掉电，日志未回放                        | `umount` 后 `xfs_repair /dev/sdb1`（**必须先卸载**，挂载状态下修复会毁数据）                                                                           |
| 三台容量/挂载点不一致                                         | 某台少做或两个挂载点挂反                         | 按 2.4.9 逐台核对；`/data/doris` 与 `/data/store` 挂反不影响功能（容量相同），但台账必须三台统一                                                                 |

**emergency 抢救流程（fstab 写错时按此走，不要从头重做）**

```bash
mount -o remount,rw /          # 不改这个，后面所有写操作都会失败

# 1) 从报错里直接读出错误值：屏幕上的 dev-disk-by\x2duuid-<值>.device，\x2d 就是 "-"
#    例：...uuid-sdb1\x2dx2dae19c0bf... 还原成  sdb1-ae19c0bf...  → 多了设备名前缀

# 2) 判断"文件系统是否还健在"：不看 sdX，看 UUID（UUID 跟前缀无关）
blkid | grep -iE '<fstab 里那段 UUID 的前 8 位>'
#    有输出 → 盘在、文件系统在，只是盘符漂了，去掉前缀即可
#    无输出 → 盘根本没被识别（虚拟盘掉了/没插/控制器丢失），另查硬件层

# 3) 去掉多余前缀（最常见的修复，不重新格式化）
cp /etc/fstab /etc/fstab.bak.$(date +%F)
sed -i -E 's#^UUID=[^[:space:]]*-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})([[:space:]])#UUID=\1\2#' /etc/fstab
grep -E '/data/(doris|store)' /etc/fstab

# 4) 验证通过才重启
mount -a && findmnt /data/doris /data/store
```

> **`blkid -s UUID -o value /dev/sdb1` 返回空，不等于要重新格式化。** 它为空只说明"`sdb1` 这个名字现在不存在"——盘序漂移后数据盘可能已经叫 `sda1`/`sdc1`。**UUID 存在文件系统超级块里，跟着文件系统走，改名不会让它消失**。所以优先从 fstab 里已有的值去修（去前缀），而不是重新取值再手抄一遍——手抄正是错误的来源。
>
> 只有 `blkid | grep <前缀>` 也找不到，才是文件系统真的没了（那种情况才需要回 2.4.3/2.4.4 重做，而且要先确认打的是不是正确的那块盘）。

#### 2.4.11 后续扩容（BE 用量越过 53% 自愈线时）

§0.3.3 的自愈线（256G/节点）触发后的标准动作是**给虚拟机扩盘**，不是删数据：

```bash
dnf install -y cloud-utils-growpart      # growpart 不在默认最小安装里

# 1) 虚拟化层把 sdb 从 500G 扩到 800G 后，让客户机内核识别新容量
echo 1 > /sys/class/block/sdb/device/rescan
lsblk /dev/sdb                            # sdb 应显示 800G，而 sdb1 仍是 500G

# 2) 扩分区（改分区表）
growpart /dev/sdb 1

# 3) 扩 XFS 文件系统（在线扩容，无需卸载）
xfs_growfs /data/doris

df -hT /data/doris
```

> 顺序不能反：**先扩分区、再扩文件系统**。只做 `xfs_growfs` 会报 `data size unchanged`——因为分区边界还是 500G。`growpart` 只能吃到分区后面紧邻的空闲空间，所以当初分区必须建在盘尾（2.4.3 的 `100%` 保证了这点）。

#### 2.4.12 应用节点（dbapi-gw01/02/03）：单盘同盘，不分区不挂载

应用节点每台只有**一块 500G 盘**，OS 与 `/data` 同盘（§0.3.1），**没有第二块盘可挂**——所以本节的 2.4.1~2.4.11 在这三台**全部跳过**，只做"建目录 + 打标签"：

```bash
# 直接建在根文件系统上（/ 就是那块 500G 盘）
mkdir -p /data/pg /data/redis /data/redis-sentinel /data/etcd

# SELinux 标签按运行形态区分（与数据节点不同，这里 PG 也在宿主机上跑）
semanage fcontext -a -t postgresql_var_lib_t "/data/pg(/.*)?" 2>/dev/null || true
restorecon -Rv /data/pg
chown -R postgres:postgres /data/pg          # Patroni 首次启动前必须属主正确
chcon -Rt container_file_t /data/redis /data/redis-sentinel /data/etcd
```

确认空间够用（装上 docker 后 `/` 应还有 ≥350G 可用）：

```bash
df -hT /
```

> **为什么不给应用节点单独挂盘**：PG 数据量按本方案规模是几十 GB 量级、Redis 2G 上限、etcd 2G 配额，500G 单盘完全够用；为这三个组件多要一块盘不划算。代价是 PG 的 WAL `fsync` 会和 docker 镜像、系统日志抢同一块盘，理论上提交延迟有抖动——实测影响远小于数据节点（那边是 BE Compaction 级别的持续随机 IO）。**若后续发现 PG 提交延迟抖动明显（`pg_stat_bgwriter` 的 `buffers_backend_fsync` 上涨），再给三台各加一块盘，按 2.4.3~2.4.6 把 `/data` 挂上去即可，不需要重装**——这也是把目录统一放在 `/data` 下的好处。
>
> **`/data/pg` 的三件事必须按顺序做**：建目录 → `semanage`+`restorecon` 打 `postgresql_var_lib_t` → `chown postgres`。漏了标签 PG 直接 permission denied，漏了属主 Patroni 拒绝以 root 启动（§7-25）。且 `/data/pg` 必须是**空目录**，Patroni 首次启动会自己 `initdb` 并拉基础备份。

### 2.5 关闭透明大页（THP）并固化

```bash
# 立即生效
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag

# 固化：systemd 开机单元（重启后自动关闭，不依赖 rc.local）
cat > /etc/systemd/system/disable-thp.service <<'EOF'
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
EOF
systemctl daemon-reload
systemctl enable --now disable-thp.service

# 验证（应输出 always madvise [never]）
cat /sys/kernel/mm/transparent_hugepage/enabled
```

**解释**：直接 `echo` 只对本次开机有效，重启后 AlmaLinux 10 会恢复 THP——所以必须配 `disable-thp.service`（oneshot 类型，开机早期把两处都写成 never）。**顺序很关键**：`/sys/kernel/mm/transparent_hugepage/*` 属于 sysfs，由 `sysinit.target` 阶段挂载，所以必须写 `After=sysinit.target`；旧稿的 `Before=sysinit.target` 会让脚本在 sysfs 就绪前执行，`echo` 因路径不存在而失败，而 `Type=oneshot` 失败**不阻塞启动**，表面上"已启用"实际没生效、重启后 THP 复活。验证行中方括号落在 `never` 上才算生效，**且必须重启一次再验**才算真固化（`systemctl status disable-thp.service` 应为 active (exited)）。此步为 Doris BE 与 Redis 的延迟稳定性前置条件。

### 2.6 文件描述符与进程数上限

```bash
cat >> /etc/security/limits.conf <<'EOF'
* soft nofile 655350
* hard nofile 655350
* soft nproc 65535
* hard nproc 65535
EOF
```

**解释**：`nofile` 取 655350 是 **Doris 官方要求**（BE 管 ODS 全量数据时 Tablet/Segment 文件句柄极多，65535 在大表多分区场景会耗尽，报 "too many open files"）。**关键提醒**：`limits.conf` 只对 PAM 登录会话生效，**对容器不生效**——本平台 PG/Redis/Kafka/SILO/etcd 全跑在 Docker 里，容器继承的是 dockerd 的限制。须在 §2.8 的 `/etc/docker/daemon.json` 里配 `default-ulimits`（见下节），否则容器内组件仍可能撞句柄上限。

### 2.7 安装 JDK（系统 Java 21 + Doris 专用 Java 17 双版本并存）

> **AlmaLinux 10 打包变更**：系统仓库（AppStream）仅提供 `java-21-openjdk` 与 `java-25-openjdk`，**已移除 Java 17**，因此 `yum install java-17-openjdk` 会报 `No match for argument`。
>
> **组件 Java 版本官方口径（v1.6 已修正）**：Doris **2.1 及更早要求 Java 8，3.0 及以后（含 4.1.5）要求 Java 17（jdk-17.0.10+）**，官方仍不支持 Java 21。
>
> ⚠️ **上一版有一处写错了**：原文称「Flink 1.19+ 兼容 Java 21」，这是不对的。Flink 对 Java 21 的支持**至今标注为实验性**（2.0.0 起引入，1.19/1.20 仍属 beta）。本版把 **Flink 也切到 Temurin 17**，与 Doris 共用同一份 JDK；系统 Java 21 只留给 DBAPI（Spring Boot 3）。
>
> 因此形成**双 JDK 并存**格局：全机装系统 Java 21（DBAPI / Kafka 客户端 / ZK / DS 用）；**数据节点另装 Temurin JDK 17**（**Doris + Flink 共用**，组件级 `JAVA_HOME` 隔离，见 2.6.2 与 §3.1 步骤 3、9）。

#### 2.7.1 安装系统 Java 21（六台全装）

```bash
yum install -y java-21-openjdk java-21-openjdk-devel
java -version            # 21.x

# 解析并设置 JAVA_HOME（各组件显式引用，勿依赖系统默认 java）
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
echo "JAVA_HOME=$JAVA_HOME"

# 持久化（所有节点执行）
cat > /etc/profile.d/java.sh <<'EOF'
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
export PATH=$JAVA_HOME/bin:$PATH
EOF
```

**解释**：`java-21-openjdk-devel` 包含 `javac` 等开发工具，Flink/DBAPI 的启动与编译需要 JDK 而非仅 JRE。`JAVA_HOME` 用 `readlink -f` 顺链解析出真实安装路径（AlmaLinux 10 会建 `/usr/lib/jvm/java-21-openjdk` 短链接，但以解析值为准更稳妥），并写入 `/etc/profile.d/java.sh` 持久化；DBAPI 启动、Flink `conf/flink-conf.yaml`、DS 都显式引用它。**Doris 例外**——它用 2.6.2 的专用 JDK 17，不引用系统 21。**切勿**在已装多版本 JDK 时随意用 `alternatives` 切换默认 `java`，否则可能把系统默认版本改掉、导致组件用错 JDK——始终以各组件自己的 `JAVA_HOME` 为准。

#### 2.7.2 安装组件专用 JDK 17（Doris + Flink 共用；仅数据节点 doris-dw01/02/03）

**当前锁定版本：Temurin `17.0.21+9`（2026-10-22 发布）**。下载地址与校验值已按官方 release 实测核对（Linux x64 JDK 包 **193,594,705 字节**）。

```bash
cd /opt
# Temurin 17 以 tarball 安装：AlmaLinux 10 仓库无 Java 17，tarball 不依赖发行版仓库
curl -fLO "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.21%2B9/OpenJDK17U-jdk_x64_linux_hotspot_17.0.21_9.tar.gz"

# 完整性校验（值取自 Adoptium 官方 .sha256.txt，必须一致）
echo "8b9b611d917e3dfd8419d3566b5949226f69b052fed7e109c1368add1dfe8711  OpenJDK17U-jdk_x64_linux_hotspot_17.0.21_9.tar.gz" | sha256sum -c -

tar -xf OpenJDK17U-jdk_x64_linux_hotspot_17.0.21_9.tar.gz
mv /opt/jdk-17.0.21* /usr/lib/jvm/temurin-17-jdk
/usr/lib/jvm/temurin-17-jdk/bin/java -version   # 期望 openjdk version "17.0.21" 17.0.21+9
```

**已经装过旧版（如 17.0.13）怎么升**：三台数据节点逐台做，别一次全停。

```bash
# 1) 停本机的 Doris FE/BE 与 Flink（其他两台保持在线，集群不中断）
# 2) 旧目录改名留退路，不要直接删
mv /usr/lib/jvm/temurin-17-jdk /usr/lib/jvm/temurin-17-jdk.old
# 3) 按上面步骤装 17.0.21 到同一路径 /usr/lib/jvm/temurin-17-jdk
# 4) 组件配置无需改动（JAVA_HOME 和 env.java.home 指向的都是这个固定路径）
# 5) 启动组件，确认 java -version 与进程正常后再处理下一台
# 确认无误后删除旧目录：rm -rf /usr/lib/jvm/temurin-17-jdk.old
```

> **为什么路径固定成 `temurin-17-jdk` 而不是带版本号**：Doris 的 `JAVA_HOME`、Flink 的 `env.java.home` 都写这个固定路径，升级只换目录内容、不动配置。代价是"当前装的是哪个补丁版本"看不出来——用 `java -version` 或 `/usr/lib/jvm/temurin-17-jdk/release` 文件核对。

**解释**：Doris 4.1.5 官方要求 Java 17（jdk-17.0.10+），**Flink 也用它**（Flink 对 Java 21 只是实验性支持，不适合生产），而 AlmaLinux 10 仓库不提供 Java 17，故用 Adoptium Temurin 17 的 tarball 直接解压到 `/usr/lib/jvm/temurin-17-jdk`（ARM 机型把文件名里的 `x64` 换成 `aarch64`）。Doris 的 `env.sh`/`start_fe.sh`/`start_be.sh` 通过**组件级 `JAVA_HOME`** 使用它（§3.1 步骤 3 写入 `fe.conf`/`be.conf`）；Flink 通过 `flink-conf.yaml` 的 `env.java.home` 使用它（§3.1 步骤 9）。应用节点（dbapi-gw01/02/03）既无 Doris 也无 Flink，**跳过本步**。再次强调：**不要**把 Temurin 17 加入 `/etc/profile.d/` 或设为系统默认 `java`——系统默认必须保持 21（DBAPI 用）。

### 2.8 安装 Docker

```bash
yum install -y dnf5-plugins-core
yum config-manager --add-repo https://download.docker.com/linux/rhel/docker-ce.repo
yum install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# daemon 全局配置：容器句柄上限（§2.6 的 limits.conf 对容器不生效）+ 日志轮转
mkdir -p /etc/docker
cat > /etc/docker/daemon.json <<'EOF'
{
  "bip": "172.31.0.1/24",
  "default-address-pools": [
    { "base": "172.31.1.0/24", "size": 24 }
  ],
  "default-ulimits": {
    "nofile": { "Name": "nofile", "Soft": 655350, "Hard": 655350 }
  },
  "log-driver": "json-file",
  "log-opts": { "max-size": "100m", "max-file": "5" }
}
EOF
systemctl enable --now docker

# 验证：docker0 应是 172.31.0.1/24，且没有 172.17.x.x
ip -4 addr show docker0 | grep inet
docker network ls
```

> **备选**：若 docker 官方 repo 暂未收录 EL10，改用 `yum install -y podman podman-compose`，后续 `docker` 命令替换为 `podman`。

> **`bip` 与 `default-address-pools` 是必配项，不是可选项**。docker 默认把 `docker0` 建成 `172.17.0.1/16`，自定义网络也从 `172.17.0.0/16` 里切。若公司内网用到了 `172.16.0.0/12` 大段（172.16~172.31 都算），宿主机访问内网某台 `172.x` 的机器时，路由可能被 docker0 抢走——表现为"ping 内网 IP 不通 / 通一会儿断一会儿 / 容器能出去但回不来"，极难定位。本方案统一挪到 `172.31.x`。
>
> | 配置项                     | 作用                              | 本方案取值                            | 说明                                      |
> | ----------------------- | ------------------------------- | -------------------------------- | --------------------------------------- |
> | `bip`                   | `docker0` 的网关地址与网段              | `172.31.0.1/24`                  | 桥接网段 = `172.31.0.0/24`，容器默认走这里          |
> | `default-address-pools` | `docker network create` 分配网段的来源 | `base 172.31.1.0/24` + `size 24` | 当前配置可切 **1 个 /24**（172.31.1.0/24），见下方约束 |
>
> **`base` 的掩码必须大于 `size`，否则只能建一个网络**。当前取值 `base: 172.31.1.0/24` + `size: 24` 就是这种情况：母段与子网同大小，**只能分配 1 个自定义网络**（172.31.1.0/24），建第二个会报 `all predefined address pools have been fully subnetted`。
>
> **本方案可以接受这个限制**：Kafka / etcd / ZK / Redis 哨兵 / SILO / APISIX **全部 `--net host`**，走的是 docker0（`bip` 那段），根本不消耗地址池；只有将来创建自定义 bridge 网络（如给某个临时工具隔离网络）时才会用到第 1 个名额。
>
> **真要建第 2 个网络时怎么扩**：把 `base` 改成 `/20` 留出余量，然后重启 docker（**已存在的网络不会自动改网段**，要删掉重建）：
>
> ```bash
> # 1) 改 daemon.json：{"base": "172.31.16.0/20", "size": 24}
> systemctl restart docker
> # 2) 同步 §6.3/§6.4 的防火墙兜底：把 172.31.1.0/24 换成 172.31.16.0/20
> firewall-cmd --permanent --zone=datanode --remove-source=172.31.1.0/24
> firewall-cmd --permanent --zone=datanode --add-source=172.31.16.0/20
> # appnode 同理；然后 firewall-cmd --reload
> ```
>
> **扩池后务必同步防火墙**，否则新建的桥接容器源地址落在未放行的段里，一样被拦。
>
> **`bip` 与地址池必须错开**：`172.31.0.0/24`（docker0）与 `172.31.1.0/24`（池）不重叠——两个 /24 首尾相邻、互不覆盖。若把池写成 `base 172.31.0.0/16`，就套住了 docker0 的段，虽然 docker 会跳过已用网段，但显式错开更省事。
>
> **改完要重启 docker，且已有网络不会变**：`systemctl restart docker` 会重启所有容器（有 `restart:` 策略的会自动拉起，没有的需手工起）；已存在的 bridge 网络保留旧网段，只有**新建**的才用新池。所以这项配置要在装第一个容器**之前**做完，中途改会留下"一半旧段一半新段"的混乱局面。
>
> **防火墙必须同步**（§6.3 / §6.4）：桥接容器访问宿主端口时源地址是 `172.31.0.1`，`datanode`/`appnode` 两个 zone 都要放 `172.31.0.0/24`（docker0）与 `172.31.1.0/24`（地址池），否则容器被拦在门外（症状见 §7-28）。

**解释**：`default-ulimits` 把 §2.6 的 655350 句柄上限下发给**每一个容器**（PG/Redis/Kafka/SILO/etcd 都吃这个配置）。`log-opts` 是**必配项**：docker 默认 `json-file` 驱动**不限大小**，Kafka/PG 这类高频写日志的容器几周就能把系统盘写满——`max-size 100m × 5` 封顶每容器 500MB 日志。注意 `daemon.json` 只影响**新建**容器，若部署中途才改，需 `docker compose up -d --force-recreate` 重建已有容器。

**六台系统准备到此完成，下面进入逐节点组件部署。**

---

## 3. 逐节点部署（两层分册，每台独立成节、互不交叉）

> 本章把"在某一台上执行的全部步骤"收进同一节。顺序：**先数据层 3.1→3.2→3.3（doris-dw01 先成为引导节点），再应用层 3.4→3.5（dbapi-gw01 成为主用/引导）**。DS 集群不在本章范围。

---


### 3.1 节点 doris-dw01（192.168.110.201 ｜ 数据/入湖层 · 引导节点）

> 本节点角色：Doris FE(Leader) + BE + Kafka broker1 + SILO(节点1) + Flink HA(JM+TM) + ZooKeeper(1)。

**步骤 1：确认 §2 系统准备已完成**

```bash
hostname            # 应返回 doris-dw01
timedatectl status  # System clock synchronized: yes
docker ps           # 守护进程正常
java -version       # 21.x
```

**步骤 2：部署 Doris 二进制（4.1.5 稳定版）**

```bash
# 0) 确认 CPU 支持 AVX2（Doris 4.x BE 依赖 AVX2 向量化；无输出则须改下 no-AVX2 安装包）
grep -q avx2 /proc/cpuinfo && echo "AVX2 OK" || echo "NO AVX2：请下载 no-AVX2 安装包"

# 1) 确认两块数据盘已按 §2.4 挂好并已重启复验（BE 独占 /data/doris，其余在 /data/store）
df -h /data/doris /data/store

# 2) 下载并解压到临时目录（不要直接解到 /data/doris，那里已挂载 sdb）
#    官方下载页：https://doris.apache.org/zh-CN/download/ （版本/架构/校验和以此页为准）
#    包名架构段是 x64，不是 x86_64；no-AVX2 机器用 -bin-x64-noavx2.tar.gz，ARM 用 -bin-arm64.tar.gz
cd /opt
curl -fLO https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-4.1.5-bin-x64.tar.gz
# 校验（防止下到被篡改/截断的包）：下面这串是官方下载页给出的 avx2 包 SHA-512；no-AVX2/ARM 包各有自己的校验和，请以官网为准
echo "ec7660a4cef05ec9134914941bb6c2f3df0d5c19319b31469fdc1b0edb7da697f225669a3f31f749178b15df5ef2bfca0cd985726c1c029bbab133eb5cb371b6  apache-doris-4.1.5-bin-x64.tar.gz" | sha512sum -c -
tar -xf apache-doris-4.1.5-bin-x64.tar.gz

# 3) 把解压内容并入 /data/doris（Doris 主目录），避免 mv 到挂载点里再套一层
mkdir -p /data/doris
cp -a apache-doris-4.1.5-bin-x64/. /data/doris/
mkdir -p /data/doris/fe/meta /data/doris/be/storage
# SELinux：cp -a 会保留归档里的旧上下文，并入后必须重打一次（否则 BE 启动读不到自己的库目录）
chcon -Rt var_lib_t /data/doris
rm -rf /opt/apache-doris-4.1.5-bin-x64 /opt/apache-doris-4.1.5-bin-x64.tar.gz

# 4) 结构自检：应看到 bin/ conf/ fe/ be/
ls /data/doris
```

> **路径要点**：本版 `/data/doris` 是**独立挂载点**（sdb 500G），不是普通目录。所以不能再写 `cd /data && mv apache-doris-xxx doris`——那样会变成 `/data/doris/apache-doris-xxx` 嵌套一层，后面 `fe.conf`/`be.conf` 的路径全对不上。用 `cp -a .../.` 把内容并入挂载点。

**解释**：拉取 Doris **4.1.5**（2026-10-16 发布，4.1 线第 5 个补丁；ARM 选 `arm64` 包，文件名 `-bin-arm64.tar.gz`）。`grep -q avx2` 有输出表示 CPU 支持 AVX2，Doris 4.x BE 依赖它做向量化加速，不支持的 CPU 必须下 no-AVX2 专用包（`-bin-x64-noavx2.tar.gz`）否则 BE 无法启动。`/data/doris/fe/meta` 存 FE 元数据，`/data/doris/be/storage` 存 BE 数据，须提前建好。**版本选型依据见 `DBAPI_Doris组件版本基线.md` §4**：4.0.8 是 4.0 线的收官维护版，4.0 线已停止演进；新装没有存量包袱，直接落 4.1.5，避免开局就欠一次升级。

**步骤 3：配置 FE 与 BE**

```bash
cat >> /data/doris/fe/conf/fe.conf <<'EOF'
# 多网卡/容器网络下必须显式声明，避免 FE 绑到错误网卡（v1.8 新增）
priority_networks = 192.168.110.0/24
meta_dir = /data/doris/fe/meta
http_port = 8030
rpc_port = 9020
query_port = 9030
edit_log_port = 9010
JAVA_HOME = /usr/lib/jvm/temurin-17-jdk
# JVM 4G：64G 节点上 FE 给 8G 是浪费；JDK17 下 Doris 优先读 JAVA_OPTS_FOR_JDK_17，两个都写
JAVA_OPTS = "-Xmx4g -Xms4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Dfile.encoding=UTF-8"
JAVA_OPTS_FOR_JDK_17 = "-Xmx4g -Xms4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Dfile.encoding=UTF-8"
qe_max_connection = 1024
max_running_txn_num_per_db = 1000
stream_load_default_timeout_second = 600
dynamic_partition_enable = true
EOF
cat >> /data/doris/be/conf/be.conf <<'EOF'
# ---- 网络：与 FE 一致，显式声明网段（v1.8 新增）----
priority_networks = 192.168.110.0/24
# ---- 存储：BE 独占 /data/doris（/dev/sdb 500G），单路径即可 ----
storage_root_path = /data/doris/be/storage
be_port = 9060
webserver_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060
JAVA_HOME = /usr/lib/jvm/temurin-17-jdk
# ---- 内存：真正的旋钮是 mem_limit，不是 JAVA_OPTS ----
mem_limit = 40%
soft_mem_limit_frac = 0.8
storage_page_cache_limit = 20%
disable_storage_page_cache = false
# ---- 4.1.5 必改项：enable_file_cache 默认值 false→true，BE 会占本地盘做 file cache ----
# 480G 要留给数据本身与 Compaction 余量，必须显式关掉（否则磁盘账本失效）
enable_file_cache = false
# ---- 导入 / Compaction（64G 共置机必须限速）----
load_process_max_memory_limit_percent = 40
compaction_max_memory_limit_percent = 20
write_buffer_size = 100M
max_routine_load_task_num_per_be = 3
routine_load_thread_pool_size = 6
max_consumer_num_per_group = 3
streaming_load_max_mb = 10240
max_tablet_version_num = 500
EOF
```

**解释**：`fe.conf` 的 `meta_dir` 指定元数据目录；`JAVA_OPTS` 给 FE 4G 堆（64G 共置机上 8G 是浪费），JDK17 下 Doris 优先读 `JAVA_OPTS_FOR_JDK_17`，两个都写以免版本差异导致失效。

**`be.conf` 是本版最重要的一处修正**：旧稿写 `JAVA_OPTS="-Xmx32g"` 有两层错误——① **BE 是 C++ 进程（`doris_be`），没有 JVM 堆**，`be.conf` 里的 `JAVA_OPTS` 只影响 Java UDF 等边缘功能，对 BE 主进程内存**毫无约束**；② 即便有效，64G 机器给 32G 也会把全栈顶到 48G，导入峰值一冲就 OOM Kill（通常先被杀的是 SILO/Kafka，表现为"checkpoint 写失败"这种看似无关的故障）。真正的旋钮是 **`mem_limit`（百分比，默认 90%）**，共置场景必须压到 **40%（25.6G）**；`soft_mem_limit_frac=0.8` 让软限 20.5G 提前触发回收，避免撞硬限。`max_routine_load_task_num_per_be=3`（原默认 5）是 16C 共置机的收敛值。

`JAVA_HOME = /usr/lib/jvm/temurin-17-jdk` 是 Doris 专用 Java 17（§2.7.2 安装）——Doris 4.1.5 要求 Java 17，不支持系统默认的 21；该配置只在 Doris 进程内生效，与系统 Java 21 互不干扰（Flink 也用这份 17，见 §3.1 步骤 9）。

**验证**：

```sql
SHOW BACKEND CONFIG LIKE 'mem_limit';            -- 期望 40%
SHOW BACKEND CONFIG LIKE 'soft_mem_limit_frac';  -- 期望 0.8
SHOW BACKEND CONFIG LIKE 'enable_file_cache';    -- 期望 false（4.1.5 默认是 true，必须显式关）
SHOW PROC '/backends';                           -- 看 MemUsed / MaxMemory
```

```bash
curl -s http://192.168.110.201:8040/mem_tracker | head -30   # 看 Query/Load/Compaction 三项
ps -o rss= -p $(pgrep doris_be) | awk '{printf "BE RSS: %.1fG\n", $1/1024/1024}'   # 稳定 20~25G，不应超 27G
```

**步骤 4：启动 FE（作为初始 Leader，不加 --helper）**

```bash
/data/doris/fe/bin/start_fe.sh --daemon
```

**解释**：首台不加 `--helper`，自己成为集群第一个 FE 即 Leader。约 10–20 秒后用 `mysql -h 192.168.110.201 -P 9030 -u root -e "SHOW PROC '/frontends';"` 验证 `Alive=true`、`IsMaster=true`。

**步骤 5：启动 BE**

```bash
/data/doris/be/bin/start_be.sh --daemon
# 可选 NUMA 绑核（多路服务器建议）：先 yum install -y numactl，再用下行代替上行
# numactl --interleave=all /data/doris/be/bin/start_be.sh --daemon
```

**解释**：BE 先起进程，但**未加入集群**——须由第 4 章 `ALTER SYSTEM ADD BACKEND` 注册。注册前日志有连接报错属正常。NUMA 说明：`vm.zone_reclaim_mode=0`（§2.3）已消除跨节点激进回收；双路及以上 CPU 的机器上，进一步用 `numactl --interleave=all` 让 BE 内存交叉分布在两个 NUMA 节点，避免单节点内存被打满后触发远端分配慢路径；单路服务器无需此步。

**步骤 6：部署 Kafka（broker1）**

```bash
mkdir -p /data/store/kafka/data
# ⚠️ 必须改属主：apache/kafka 官方镜像以 uid:gid=1000(appuser) 运行，而本目录由 root 创建（属主 0:0）。
# 不改属主 → 容器里 1000 写不进数据目录 → 启动即报
#   java.nio.file.AccessDeniedException: /var/lib/kafka/data/bootstrap.checkpoint.tmp
# 且因 --restart unless-stopped 陷入无限重启。三台数据节点都要做。
chown -R 1000:1000 /data/store/kafka/data

# ① 生成集群 ID：只需一次，三台 broker 必须填同一个值
docker run --rm apache/kafka:4.3.2 /opt/kafka/bin/kafka-storage.sh random-uuid
# 把输出的字符串记下来，dw01/dw02/dw03 三台都用它

# ② 启动 broker1（--net host：broker 间副本同步与 controller quorum 直连，免 docker NAT）
docker run -d --name kafka --restart unless-stopped --net host \
  -v /data/store/kafka/data:/var/lib/kafka/data:z \
  -e CLUSTER_ID=<上一步输出的集群ID> \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@192.168.110.201:9093,2@192.168.110.202:9093,3@192.168.110.203:9093 \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://192.168.110.201:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_LOG_DIRS=/var/lib/kafka/data \
  -e KAFKA_HEAP_OPTS="-Xmx2g -Xms2g" \
  -e KAFKA_NUM_PARTITIONS=3 \
  -e KAFKA_DEFAULT_REPLICATION_FACTOR=3 \
  -e KAFKA_MIN_INSYNC_REPLICAS=2 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 \
  -e KAFKA_TRANSACTION_MAX_TIMEOUT_MS=1800000 \
  -e KAFKA_LOG_RETENTION_HOURS=48 \
  -e KAFKA_LOG_RETENTION_BYTES=5368709120 \
  -e KAFKA_LOG_SEGMENT_BYTES=536870912 \
  -e KAFKA_LOG_CLEANUP_POLICY=delete \
  -e KAFKA_COMPRESSION_TYPE=lz4 \
  -e KAFKA_NUM_IO_THREADS=8 \
  -e KAFKA_NUM_NETWORK_THREADS=4 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
  apache/kafka:4.3.2
```

**解释**：Kafka 是入湖缓冲层，Flink CDC 把业务库变更投到 Kafka，Doris Routine Load 再消费进仓。3 台数据节点各跑 1 个 broker，组成 3 broker 集群（`KAFKA_CONTROLLER_QUORUM_VOTERS` 列出三台控制器）。`ADVERTISED_LISTENERS` 必须写本机 IP，否则其他 broker/客户端连不上。

> **v1.6 换镜像的三个理由**：① Bitnami 已把全部版本化 tag 迁到 `bitnamilegacy` 并从公共目录移除，`bitnami/kafka:3.7` 拉不到；② Kafka 3.x 早已 EOL（只维护最近三个版本，随 4.4.0 发布 **4.1 也掉出支持集**）；③ 官方 `apache/kafka` 镜像**自带 JRE**，不再依赖宿主机 Java 版本。选 **4.3.2**（2026-10-20，最新补丁版）而不是 4.4.0（2026-10-15 发布、零补丁版）。

> ⚠️ **最容易翻车的一点：环境变量前缀变了**。`apache/kafka` 用 **`KAFKA_*`**，不是 Bitnami 的 `KAFKA_CFG_*`。若沿用旧变量名，**会被静默忽略**——容器照常启动，但 KRaft 的 controller quorum、副本因子、保留策略全部回到默认值，表现为"集群能连、但 topic 建不出 3 副本"。上一版手册里"换成 apache/kafka 后 `KAFKA_CFG_*` 同样支持"这句话是**错的**，已删除。

**三个必填项**（缺了 KRaft 起不来或副本同步走不通）：

- `CLUSTER_ID`：KRaft 的集群标识，**三台必须完全一致**，不一致则各自为政、选不出 controller。用 ① 的命令生成一次即可。
- `KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER`：`PROCESS_ROLES=broker,controller` 时必须显式声明哪个 listener 用于控制器通信，缺了会报 `controller.listener.names must be set`。
- `KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT` + `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`：显式指定 broker 间通信走 PLAINTEXT 并映射两个 listener 的协议，否则可能选错 listener 导致副本同步失败。

**为什么用 `--net host`**：broker 间副本同步和 controller quorum（9093）都是高频内网流量，走 `--net host` 直接监听宿主网卡，省掉 docker NAT 与 docker-proxy，也避免"同一网段但容器内 IP 通告错"那类问题。这与 Kafka 的设计假设（advertised 地址 = 客户端可达地址）一致。

**事务参数为什么必须显式写**（v1.8 新增）：Flink 的 `upsert-kafka` sink 在 `EXACTLY_ONCE` checkpoint 模式下会自动启用**事务生产者**，事务状态写内部 topic `__transaction_state`。该 topic 的默认副本因子虽是 3，但 `min.insync.replicas` 不会自动继承 `KAFKA_MIN_INSYNC_REPLICAS`——不显式写 `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2`，事务协调器可能在 ISR 收缩时卡住。另外 broker 默认 `transaction.max.timeout.ms=900000`，而客户端写的 `properties.transaction.timeout.ms` 也是 900000，**等于上限没有余量**，一旦 broker 侧稍有延迟就会 `InvalidTxnTimeoutException` → 把上限提到 1800000 留出缓冲。

**磁盘保护**：日志目录挂到第二块盘 `/data/store/kafka/data`（容器内统一用 `/var/lib/kafka/data`），并用 `KAFKA_LOG_RETENTION_HOURS=48` + `KAFKA_LOG_RETENTION_BYTES=5G/分区` 做**双闸**（原默认 168h 在 120G 配额下必然写满）；`KAFKA_COMPRESSION_TYPE=lz4` 对 CDC JSON 有 3~5× 压缩比，直接换算成磁盘和网络收益；`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` 防止拼错 topic 名时静默建出 1 分区 1 副本的坑 topic。`KAFKA_HEAP_OPTS=2g`：单 broker 峰值 <5MB/s，heap 大了只会拖长 GC。

**验证**：`docker logs kafka | grep -i "Kafka Server started"`；`docker exec kafka /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server 192.168.110.201:9092 describe --status` 应看到 3 个 voter 且 elect 出一个 leader。

> ⚠️ **"只起一台 → 一直转圈"是正常过渡态，不是卡死**。KRaft 的 controller quorum 需要 **2/3 多数票**才能选出 leader 并发布元数据。若只起了 dw01（node 1），日志会持续刷 `initializeNewPublishers: the loader is still catching up because we still don't know the high water mark yet`，并带 `Connection to node 2/3 (/192.168.110.202:9093、/203:9093) could not be established`、`voterStates={1=GRANTED, 2=UNRECORDED, 3=UNRECORDED}`——这是因为另外两台还没起来，**不是配置错也不是上一个 AccessDenied 重启坑**（format 已经过了）。先把 dw02/dw03 的 Kafka 也按本步骤拉起，**两台都要先 `chown -R 1000:1000`（§7-46）**，且 `CLUSTER_ID` 用 dw01 生成的同一个值；三台互连后几秒内 quorum 自愈、leader 选出、broker 完成注册。
>
> 若三台都起来了仍连不上 202/203:9093，再按此顺序排：① `nc -vz 192.168.110.202 9093` / `nc -vz 192.168.110.203 9093` 区分"Connection refused=对端 Kafka 没监听"还是"timed out=防火墙挡"；② 每台 `firewall-cmd --zone=datanode --list-ports | grep -E '9092|9093'`（§6.3 端口循环已含，但需三台都执行且 `--reload`）；③ 三台 `docker inspect` 出的 `CLUSTER_ID` 是否一致。

**步骤 7：部署 SILO（对象存储，分布式，节点1）**

```bash
mkdir -p /data/store/silo/d1 /data/store/silo/d2

docker run -d --name silo --restart unless-stopped --net host \
  -e MINIO_ROOT_USER=<强用户名> -e MINIO_ROOT_PASSWORD=<≥16位强口令> \
  -e GOMEMLIMIT=1GiB \
  -v /data/store/silo/d1:/d1 -v /data/store/silo/d2:/d2 \
  docker.io/pgsty/silo:RELEASE.2026-10-18T00-00-00Z server \
    http://192.168.110.201:9000/d1 http://192.168.110.201:9000/d2 \
    http://192.168.110.202:9000/d1 http://192.168.110.202:9000/d2 \
    http://192.168.110.203:9000/d1 http://192.168.110.203:9000/d2 \
    --console-address ":9001"

# 就绪检查：镜像内置探针（返回 0 即就绪），比 curl /minio/health/live 更准
docker exec silo silo healthcheck ready && echo SILO-READY
```

> ⚠️ **必须 `--net host`，不能用 `-p 9000:9000`（v1.7 修正）**：启动参数里声明的 drive 地址是 `192.168.110.201:9000` 这类**宿主 IP**，容器要自己去连这些地址做 EC 池初始化。若走 bridge + 端口映射，容器访问宿主 IP 的源地址是 `172.17.0.1`，不在 `ipset:data-nodes` 里，会被 `datanode` zone 拒绝 → **SILO 起得来但 EC 池初始化失败/6 drive 只认到本地**。改 host 网络后直接监听宿主 9000/9001，EC 三节点互连也少一层 NAT。同理，凡是"容器要访问本机其它服务端口"的组件（APISIX 见下方）都必须 host 网络。

**解释**：对象存储用分布式模式把三台数据节点的目录组成一个存储池，DBAPI 异步导出大结果集时落地这里；**同时它也是 Flink 的 checkpoint / savepoint / HA 元数据后端**（§3.1 步骤 9）。

> **v1.6 换 SILO 的原因（阻断项，不是优化）**：MinIO 社区版 2025-10 起停发二进制，Docker Hub 仓库 2026-04 归档、**2026-09-11 镜像被删除**——原命令 `minio/minio` 必然拉取失败，而 Flink 的 checkpoint 恰好存在这里。
>
> **SILO 是 Pigsty 社区维护的 MinIO fork**，守住三条契约：S3 API 与存储格式（`.minio.sys`）不变、`MINIO_*` 环境变量与 `/minio/*` 路由保留、`minio_*` 指标名不变。所以本步除镜像名与容器名外，参数几乎不用改（目录统一为 `/data/store/silo`，与存储格式无关）。
>
> 三个使用注意：① **必须钉死 release tag 或 digest**，不要用 latest；② 运维客户端统一用镜像内置的 **`mcli`**（`docker exec silo mcli ...`），**不要混用旧 `mc`**——SILO 与未改动的上游客户端只保证 best-effort 兼容；③ **禁止混部滚动升级**，要么整集群同版本全停全起，要么不动。每次升级都当"下游升级"对待：先读 release notes（20261018 这版移除了 strict multipart listing、改了对象锁与 IAM 授权），留好回滚路径。

**为什么每台要划 2 个目录**：SILO 沿用 MinIO 的纠删码实现，**启用 EC 至少需要 4 个 drive**。旧稿"每节点 1 目录 × 3 节点 = 3 drive"**不具备 EC 冗余**——而 Flink 的 checkpoint 恰好存在这里，任一节点磁盘故障就可能导致 checkpoint 整体不可读，HA 恢复链断裂。改为**每节点 2 个目录**（`/data/store/silo/d1`、`d2`，各 150G），3×2=**6 drive**，满足 EC 最低要求。但**可用容量不是裸盘的 2/3**——按上游默认 parity 表（1→EC:0、2~~3→EC:1、4~~5→EC:2、**6~7→EC:3**、8~16→EC:4），6 drive 默认 **EC:3**，可用 = 裸容量×(6−3)/6 = **50%，即 900G 裸 → ≈450G**（v1.8 修正，旧稿按 2/3 记 600G 是错的，容量规划会虚高 33%）。实测用 `docker exec silo mcli admin info local` 看 EC 值确认。若确实要 2/3 利用率，可显式设 `MINIO_STORAGE_CLASS_STANDARD=EC:2`（容错从 3 盘降到 2 盘，生产不建议）；启动命令三台**完全一致**。

> ⚠️ **残留风险必须知道**：同一物理盘划两个目录，只能防"对象被 EC 切分后单盘损坏导致对象不可读"，**不能防整块 500G 盘损坏**。所以必须同时做三件事：① `execution.checkpointing.num-retained=3`（§3.1 步骤 9）；② 桶配额（§4 步骤 6，防止 SILO 吃满与 Kafka/Flink 共用的第二块盘）；③ 关键 savepoint 定期归档。生产最终态建议给第二块盘扩容时按**独立物理盘**规划。
>
> `MINIO_ROOT_USER/PASSWORD` 必须改强口令并与 Flink 的 `s3.access.key`/`s3.secret.key`、DBAPI 的 S3 配置保持一致（旧稿用默认 minioadmin/minioadmin，等于裸奔）。`GOMEMLIMIT=1GiB` 限制 Go 运行时内存，避免在 64G 共置机上失控。

**步骤 7b：SILO 入口 VIP `192.168.110.207`（数据层 keepalived，节点1；v1.12 新增）**

```bash
# 前置：确认 .207 未被其它设备占用（三台各跑一次，必须"无回包"）
arping -c 3 -I eth0 192.168.110.207 || echo "有回包！换地址或查 ARP 冲突"

dnf install -y keepalived
cat > /etc/keepalived/keepalived.conf <<'EOF'
vrrp_script chk_silo {
    # SILO 镜像自带 healthcheck，比裸 curl 更准（EC 池未就绪时 curl 也可能返回 200）
    script "docker exec silo silo healthcheck ready"
    interval 2
    fall 2                        # 连续 2 次失败才降级，防抖动误切
    rise 1
    weight -80                    # 降级后优先级必须低于全场最低(100)，推导同 §3.4
}
vrrp_instance VI_1 {
    state BACKUP
    interface eth0                # 按 ip a 实际网卡改
    virtual_router_id 53          # ★ 与应用层 VI_1(51)/VI_2(52) 区分，不能撞
    priority 150                  # 三台：dw01=150 / dw02=120 / dw03=100
    nopreempt
    advert_int 1
    authentication { auth_type PASS; auth_pass <VRRP认证口令> }
    virtual_ipaddress { 192.168.110.207/24 }
    track_script { chk_silo }
}
EOF

systemctl enable --now keepalived
ip a | grep 192.168.110.207       # 应只在 dw01 上出现
curl -sf http://192.168.110.207:9000/minio/health/live && echo VIP-OK
```

**解释**：这一步补的是"**数据层有 EC、入口却绑死单节点**"的残余单点（v1.12 前的 `s3.endpoint` 直接写 `.201:9000`）。要点：

- **SILO 本身就是三节点集群**，所以 VIP 后面**不需要 HAProxy**——VIP 只要保证"总有一个存活节点的 S3 端口可达"，请求落到哪台，那台的 SILO 会按 EC 布局到全集群取数据。
- **健康检查必须查 SILO 而不是查网卡**：`docker exec silo silo healthcheck ready` 覆盖"容器在但 EC 池未就绪"的情况；只用 `ping` 或 `ss -lntp` 会出现"VIP 漂到一台 SILO 起不来的节点上"。
- **`virtual_router_id` 必须是 53**：应用层已用 51/52，同网段同 VRID 会互相串通告（表现为两个 VIP 抢同一块 ARP）。
- **三台数据节点都要装 keepalived**，priority 150/120/100 + `weight -80`，`auth_pass` 三台必须一致，且**不要用示例口令**（见 §0.5）。
- 与 `--net host` 的配合：SILO 监听 `0.0.0.0:9000`，VIP 落在哪台，该台就自然接住请求，**无需改 SILO 启动参数**。
- 防火墙需放行 VRRP（协议号 112），见 §6.3；**漏放行时 VIP 起得来但三台互相收不到通告，会形成多主**。

**节点2/节点3 的差异**：`/etc/keepalived/keepalived.conf` 内容完全相同，**只改 `priority`**（dw02=120、dw03=100）。`auth_pass`、`virtual_router_id 53`、VIP 地址三台必须一字不差。

**步骤 8：部署 ZooKeeper（节点1，server.id=1）**

```bash
mkdir -p /data/store/zookeeper/data /data/store/zookeeper/log

docker run -d --name zk --restart unless-stopped --net host \
  -v /data/store/zookeeper/data:/data \
  -v /data/store/zookeeper/log:/datalog \
  -e ZOO_MY_ID=1 \
  -e ZOO_SERVERS="server.1=192.168.110.201:2888:3888;2181 server.2=192.168.110.202:2888:3888;2181 server.3=192.168.110.203:2888:3888;2181" \
  -e ZOO_TICK_TIME=2000 \
  -e ZOO_INIT_LIMIT=10 \
  -e ZOO_SYNC_LIMIT=5 \
  -e ZOO_MAX_CLIENT_CNXNS=200 \
  -e ZOO_AUTOPURGE_SNAPRETAINCOUNT=3 \
  -e ZOO_AUTOPURGE_PURGEINTERVAL=24 \
  -e ZOO_4LW_COMMANDS_WHITELIST="srvr,ruok" \
  -e ZOO_ADMINSERVER_ENABLED=false \
  -e JVMFLAGS="-Xms1g -Xmx1g" \
  zookeeper:3.9.6
```

**解释**：ZooKeeper 是 Flink HA 的协调组件，3 实例组成 `quorum=2`（多数派）仲裁。Flink 的 JobManager 选主、HA 元数据都由 ZK 管理。`ZOO_MY_ID` 三台各不相同（本台=1）；`ZOO_SERVERS` 三台完全一致，列出全部 3 个节点的选主端口 2888/3888。2888 是 follower↔leader 数据同步端口，3888 是 leader 选举端口。

> **v1.11 网络模式（上机实测，2026-10-24）**：ZK **必须 `--net host`**（与同数据层的 Kafka、SILO 一致），**不能**用 bridge + `-p` 端口映射。官方 `zookeeper` 镜像默认 `quorumListenOnAllIPs=false`，ZK 只监听 zoo.cfg 中**本机** `server.N` 对应的那个 IP（`192.168.110.201`）；bridge 模式下容器没有宿主 IP，于是 3888 选举端口 `BindException: Cannot assign requested address` → 选举监听线程退出 → `Exiting JVM with code 14`（`--restart unless-stopped` 循环拉起死循环）。client 端口 2181 默认监听 0.0.0.0 所以不报错，但 **3888 必败**。这与 Kafka 那次"只起一台转圈（KRaft 票不够）"完全不同。**修复**：加 `--net host` 并删掉三行 `-p`（host 模式不需要、也不该用 `-p`）。

> **v1.6 换镜像**：`bitnami/zookeeper:3.8` 已随 Bitnami 目录调整下架，改用**官方 `zookeeper:3.9.6`**。变量名同步变化：`ZOO_SERVER_ID` → **`ZOO_MY_ID`**（这是最容易照抄旧稿踩的坑，写错会导致三台 myid 全是 1、集群起不来），数据目录从 `/bitnami/zookeeper` 改为 `/data`（事务日志另挂 `/datalog`）。

- `JVMFLAGS="-Xms1g -Xmx1g"`：官方镜像用 ZooKeeper 自身的 `JVMFLAGS` 传 JVM 参数（原 Bitnami 的 `ZOO_HEAP_SIZE` 官方镜像不认），64G 共置机上把堆收成 1G。
- `ZOO_ADMINSERVER_ENABLED=false`：关掉内置 AdminServer（默认占 8080），省内存也避免端口冲突。
- `ZOO_4LW_COMMANDS_WHITELIST="srvr,ruok"`：3.5 起四字命令默认只放 `srvr`，这里显式放开 `srvr`（§5 验证要用它看 Mode: leader/follower）与 `ruok`。
- `ZOO_AUTOPURGE_SNAPRETAINCOUNT=3` + `PURGEINTERVAL=24`：ZK 的 snapshot/事务日志不清会慢涨，在第二块盘的 10G 配额里**必须开自动清理**。
- 数据目录挂到 `/data/store/zookeeper`，不要落在系统盘（`/` 只有 200G，且要与 docker/Doris 日志共用）。

**步骤 9：部署 Flink HA 集群（节点1：JobManager 候选 + TaskManager）**

```bash
cd /data
curl -O https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz
tar -xf flink-1.20.5-bin-scala_2.12.tgz && mv flink-1.20.5 flink

cat > /data/flink/conf/flink-conf.yaml <<'EOF'
# ---- Java：固定用 Temurin 17（Flink 对 Java 21 仅实验性支持，见 §2.7.1）----
env.java.home: /usr/lib/jvm/temurin-17-jdk

# ---- 进程与地址 ----
# jobmanager.rpc.address：各节点写本机主机名（doris-dw01 / dw02 / dw03）。
# 注意语义（v1.12 修正）：ZK HA 模式下 Leader discovery 由 HA service 完成，
# 本项不是"Leader 地址的唯一来源"，只用于本机 JM 绑定与初始连接；真正决定
# 客户端/TM 找到 Leader 的是 high-availability.* 一组配置。
jobmanager.rpc.address: doris-dw01
jobmanager.rpc.port: 6123
jobmanager.bind-address: 0.0.0.0
taskmanager.bind-address: 0.0.0.0
taskmanager.host: doris-dw01              # 各节点写自己主机名（缺省会反查到容器网段/短名）
rest.bind-address: 0.0.0.0                # 监听地址
rest.address: doris-dw01                  # 客户端连接地址（各节点写自己主机名）
rest.port: 8081
# slots / parallelism 是"当前资源条件下的初始基线"，不是固定最佳值（v1.12 澄清）。
# 最终值须按 state size、checkpoint duration、backpressure、CPU、GC、
# RocksDB managed memory 实测后确定（见步骤 9 后的"资源基线说明"）。
taskmanager.numberOfTaskSlots: 4
parallelism.default: 4

# ---- 内存（JM 2G + TM 4G，对应 §0.3.3 账本；同为初始基线，非最佳值）----
jobmanager.memory.process.size: 2048m
taskmanager.memory.process.size: 4096m
taskmanager.memory.managed.fraction: 0.4

# ---- 状态与 checkpoint（Flink 1.20 配置模型，v1.12 统一）----
# 1.20（FLINK-34255）把状态/检查点配置按前缀重组。旧键 state.checkpoints.dir /
# state.savepoints.dir / state.checkpoints.num-retained / state.backend /
# state.backend.incremental 均已 @Deprecated、当前仍兼容生效；本版统一改新键，
# 避免同一份手册混用两代配置风格，也为 Flink 2.0 强制迁移铺路。
state.backend.type: rocksdb
execution.checkpointing.incremental: true
state.backend.rocksdb.memory.managed: true
# RocksDB 本地状态与 io.tmp 分目录：/data/store 已由 Kafka/SILO/Flink/ZK 共享，
# 混在同一目录会互相抢空间（v1.12 拆分）
state.backend.rocksdb.localdir: /data/store/flink/rocksdb
io.tmp.dirs: /data/store/flink/tmp
execution.checkpointing.interval: 30s
execution.checkpointing.min-pause: 10s
# 3min 而非 10min：30s 间隔 + 3 倍余量。10min 只是"大 state 保护值"，
# 正式上线前须以实测 checkpoint duration 复核（见"资源基线说明"）
execution.checkpointing.timeout: 3min
execution.checkpointing.storage: filesystem
execution.checkpointing.dir: s3://flink/checkpoints/
execution.checkpointing.savepoint-dir: s3://flink/savepoints/
execution.checkpointing.num-retained: 3
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

# ---- 高可用（ZooKeeper 选主 + SILO 存元数据；Flink 1.20 键名）----
high-availability.type: zookeeper
high-availability.cluster-id: dbapi-flink
high-availability.zookeeper.quorum: 192.168.110.201:2181,192.168.110.202:2181,192.168.110.203:2181
high-availability.zookeeper.path.root: /flink
high-availability.storageDir: s3://flink/ha/
# 必须显式固定 JM 的 IPC 端口（v1.12 新增，P0 级）。
# 官方原文：默认情况下 JM 会为进程间通信"挑一个随机端口"，HA 下 TM 重连与
# 故障切换后的重新注册都依赖它。不写死 → 防火墙无法精确放行，切换后 TM 连不回来。
high-availability.jobmanager.port: 6123

# ---- SILO（S3 兼容，原 MinIO）承载 checkpoint / HA 元数据 ----
# ⚠️ endpoint 必须指向数据层 VIP .207（v1.12 补齐，见 §3.1 步骤 7b），
# 不要写某个节点的 IP（如 .201）：那样 dw01 整机故障时 Flink 写不进 checkpoint。
# 测试环境 HTTP；生产须换 HTTPS（证书 SAN 必须含 .207，见 §7 易错点 53）
s3.endpoint: http://192.168.110.207:9000
s3.path.style.access: "true"
s3.access.key: <SILO 用户名>
s3.secret.key: <SILO 强口令>
EOF

# 建本地状态目录：RocksDB 与 io.tmp 分开放（v1.12 拆分，避免互相抢盘）
mkdir -p /data/store/flink/rocksdb /data/store/flink/tmp

# S3 插件放 plugins/ 而不是 lib/（v1.12 按 Flink 1.20 插件机制调整）
# 插件机制把 S3 filesystem 的依赖隔离在独立 classloader，避免与 lib/ 下其他 jar
# 冲突；目录名必须与 jar 前缀对应（s3-fs-hadoop）。
# v1.8 修正沿用：用 hadoop 版而非 presto 版——presto 版缺 RecoverableWriter，且已由
# FLIP-555 列为"未来大版本正式弃用"的对象；hadoop 版配置键名同为 s3.*，换插件不改配置。
mkdir -p /data/flink/plugins/s3-fs-hadoop
cp /data/flink/opt/flink-s3-fs-hadoop-*.jar /data/flink/plugins/s3-fs-hadoop/

# 放 MySQL CDC SQL 连接器 + Kafka SQL 连接器（Flink 1.20 构建）
# 注意：Flink CDC 3.6.0 发两套——"Pipeline 引擎包 flink-cdc-3.6.0-1.20-bin.tar.gz"（YAML 管道，
# 内含 flink-cdc-pipeline-*.jar，不含 SQL 连接器）和"SQL 连接器 jar"。本方案用 SQL 模型
#（mysql-cdc 源 + upsert-kafka 汇），只要 SQL 连接器 jar 放 Flink lib/，Pipeline 引擎包用不上。
# 坐标必须带 -1.20 后缀（另有 -2.2 给 Flink 2.2），别拿错；两个 jar 都从 Maven Central 取。
wget -P /root https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/3.6.0-1.20/flink-sql-connector-mysql-cdc-3.6.0-1.20.jar
wget -P /root https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar
cp /root/flink-sql-connector-mysql-cdc-3.6.0-1.20.jar /data/flink/lib/
cp /root/flink-sql-connector-kafka-3.4.0-1.20.jar    /data/flink/lib/

# ---- conf/masters 与 conf/workers：三台内容必须完全一致（v1.12 新增，P0）----
# 官方原文：start-cluster.sh / stop-cluster.sh "rely on conf/masters and conf/workers
# to determine the number of cluster component instances"，且 HA 章节明确要求
# "In order to start an HA-cluster configure the masters file"。
# masters 每行 <host>:<该 JM 的 web UI 端口>；workers 每行一个 TM 主机。
cat > /data/flink/conf/masters <<'EOF'
doris-dw01:8081
doris-dw02:8081
doris-dw03:8081
EOF

cat > /data/flink/conf/workers <<'EOF'
doris-dw01
doris-dw02
doris-dw03
EOF

# ---- 三台免密 SSH（start-cluster.sh 远程拉起 JM/TM 的前提，v1.12 新增）----
# 官方：只有配置了免密 SSH 且目录结构相同时，脚本才能远程启停实例。
# 三台各执行一次：生成密钥（已存在则跳过），然后把本机公钥装到三台。
[ -f /root/.ssh/id_ed25519 ] || ssh-keygen -t ed25519 -N '' -f /root/.ssh/id_ed25519
for h in doris-dw01 doris-dw02 doris-dw03; do
  ssh-copy-id -i /root/.ssh/id_ed25519.pub root@$h      # 按提示输该机 root 口令
done
# 验证：以下三条都应直接返回主机名而不提示输入口令
ssh doris-dw01 hostname && ssh doris-dw02 hostname && ssh doris-dw03 hostname
```

> **启动方式二选一，不要混用（v1.12 重要）**：
>
> - **方式 A（本版采用，推荐）**：只在一台（dw01）执行 `start-cluster.sh`，脚本按 `masters`/`workers` 通过 SSH 把 **3 个 JM + 3 个 TM** 一次拉起，并打印 `Starting HA cluster with 3 masters and 3 peers in ZooKeeper quorum.`——这行输出是唯一能程序化确认"三个 JM 都起来了"的入口。停止用 `stop-cluster.sh`。
> - **方式 B（旧版做法）**：三台分别 `jobmanager.sh start` + `taskmanager.sh start`。功能上也能组成 HA（`jobmanager.sh` 只起本机、不读 masters），但失去批量启停与拓扑自检，且与已建好的 masters 形成两套并存机制，**容易重复起 JM**。
>
> 想走方式 B 就不要建 `masters`/`workers`，二选一，不要既建文件又逐台手起。

```bash
# ⚠️ 执行时机：本条**要等 §3.2、§3.3 的 Flink 也部署完**（三台配置齐备）后再执行。
#    现在（只装完 dw01）不要启动，否则 start-cluster.sh 会 ssh 到还没装 Flink 的 dw02/dw03 而失败。
# 前置检查：三台 ZK 已选出 leader；三台 conf/masters、conf/workers 逐字一致；免密 SSH 已通
for h in doris-dw01 doris-dw02 doris-dw03; do ssh $h hostname; done    # 全程无口令提示才继续

# 只在 dw01 执行一次，拉起 3 个 JM + 3 个 TM（应打印 "Starting HA cluster with 3 masters and 3 peers…"）
/data/flink/bin/start-cluster.sh

# 验证（三台都查，期望各 1 个 JM + 1 个 TM）
for h in doris-dw01 doris-dw02 doris-dw03; do
  echo "== $h =="; ssh $h "jps | grep -E 'StandaloneSessionClusterEntrypoint|TaskManagerRunner'"
done

# 停止（同样只在 dw01 执行）
/data/flink/bin/stop-cluster.sh
```

**解释**：Flink HA 集群在**三台数据节点各跑一个 JobManager 候选 + 一个 TaskManager**。按方式 A，`start-cluster.sh` 依据 `conf/masters`/`conf/workers` 通过 SSH 在三台各拉起 1 个 JM + 1 个 TM，并以 `Starting HA cluster with 3 masters…` 作为拓扑自检；ZK 从中选举出 1 个 Leader JM，其余 2 个为 Standby；TM 向 Leader 注册执行作业。**本版锁 Flink 1.20.5（LTS 线最新补丁，2026-06-03）**，理由见 `DBAPI_Doris组件版本基线.md` §6——最关键的一条是 **Flink CDC 3.6.0 只提供 1.20.x 与 2.2.x 两套构建，最新的 Flink 2.3 没有对应 CDC 包**，升 2.3 等于放弃实时入湖。`jobmanager.rpc.address` 写**本机主机名**（各节点不同，依赖 §2.1 的 /etc/hosts 全节点可解析）——**它只用于本机 JM 绑定与初始连接，不是 Leader discovery 机制**（v1.12 修正）：HA 模式下 Leader 地址由 HA service（ZK）动态解析，客户端与 TM 通过 `high-availability.*` 找到当前 Leader，因此**不要依赖"每台写不同 rpc.address"来实现 HA**，真正要保证的是 `high-availability.jobmanager.port` 固定、三台 hosts 可解析、6123 在数据层放行。

> **`env.java.home` 是本版新增的必填项**：Flink 对 Java 21 的支持**至今是实验性**（上一版手册写成"正式兼容"是错的）。固定用 §2.7.2 装的 Temurin 17，与 Doris 共用一份 JDK；不写这行则 Flink 会去用系统 Java 21。

**本版修正的三个配置项**：

- **`rest.bind-address` 与 `rest.address` 分开写**：旧稿写 `rest.address: 0.0.0.0` 是语义错误——`rest.address` 是**客户端连接 JM 的地址**（会写进返回给客户端的信息里），写 `0.0.0.0` 会让客户端拿到一个不可连接的地址；监听地址应该用 `rest.bind-address`。
- **必须显式写 `taskmanager.host`**：Standalone 模式下缺省时 TM 会用主机名反查结果注册，一旦反查到容器网段或短名，JM 连不回来，表现为"TM 一直注册不上、作业卡在 INITIALIZING"。
- **checkpoint 间隔 30s（不是 10s）**：CDC 位点靠 checkpoint 落盘，10s 间隔在共享盘上会产生持续的小文件写放大（3 个 TM 同时写 RocksDB + 上传 SILO，还要和 Kafka 抢 sdc）。30s 意味着故障恢复后**最多重放 30s 的 binlog**（ODS 是 Duplicate 模型，重放会产生重复行，由 DWD `UNIQUE KEY` 去重兜底——**不要说"幂等写入 ODS"，它不幂等**）。

**关键**：Flink 的 HA 元数据（`high-availability.storageDir`）与 checkpoint（`execution.checkpointing.dir`）全部落在**现有 SILO**（`s3://flink/ha/`、`s3://flink/checkpoints/`）。因为 SILO 只有 3 节点 6 目录的弱冗余（§3.1 步骤 7），`execution.checkpointing.num-retained: 3` 是性价比最高的一道兜底。**本地状态**（`rocksdb.localdir` 与 `io.tmp.dirs`）已分到 `/data/store/flink/rocksdb` 与 `/data/store/flink/tmp` **两个目录**（v1.12 拆分，避免 RocksDB 与临时文件互抢空间），都不放系统盘（写满 `/` 会连带 docker 与 Doris 日志一起挂），也不放 BE 盘（避开 Compaction 抢 I/O）。Flink 主目录本身（`/data/flink`，约 1G 只读二进制）落在系统盘无碍。

> **⚠️ 恢复语义必须写清（v1.12 修正，不要再写"不丢数据"）**：在 checkpoint 成功、源端 binlog 保留充分、Kafka/Doris 下游具备相应恢复条件的前提下，Flink 可从最近成功 checkpoint 恢复。故障恢复期间**可能产生重复处理**——整链语义是**至少一次 / 可能重复**。**ODS 是 `DUPLICATE KEY` 模型、本身不具幂等性**：同一批 binlog 被消费两次，ODS 就是两行。最终"不重复"由 checkpoint + Kafka sink 事务 + **DWD 的 `UNIQUE KEY`（MoW）覆盖写** + DS 每日对账保证，**不等于"端到端不丢且不重"，也绝不能拿 ODS 行数当正确性依据**。重复判定见 §4 的 `GROUP BY order_id HAVING count(*)>1`。
>
> **SILO 入口已改 VIP（v1.12）**：`s3.endpoint` 指向 `.207`，由数据层 keepalived 承载（§3.1 步骤 7b）。dw01 整机故障时 VIP 漂到 dw02/dw03，checkpoint 继续。**但要注意"VIP 漂移 3~10s 内的 checkpoint 会失败一次"**——Flink 会按 `execution.checkpointing.interval` 自动重试，只要 `tolerable-failed-checkpoints` 未耗尽就不影响作业；这就是必须做 SILO 故障演练的原因（§5）。

**资源基线与 4G→8G 策略（v1.12 新增）**

`taskmanager.numberOfTaskSlots: 4`、`taskmanager.memory.process.size: 4096m`、`parallelism.default: 4` 是**当前资源条件下的初始基线，不是最佳值**。最终值须按 state size、checkpoint duration、backpressure、CPU、GC、RocksDB managed memory 实测确定；`parallelism.default` 还要与 MySQL CDC 源并行度、Kafka 分区数、Doris sink 并行度、TM slots 数对齐（**Kafka topic 分区数就是 upsert-kafka 汇端的并行度上限**）。

| 阶段 | TM 进程内存 | 触发条件 | 操作 |
|---|---|---|---|
| 常态 | 4096m | — | 维持 |
| 首次大表全量 | 8192m | 全量期 checkpoint 连败（`Checkpoint expired before completing`）、RocksDB 写缓冲 OOM、或 TM 频繁 Full GC | ① 三台 `flink-conf.yaml` 改 `taskmanager.memory.process.size: 8192m`；② **必须重启 TM 才生效**（无热更新）；③ 逐台重启（停一台 → 作业重分配到另两台 → 再起下一台）比全停更平滑，但恢复更慢，维护窗口内可直接三台同停同起；④ **全量结束必须改回 4096m 并重启**，否则三台各多占 4G，64G 共置机上会与 BE/Kafka 争内存 |

全量期验收阈值：checkpoint duration < timeout 的 50%（即 < 90s）、backpressure 不持续 HIGH、单次 GC pause < 1s。**超阈值说明 8G 不够，应拆作业或调小 CDC chunk size，而不是继续加内存。**

**步骤 10：确认 Flink 进程与 Web UI**

```bash
ps -ef | grep -E 'StandaloneSession|TaskManager' | grep -v grep
curl -s 192.168.110.201:8081/overview        # 返回 JSON 即 JM 已起
```

**解释**：`StandaloneSessionClusterEntrypoint` 是 JM 进程，`TaskManagerRunner` 是 TM 进程。Web UI 端口 8081，三台都能访问，但只有 Leader JM 的 UI 显示 Running Jobs。

**v1.11 变更说明（原步骤 11 已整段迁出）：数据节点不再部署 etcd**

etcd 集群的三个成员现在**全部落在应用层**：`dbapi-gw01(.204)` / `dbapi-gw02(.205)` / `dbapi-gw03(.206)`，分别见 **§3.4 步骤 2**、**§3.5 步骤 2**、**§3.6 步骤 2**。

> **为什么改**：v1.10 及以前只有 2 台应用节点，凑不出 3 个等价的 etcd 成员，只能拉 dw01 跨层投票（"5 台约束下的最优解"），代价是 **dw01 一次故障同时带走 Doris 的一个副本和 etcd 的 1/3 仲裁**——两个本来无关的故障域被绑在一起。现在应用层有 3 台，etcd 可以自给自足，**故障域与应用层完全重合，数据层怎么坏都不影响 APISIX 配置读取与 Patroni 选主**。
>
> 顺带三项收益：① 数据节点少一个常驻容器（省 0.5G 内存，`/data/store` 上不再需要 etcd 数据目录）；② 防火墙少 4 条跨层 rich-rule（2379/2380 双向）；③ 原来"维护 dw01 前先确认 etcd 还有 2 票"这条运维纪律不再需要。
>
> **迁移检查**：若这台机器上曾按 v1.10 起过 etcd 容器，先 `docker rm -f etcd && rm -rf /data/store/etcd` 清掉，否则它以 `etcd-dw01` 的身份重连新集群会因成员列表不含自己而**反复报 `etcdserver: member not found` 并不断发起重试风暴**。

---

---

### 3.2 节点 doris-dw02（192.168.110.202 ｜ 数据/入湖层 · Follower）

> 与 doris-dw01 的差异：FE 以 Follower 加入（带 --helper）；Kafka `KAFKA_NODE_ID=2`（`CLUSTER_ID` 与 dw01 相同）；SILO 同分布式命令；**ZooKeeper `ZOO_MY_ID=2`、Flink `jobmanager.rpc.address: doris-dw02`**。其余组件命令相同。

**步骤 1：确认 §2 系统准备（hostname 应返回 doris-dw02）**  
**步骤 2：部署 Doris 二进制**（同 3.1 步骤 2）  
**步骤 3：配置 FE 与 BE**（同 3.1 步骤 3）  
**步骤 4：启动 FE（Follower，带 --helper 指向 doris-dw01）**

```bash
/data/doris/fe/bin/start_fe.sh --helper 192.168.110.201:9010 --daemon
```

**解释**：`--helper 192.168.110.201:9010` 让本 FE 去 doris-dw01 报到加入集群，成为 Follower（参与元数据同步与选主，但不担 Leader 写）。它须被 doris-dw01 用 `ALTER SYSTEM ADD FOLLOWER` 接纳（第 4 章）。  
**步骤 5：启动 BE**（同 3.1 步骤 5）  
**步骤 6：部署 Kafka（broker2）**：命令同 3.1 步骤 6，仅改 `KAFKA_NODE_ID=2` 与 `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://192.168.110.202:9092`；**`CLUSTER_ID` 填 dw01 生成的那个值**。  
**步骤 7：部署 SILO（节点2）**：命令同 3.1 步骤 7（三节点地址不变）。  
**步骤 7b：SILO VIP keepalived（节点2，v1.12）**：配置文件同 3.1 步骤 7b，**只把 `priority` 改为 `120`**；`virtual_router_id 53`、VIP `192.168.110.207/24`、`auth_pass` 必须与节点1 一致。  
**步骤 8：部署 ZooKeeper（myid=2）**：命令同 3.1 步骤 8，仅改 `-e ZOO_MY_ID=2`。  
**步骤 9：部署 Flink HA（节点2）**：tarball 与 conf 同 3.1 步骤 9，**三处改成本机名**——`jobmanager.rpc.address: doris-dw02`、`taskmanager.host: doris-dw02`、`rest.address: doris-dw02`；`conf/masters`、`conf/workers` 与 dw01 **逐字一致**，S3 插件目录（`plugins/s3-fs-hadoop/`）与两个连接器 jar 本机同样要有一份。**本机不要再执行 `jobmanager.sh start`**（v1.12）：按方式 A，dw02 的 JM 与 TM 由 dw01 上的 `start-cluster.sh` 通过免密 SSH 远程拉起，本机只需保证 ssh 免密可达、`/data/flink` 目录结构与 dw01 相同。

---

### 3.3 节点 doris-dw03（192.168.110.203 ｜ 数据/入湖层 · Follower）

> 与 doris-dw01 的差异：FE 以 **Follower** 加入（带 `--helper`，与 dw02 相同）；Kafka `KAFKA_NODE_ID=3`；SILO 节点3；ZooKeeper `ZOO_MY_ID=3`；Flink 三处主机名改 `doris-dw03`。其余组件命令相同。
>
> **v1.11：本节点不再跑 Redis 哨兵**（原步骤 10 已删除）。三个哨兵全部在应用层 `gw01/gw02/gw03`，见 §3.4/§3.5/§3.6 各自的"部署 Redis"步骤。数据层三台现在只跑 Doris FE/BE + Kafka + SILO + ZK + Flink，**职责单一**。

**步骤 1：确认 §2（hostname 应返回 doris-dw03）**  
**步骤 2：部署 Doris 二进制**（同 3.1 步骤 2）  
**步骤 3：配置 FE 与 BE**（同 3.1 步骤 3）  
**步骤 4：启动 FE（Follower，带 --helper）**

```bash
/data/doris/fe/bin/start_fe.sh --helper 192.168.110.201:9010 --daemon
```

**解释**：Follower 与 dw02 完全相同——启动命令都带 `--helper`，由 doris-dw01 用 `ALTER SYSTEM ADD FOLLOWER` 接纳（第 4 章）。**Follower 与 Leader 一起构成 BDBJE 三副本并参与选举**：Leader 宕机后剩余 2 个 Follower 达到多数派（2/3），可自动选出新 Leader，这就是 FE 高可用的全部意义。**Observer 只同步元数据、不投票，不能用来凑高可用配额**，仅在大集群做读扩展时额外添加。

> **旧稿的致命错误**：把 dw03 配成 Observer，投票成员只剩 Leader + dw02 两台。Leader 一宕，剩 1 个 Follower < 多数派 2，**选不出新 Leader，FE 僵死（读写都不行）**，表现为"整个 Doris 不可用"——高可用名不副实。

**步骤 5：启动 BE**（同 3.1 步骤 5）  
**步骤 6：部署 Kafka（broker3）**：`KAFKA_NODE_ID=3`，`KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://192.168.110.203:9092`，`CLUSTER_ID` 同 dw01。  
**步骤 7：部署 SILO（节点3）**：命令同 3.1 步骤 7。  
**步骤 7b：SILO VIP keepalived（节点3，v1.12）**：同上，**`priority` 改为 `100`**（全场最低，作为最后一级接盘）。  
**步骤 8：部署 ZooKeeper（myid=3）**：命令同 3.1 步骤 8，仅改 `-e ZOO_MY_ID=3`。  
**步骤 9：部署 Flink HA（节点3）**：conf 中 `jobmanager.rpc.address`、`taskmanager.host`、`rest.address` 三处均改 `doris-dw03`；`masters`/`workers`、plugins 目录、连接器 jar 同 dw01 一致。**同样不在本机启动**，由 dw01 的 `start-cluster.sh` 远程拉起（v1.12）。

**v1.11 变更说明（原步骤 10 已删除）：本节点不再部署 Redis 哨兵**

Redis 的第 3 个哨兵现在由 `dbapi-gw03` 承担（**§3.6 步骤 5**），与 gw01/gw02 的哨兵同处应用层。

> **为什么改**：v1.10 跨层放这个哨兵的唯一目的是"凑够 3 个奇数、让 quorum=2 的多数派授权成立"。但代价是 **dw03 的 26379 要对应用层开放、gw01/gw02 的 26379 要对 dw03 开放**（双向跨层规则），且 dw03 一宕，Redis 故障转移就从"容忍 1 台"退化为"再坏一台就切不了"。现在应用层自己就有 3 台，哨兵与 Redis 主从**同层同组**，故障域一致。
>
> 迁移检查：若曾按 v1.10 起过 `redis-sentinel` 容器，`docker rm -f redis-sentinel && rm -rf /data/redis-sentinel` 清掉；该目录只剩一个 `sentinel.conf`，不存数据，删除无风险。

---


### 3.4 节点 dbapi-gw01（192.168.110.204 ｜ 应用服务层 · 主用/引导）

> 本节点角色：APISIX + DBAPI + PG 主库（Patroni 托管）+ Redis(主) + etcd 集群成员(1) + keepalived(两个 VIP)。

**步骤 1：确认 §2 系统准备（hostname 应返回 dbapi-gw01）**

**步骤 2：部署 etcd 集群成员（成员1）+ APISIX**

> v1.11：etcd 三个成员全部在应用层（`etcd-gw01/.204`、`etcd-gw02/.205`、`etcd-gw03/.206`）。本节是成员1，成员2/3 见 §3.5 步骤 2、§3.6 步骤 2。

```bash
# etcd 集群成员（gw01=成员1；三台的 --initial-cluster 必须逐字一致，仅 --name 与两个 IP 不同）
mkdir -p /data/etcd
docker run -d --name etcd --restart unless-stopped --net host \
  -v /data/etcd:/etcd-data \
  -e GOMEMLIMIT=512MiB \
  quay.io/coreos/etcd:v3.7.2 \
  etcd --name etcd-gw01 \
  --data-dir /etcd-data \
  --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://192.168.110.204:2379 \
  --listen-peer-urls http://0.0.0.0:2380 \
  --initial-advertise-peer-urls http://192.168.110.204:2380 \
  --initial-cluster "etcd-gw01=http://192.168.110.204:2380,etcd-gw02=http://192.168.110.205:2380,etcd-gw03=http://192.168.110.206:2380" \
  --initial-cluster-state new \
  --quota-backend-bytes=2147483648 \
  --auto-compaction-mode=periodic \
  --auto-compaction-retention=1h \
  --snapshot-count=10000 \
  --heartbeat-interval=100 \
  --election-timeout=1000

# 验证 3 成员集群健康（任一台执行）
docker exec etcd etcdctl endpoint status --cluster -w table
# 预期：3 个 endpoint 全部 true，其中 1 个 IS_LEADER=true
```

**解释**：etcd 在本方案里是**应用层两大组件的协调中枢**——① APISIX 的路由/插件配置中心（**三台** APISIX 读同一份，发布 API 一次生效，VIP 漂移不丢路由）；② Patroni 的选主仲裁（PG 谁是主库由 etcd lease 决定）。etcd 必须**奇数节点**：2 节点 quorum=2，任何一台宕机即丢仲裁，比单点还脆；3 成员 quorum=2，容忍 1 台宕。

**v1.11：三个成员全部在应用层**（gw01 + gw02 + gw03）。v1.10 及以前是 `gw01+gw02+dw01` 跨层投票，那是只有 2 台应用节点时的妥协——代价是 dw01 一次故障同时带走 Doris 副本与 1/3 仲裁。现在应用层已有 3 台，etcd 与它服务的两个用户（APISIX、Patroni）**同层同组**：数据层怎么折腾都不影响控制面，且这三台本来就为 PG 1主2从 而必须存在，etcd 不额外要求任何节点常驻。三台互访走 `ipset:app-nodes`（§6.4），**不再有任何跨层放行**。

> **v1.6 升 3.7.2 的注意事项**（3.5.15 带 CVE-2026-33413，且 3.5 线已落后两个小版本）：
>
> - **tag 不要带架构后缀**。3.7 起只发多架构镜像，`v3.7.2-linux-amd64` 这类写法会直接拉取失败，必须用 `v3.7.2`。
> - **3.7 移除了全部 `--experimental-*` 参数，进程遇到即拒绝启动**。本命令用的是稳定参数（`--quota-backend-bytes`、`--auto-compaction-*` 等），不受影响；以后加参数时别照抄老博客里的 `--experimental-*`。
> - **3.7 完全移除了 v2 store / v2 API**。若仍有工具用 etcd v2 协议，会失效（我们只用 v3，无影响）。
> - `--snapshot-count` 在 3.7 里**仍保留**（3.7 仍在生成 v2 快照以兼容旧读），但 **3.8 会删除它**——将来升 3.8 时这行要一并去掉。
> - **etcd 与 Patroni 必须成对升级**：etcd 从 3.6.9/3.5.28 起收紧了认证——开启认证后，集群拓扑读取与 lease 续约不再允许匿名调用；Patroni **4.1.1** 才开始在这两条路径上做认证。本方案 etcd 不开认证（内网 + ipset 限制），所以不触发；但**今后若要给 etcd 开 auth/TLS 客户端认证，Patroni 必须 ≥4.1.1**（本手册用 4.1.5）。

后端补充参数（对"配置中心 + 选主仲裁"双用户都必要）：

| 参数                                                                                | 作用                                                                   |
| --------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `--quota-backend-bytes=2GiB`                                                      | 默认本身即 2GiB，但**写满会直接拒绝写入**（APISIX 改不了路由、Patroni 抢不到 lease）；显式声明便于监控告警 |
| `--auto-compaction-retention=1h`                                                  | APISIX 每次路由变更都写新 revision，不压缩 db 体积会持续增长；1h 压缩对 500G 盘成本可忽略          |
| `--snapshot-count=10000` / `--heartbeat-interval=100` / `--election-timeout=1000` | LAN 内推荐值，显式写出便于复制；选举超时 1s 意味着单成员故障时选主在秒级完成                           |
| `-e GOMEMLIMIT=512MiB`                                                            | Go 运行时软上限，防止 etcd 异常时吃满内存（32G 共置机上必须设）                               |

APISIX 走 compose（**不再含独立 etcd 服务**），`/opt/apisix/docker-compose-apisix.yml`：

```yaml
services:
  apisix:
    image: apache/apisix:3.19.0-debian
    network_mode: host                 # 必须 host：要连宿主 etcd 2379 与 DBAPI 8080
    volumes: ["./config.yaml:/usr/local/apisix/conf/config.yaml:ro"]
```

> ⚠️ **为什么是 `network_mode: host` 而不是 `ports: ["9080:9080"]`（v1.7 修正）**：APISIX 启动时就要连 `192.168.110.204:2379`（宿主 etcd，`--net host` 运行）与上游 `192.168.110.204:8080`（宿主 DBAPI，systemd 运行）。走 bridge 时这两条出站流量的源地址是 `172.17.0.1`，既不属 `ipset:app-nodes` 也不在 `public` zone 的放行列表里，会被 `appnode` zone 拒绝——表现为 **APISIX 启动即报 etcd 连接失败、或上游 502**。host 网络下容器直接监听宿主 9080/9443，防火墙规则就是 §6.4 那几条，不再有 NAT 夹层。
>
> 副作用：host 模式下 `extra_hosts` 不生效（它只对自定义 bridge 网络有用），主机名解析改用宿主 `/etc/hosts`——§2.1 已经把六台的 `doris-dw01/02/03`、`dbapi-gw01/02/03` 写进去了，无需额外配置。

`config.yaml` 的 etcd 段（**三台 APISIX 完全一致，都指向 3 成员集群**）：

```yaml
apisix:
  node_listen: 9080
  enable_ipv6: false
  worker_processes: 8
  worker_rlimit_nofile: 655350

deployment:
  role: traditional
  role_traditional:
    config_provider: etcd
  etcd:
    host:                          # 注意：是 host，不是 endpoints（endpoints 是 Dashboard 的写法）
      - "http://192.168.110.204:2379"
      - "http://192.168.110.205:2379"
      - "http://192.168.110.206:2379"
    prefix: /apisix
    timeout: 30
  admin:
    allow_admin:
      - 127.0.0.1/32
      - 192.168.110.204/32         # v1.9 收窄：原写 192.168.110.0/24，而 DS 与办公终端同在此网段，
      - 192.168.110.205/32         #   Admin API 9180 可增删路由/上游，暴露面应按主机收，不按网段收
      - 192.168.110.206/32         # v1.11 增补第三台；其余机器一律不可调 Admin API
    admin_key:
      - name: admin
        key: <强随机串>
        role: admin
```

```bash
mkdir -p /opt/apisix && cd /opt/apisix
docker compose up -d
```

**解释**：APISIX 是**唯一对外入口**，监听 9080/9443。

> **v1.6 升到 3.19.0 的原因**：APISIX 只维护「最新版 + 最新 LTS」两条线，**3.9 在 2024 年 4 月就已停止支持**。它是整个平台唯一对外暴露的组件，长期跑无补丁版本不可接受。当前最新为 3.19.0（2026-09-28）。升级后需回验三处：`deployment.etcd.host` 键名仍有效、`key-auth`/`limit-req`/`prometheus` 插件正常加载、Admin API 的 `admin_key` 生效。

本步是 v1.4 的关键变更：三台 APISIX 的 `config.yaml` 指向**同一个 etcd 集群、同一个 prefix `/apisix`**——发布 API 时只需经 VIP 调一次 Admin API，三台即刻同时生效；VIP 漂移到 gw02/gw03 后路由**零缺失**（v1.3.1 是每台一套独立 etcd 须双写，漏写一台即路由缺失）。启动后通过 Admin API 建路由，上游指向**三台 DBAPI**（`192.168.110.204:8080,192.168.110.205:8080,192.168.110.206:8080`），启用 `key-auth`、`limit-req`、`prometheus`。etcd 用 `--net host`（省端口映射，成员间 2380 直通）。

> **为什么三台都要起 APISIX，而不是只在前两台起（v1.11）**：keepalived 的 VI_1 跟随 APISIX 健康，三台都是 VIP 候选持有者。若 gw03 不起 APISIX，`chk_apisix` 必然失败 → priority 从 100 掉到 20 —— 平时看不出问题，但 **gw01/gw02 同时不可用时 VIP 无人可接，API 入口直接变黑洞**（VRRP 无候选者，路由通告都不再发出）。三台同构是这套拓扑的前提，不是冗余浪费。

> **`host` 与 `endpoints` 的区别（旧稿写错的地方）**：APISIX 3.x 的 `config.yaml` 里 etcd 地址键名是 **`host`**；`endpoints` 是 APISIX **Dashboard** 的配置键。写成 `endpoints` 时 APISIX 会**忽略该段**并回退到默认 `127.0.0.1:2379`，结果是三台 APISIX 都连不上 etcd——启动报连接失败，或路由永远为空。同理必须显式写 `deployment.role: traditional` + `role_traditional.config_provider: etcd`。
>
> **Admin API 必须保护**：`admin_key` 换成强随机串，`allow_admin` 只放**三台应用节点 + 127.0.0.1**（v1.9 起不按网段放），9180 不要对外开。验证：`curl -H 'X-API-KEY: <key>' http://127.0.0.1:9180/apisix/admin/routes` 应返回 200，`docker logs apisix` 不应出现 etcd 连接错误。

**步骤 3：部署 DBAPI + 发布控制台**  
`/etc/systemd/system/dbapi.service`：

```ini
[Unit]
Description=DBAPI Service
After=network.target docker.service
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
[Install]
WantedBy=multi-user.target
```

`application-prod.yml` 关键项（dbapi-gw01 视角）：

```yaml
doris: jdbc:mysql:loadbalance://192.168.110.201:9030,192.168.110.202:9030,192.168.110.203:9030/dw_db
pg:    jdbc:postgresql://192.168.110.200:5432/dbapi_db
redis: 哨兵节点 192.168.110.204:26379,192.168.110.205:26379,192.168.110.206:26379  (mymaster)
minio: http://192.168.110.207:9000        # 键名为 DBAPI 既有配置项，底层由 SILO 提供 S3 兼容接口
# ✅ v1.12：指向数据层 VIP .207（keepalived，§3.1 步骤 7b），不再写单节点 .201。
#    这样 dw01 整机故障时 DBAPI 的大结果集导出仍可用；查询链路本身不经过 SILO。
```

```bash
useradd -r -s /sbin/nologin dbapi
cp dbapi.jar /opt/dbapi/
systemctl daemon-reload && systemctl enable --now dbapi
```

**解释**：DBAPI 只查 Doris ADS 层，不直连业务库。`loadbalance` JDBC 在 3 个 FE 间自动选活节点；**PG 连接串指向 VIP `192.168.110.200` 而非某台具体机器**——主库切换时 Patroni+keepalived 把 VIP 自动漂到新主，DBAPI **零配置改动**；Redis 走哨兵地址（**三个哨兵都写上**，客户端会逐个尝试）；对象存储（SILO，配置键名 `minio`）指向数据层。防火墙须放行到数据层（第 6 章）。

JVM 参数说明：`-Xmx3g`（旧稿 2G 偏紧——Doris 结果集反序列化 + 字段脱敏 + 导出都在堆内）、`MaxDirectMemorySize=512m` 限制 Netty/S3 SDK（SILO）的堆外内存防泄漏、`LimitNOFILE=655350`（**systemd 不读 `limits.conf` 的通配 `*`，必须显式写**，否则容器/进程仍是默认 1024）。32G 机器给 3G 堆 + 其余组件后仍有 13G 余量。

**步骤 4：部署 Patroni + PostgreSQL（主库候选，自动故障转移）**

`/etc/patroni/patroni.yml`（gw01 视角）：

```yaml
scope: dbapi-pg
namespace: /dbapi/
name: pg-gw01
restapi:
  listen: 0.0.0.0:8008          # keepalived VI_2 探针 :8008/primary 打这里
etcd3:
  hosts: 192.168.110.204:2379,192.168.110.205:2379,192.168.110.206:2379   # 复用应用层 etcd 集群
bootstrap:
  dcs:
    ttl: 30                     # 主库失联 30s 判定，触发选举
    loop_wait: 10
    retry_timeout: 10
    maximum_lag_on_failover: 1048576    # 延迟超 1MB 的从库不参与选举，防丢数据
    synchronous_mode: true      # 已确认开启：只提升同步从库，RPO=0（不开 strict：从库宕时降级异步保可用）
    postgresql:
      use_pg_rewind: true       # 旧主恢复后自动降级 rejoin，无需人工重建
      # v1.11：PG 改 1 主 2 从，复制与业务访问都要覆盖三台应用节点
      pg_hba:
        - host replication replicator 192.168.110.204/32 md5
        - host replication replicator 192.168.110.205/32 md5
        - host replication replicator 192.168.110.206/32 md5
        # v1.9 收窄：DS 集群在本网段，写 /24 等于把 5432 对 DS 及网段内所有机器开放（DS 无需连 PG）
        - host all all 192.168.110.204/32 md5
        - host all all 192.168.110.205/32 md5
        - host all all 192.168.110.206/32 md5
        - host all all 192.168.110.200/32 md5    # VIP .200：兜底条目。DBAPI 实际源地址是节点 IP(.204~.206)，此行防“从 VIP 本地发起连接”被拒
  initdb:
    - encoding: UTF8
postgresql:
  listen: 0.0.0.0:5432
  connect_address: 192.168.110.204:5432
  data_dir: /data/pg/data
  bin_dir: /usr/pgsql-17/bin        # 必填：rpm 装的 PG 二进制不在 PATH 里，漏了 Patroni 找不到 initdb/pg_ctl
  authentication:
    superuser:    {username: postgres,   password: <强口令>}
    replication:  {username: replicator, password: <强口令>}
  parameters:
    unix_socket_directories: /tmp
    # 内存（32G 机器：6G 共享缓存）
    shared_buffers: 6GB
    effective_cache_size: 12GB
    work_mem: 16MB
    maintenance_work_mem: 1GB
    wal_buffers: 16MB
    # 连接：DBAPI 2 实例 × 25 + DS + 运维监控
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
    # 复制（synchronous_commit 由 Patroni synchronous_mode 托管，勿手写 synchronous_standby_names）
    wal_level: replica
    max_wal_senders: 10
    max_replication_slots: 10
    wal_keep_size: 1GB
    hot_standby: on
    wal_log_hints: on          # v1.8 新增：pg_rewind 依赖 wal_log_hints 或 data checksums；
                               # PGDG 的 initdb 默认不开 checksums，不写这行则旧主回归时 rewind 失败
    hot_standby_feedback: on   # v1.8 新增：从库长查询不会被主库 vacuum 打断
    # 观测
    shared_preload_libraries: pg_stat_statements
    log_min_duration_statement: 1000
tags:
  nofailover: false
  noloadbalance: false
```

```bash
# 0) 若按旧版手册起过手搓 postgres:16 容器或 patroni 容器，先清掉（未投产数据可弃）
docker rm -f pg patroni 2>/dev/null; rm -rf /data/pg

# 1) 装 PostgreSQL 17 + Patroni：用 PGDG 官方仓库的 rpm 包，不依赖 ghcr 镜像
dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-10-x86_64/pgdg-redhat-repo-latest.noarch.rpm
dnf config-manager --enable pgdg-rhel10-extras 2>/dev/null || true   # patroni-etcd 在 extras 仓库
dnf install -y postgresql17-server postgresql17-contrib patroni patroni-etcd
patroni --version                     # 期望 4.1.5

# ⚠️ 不要执行 postgresql-17-setup initdb —— 由 Patroni 自己 initdb，手搓会让 data_dir 非空而启动失败

# 2) 数据目录：属主必须是 postgres（Patroni 以 postgres 身份跑 initdb 与 postgres）
mkdir -p /data/pg
chown -R postgres:postgres /data/pg
# SELinux enforcing 下必须注册 fcontext 再 restorecon（/data/pg 默认上下文是 default_t，
# PG 会报 permission denied；只跑 restorecon 无效——策略里没有这条路径的规则）
semanage fcontext -a -t postgresql_var_lib_t "/data/pg(/.*)?" 2>/dev/null || true
restorecon -Rv /data/pg

# 3) 写入配置并启动（PGDG 包自带 patroni.service）
#    ⚠️ v1.8 修正：配置文件路径全文统一为 /etc/patroni/patroni.yml（旧稿 §3.4 与 §3.5 一处写
#       /etc/patroni/patroni.yml、一处写 /etc/patroni.yml，照抄会让 systemd 找不到配置而启动失败）。
#    落地前用下行复核一次单元里写的到底是哪个路径，以单元为准：
systemctl cat patroni | grep -i ExecStart
#    patroni.yml 内容见本节上方，落到 /etc/patroni/patroni.yml
chown postgres:postgres /etc/patroni/patroni.yml && chmod 600 /etc/patroni/patroni.yml
systemctl daemon-reload && systemctl enable --now patroni

# 验证：本节点应成为 Leader
curl -s http://127.0.0.1:8008/primary -o /dev/null -w '%{http_code}\n'   # 预期 200
patronictl -c /etc/patroni/patroni.yml list                                     # pg-gw01 应为 Leader
```

> **v1.6 为什么从容器改成宿主机服务**：`ghcr.io/zalando/patroni` 在当前网络拉不到（你已确认）。PostgreSQL 社区**为 AlmaLinux/RHEL 官方出包**（`patroni` + `patroni-etcd`），系统 Python 依赖现成、systemd 单元现成，且比容器方案少两层麻烦：不再需要 `--net host` 处理通告地址，也不存在"容器 UID 与 `/data/pg` 属主对不上、PG 拒绝以 root 启动"的问题。
>
> **降级路径**：若 `pgdg-rhel10` 在你的源里还没发布，改用 venv 方式：`python3 -m venv /opt/patroni && /opt/patroni/bin/pip install "patroni[psycopg3,etcd3,systemd]==4.1.5"`，再手写 systemd 单元（`ExecStart=/opt/patroni/bin/patroni /etc/patroni/patroni.yml`、`User=postgres`、`KillMode=process`），`bin_dir` 同理指向 rpm 的 PG。

**解释**：Patroni 是 **PG 高可用托管器**——它替你完成"谁是主、谁是从、主挂了谁顶上"的全套决策：**① 它把 PG 主从状态注册进 etcd 集群**，用 lease（租约，ttl=30s）判定主库存活；**② 主库失联超时后，幸存节点抢到 lease 自动 promote**，只提同步从库（`synchronous_mode` 保证已提交事务不丢）；**③ 旧主恢复后 `use_pg_rewind` 自动降级为从库**重新加入，全程无人工。PG 直接监听 5432、REST API 监听 8008，由 systemd 托管（`systemctl restart patroni` 即重启 PG）。**三台**各跑一个 Patroni 服务（1 主 2 从），DBAPI 经 VIP `192.168.110.200` 连接，切换零感知。

**步骤 5：部署 Redis（主）+ 哨兵**

```bash
# 主库：必须 --net host（桥接网络下故障转移后哨兵会通告容器内网 IP，DBAPI 连不上新主）
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

# 本机哨兵（第 1 个）
# v1.8 修正：哨兵配置必须落成配置文件并挂载，不能用 SENTINEL SET 运行时下发
#   后者只存在于内存，容器/主机重启后 MONITOR 丢失，哨兵变成"不知道要监控谁"的空进程
mkdir -p /data/redis-sentinel
cat > /data/redis-sentinel/sentinel.conf <<'EOF'
port 26379
bind 0.0.0.0
protected-mode no                    # 哨兵默认 protected-mode=yes，跨节点通告会被拒（3 哨兵互相发现的前提）
sentinel monitor mymaster 192.168.110.204 6379 2
sentinel auth-pass mymaster <强口令>
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 60000
sentinel announce-ip <本机IP>          # 三台各填本机：gw01=.204 / gw02=.205 / gw03=.206
EOF
chmod 666 /data/redis-sentinel/sentinel.conf    # 容器内 redis(uid 999) 要能重写它（哨兵会回写 known-replica）

docker run -d --name redis-sentinel --restart unless-stopped --net host \
  -v /data/redis-sentinel/sentinel.conf:/usr/local/etc/redis/sentinel.conf:rw,z \
  redis:7.4 redis-server /usr/local/etc/redis/sentinel.conf --sentinel
```

**解释**：Redis 作 DBAPI 热查询缓存（`maxmemory 2gb` + `allkeys-lru`，32G 机器上给 2G 足够；旧稿 4G 偏大）。**哨兵必须 3 个**——本机 1 个 + gw02 1 个（§3.5 步骤 5）+ gw03 1 个（§3.6 步骤 5），`quorum=2`，**三个全在应用层**（v1.11 起不再跨层到 dw03）。

**为什么是 3 个而不是 2 个（旧稿的假高可用）**：哨兵的"客观下线"只需 quorum 个同意，但**执行故障转移需要多数派授权（majority of all sentinels）**。只有 2 个哨兵时多数派 = 2，任一台宕机就永远凑不齐 → 主库真挂了也不会切换。3 哨兵 quorum=2 才是标准配置。

> **3 台应用层的额外好处（v1.11）**：3 个哨兵分在三台上，一台在做维护时剩 2 个哨兵、多数派 2 仍成立，**仍能完成故障转移**；2 台时代只要有一台下线维护，整个 Redis 自动切换能力就等于零——而恰恰维护窗口正是最容易出问题的时候。

**为什么必须 `--net host`**：`-p 6379:6379` 桥接网络下，哨兵记录的是容器内网地址（本方案为 172.31.x.x，默认才是 172.17.x.x），故障转移后把它通告给客户端，DBAPI 连不上新主（§7-13 已记录该坑，本版直接按 host 网络落地，不再留隐患）。

**步骤 6：部署 keepalived（两个 VIP：API 入口 `.199` + PG 主库 `.200`）**

```bash
yum install -y keepalived
```

`/etc/keepalived/keepalived.conf`（**gw01 视角：priority 150**；gw02 用同一份文件仅把两处 `priority` 改成 **`120`**（§3.5），gw03 改成 **`100`**（§3.6））：

```
vrrp_script chk_apisix {
    script "curl -s -o /dev/null http://127.0.0.1:9080/ || exit 1"
    interval 2
    fall 2                        # 连续 2 次失败才降级，防抖动误切
    rise 1
    weight -80                    # ★ 必须让"降级后的优先级"低于全场最低优先级(100)，见下方推导
}
vrrp_script chk_pg {
    script "curl -sf http://127.0.0.1:8008/primary || exit 1"
    interval 2
    fall 2
    rise 1
    weight -80                    # ★ 同上
}
vrrp_instance VI_1 {
    state BACKUP
    interface eth0
    virtual_router_id 51
    priority 150                  # 三台：gw01=150 / gw02=120 / gw03=100
    nopreempt
    advert_int 1
    authentication { auth_type PASS; auth_pass <VRRP认证口令A> }
    virtual_ipaddress { 192.168.110.199/24 }
    track_script { chk_apisix }
}
vrrp_instance VI_2 {
    state BACKUP
    interface eth0
    virtual_router_id 52          # 与 VI_1 区分
    priority 150                  # 三台同 VI_1：150 / 120 / 100
    nopreempt
    advert_int 1
    authentication { auth_type PASS; auth_pass <VRRP认证口令B> }
    virtual_ipaddress { 192.168.110.200/24 }
    track_script { chk_pg }
}
```

```bash
systemctl enable --now keepalived
ip a | grep -E '192.168.110.199|192.168.110.200'
```

**解释**：keepalived 用 VRRP 浮动两个 VIP，**各跟各的健康源**——VI_1 的 `192.168.110.199` 探本机 APISIX（9080 通则抢到），VI_2 的 `192.168.110.200` 探**本机 Patroni 的 `/primary` 接口**（该接口**只有当前 PG 主库返回 200**）。主库切换后：新主的 `/primary` 转 200 → 新主抢到 VI_2；旧主转 503 → 自动释放。外部调用方与 DBAPI 始终指向固定 VIP，**无需 Nginx 前置、切换零配置改动**。`interface eth0` 按 `ip a` 实际网卡改。

**三台的 priority 与 weight 推导（v1.11 重算，这是最容易漏的高可用假象）**：vrrp_script 的 `weight` 是**失败时从 priority 上扣除的分值**，判定规则不再是"大于两台之差"，而是：

> **降级后的优先级必须低于「全场最低的那个 priority」，否则 VIP 不会交给任何其它节点。**

三台取值 **gw01=150 / gw02=120 / gw03=100**，最坏情况是 gw01 健康分失败：`150 − 80 = 70 < 100`，**低于全部其它节点**，VIP 必定让出；若只取 −30（旧稿），`150−30=120`，比 gw03 的 100 还高 —— **gw01 的 APISIX 已经死了，VIP .199 却还留在它身上**，外部调用全失败；同理 PG 切主后 `150−30=120 > 100`，**VIP .200 留在已降级为从库的 gw01 上，DBAPI 写入只读库直接报错**。

两个 VIP 都"看起来配了高可用、实际永不漂移"，比不配更危险（不配至少会排查）。**本版统一取 −80**，留 30 的余量（`150−80=70`，最低 100，余量 30）。

**为什么差值是 20 一档（150/120/100）**：差值太小（如 150/149/148）时任何一次探测抖动都可能改变排名，VIP 会来回跳；差值太大（如 150/110/70）则要求 `weight` 也同步放大到 −90 以上，而 keepalived 的 `weight` 取值范围是 −253~253，遇到更多节点时很快就顶到边界。20 一档是"抗抖动 + 留扩展余量"的折中。

**三节点 VRRP 的两个额外注意点**：

- **必须三台都跑 keepalived**（缺一台不是"少一个备胎"，而是把"容忍 1 台故障"直接降级为"容忍 0 台"）；
- **`nopreempt` 的语义在 3 台下同样成立**：gw01 重启恢复后虽然 priority 回到 150（最高），也不会把 VIP 从 gw02 抢回来，避免业务抖动；只有当前持有者主动释放或失联才会再次选举。

> 补充两项生产建议：① 若交换机/云平台禁用组播（224.0.0.18），在 `vrrp_instance` 里加 `unicast_src_ip <本机IP>` + `unicast_peer { <另两台IP> }`（**三台各列另外两台**，顺序无关），否则互相收不到通告会**多主抢 VIP**；② `fall 2` 避免单次探测抖动误切。

**为什么必须错开优先级**：若三台都写 `priority 100` + `state BACKUP` + `nopreempt`，VRRP 在这种组合下**按最大 IP 胜出**——初始 VIP 会落到 `dbapi-gw03(.206)`，而全文都按"gw01 主用/引导"（Patroni 也先起 gw01）叙述，实际行为与文档不一致，排障时极易误判。改为 **150 / 120 / 100** 后，`nopreempt` 仍然保留（避免 gw01 恢复后把 VIP 抢回去造成抖动）。验证：三台同时启动后 `ip a | grep 192.168.110.199` 应只在 gw01 上出现。

---

### 3.5 节点 dbapi-gw02（192.168.110.205 ｜ 应用服务层 · 副本）

> 与 dbapi-gw01 的差异只有 5 处：etcd `--name etcd-gw02` + 两个 IP 改 `.205`；PG `name: pg-gw02` + `connect_address: 192.168.110.205:5432`；Redis 为从（`replicaof` 指向 gw01）+ 第 2 个哨兵；keepalived **`priority 120`**（gw01 为 150，gw03 为 100）。APISIX 的 `config.yaml` 与 DBAPI 的连接串**逐字一致**（这正是共享 etcd 与 VIP 的意义）。

**步骤 1：确认 §2（hostname 应返回 dbapi-gw02）**  
**步骤 2：部署 etcd 集群成员（成员2）+ APISIX**

```bash
mkdir -p /data/etcd
docker run -d --name etcd --restart unless-stopped --net host \
  -v /data/etcd:/etcd-data \
  -e GOMEMLIMIT=512MiB \
  quay.io/coreos/etcd:v3.7.2 \
  etcd --name etcd-gw02 \
  --data-dir /etcd-data \
  --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://192.168.110.205:2379 \
  --listen-peer-urls http://0.0.0.0:2380 \
  --initial-advertise-peer-urls http://192.168.110.205:2380 \
  --initial-cluster "etcd-gw01=http://192.168.110.204:2380,etcd-gw02=http://192.168.110.205:2380,etcd-gw03=http://192.168.110.206:2380" \
  --initial-cluster-state new \
  --quota-backend-bytes=2147483648 \
  --auto-compaction-mode=periodic \
  --auto-compaction-retention=1h \
  --snapshot-count=10000 \
  --heartbeat-interval=100 \
  --election-timeout=1000
```

APISIX 部署同 §3.4 步骤 2（compose 与 `config.yaml` **逐字一致**，直接拷贝 `/opt/apisix/` 目录即可——这正是共享 etcd 的意义：三台读同一份路由）。  
**步骤 3：部署 DBAPI**（同 3.4 步骤 3；`application-prod.yml` 连接串逐字一致，均指向集群地址/VIP）  
**步骤 4：部署 Patroni + PostgreSQL（从库候选）**

```bash
# 同 gw01：装 PGDG 的 PG17 + Patroni（版本与包清单逐字一致）
dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-10-x86_64/pgdg-redhat-repo-latest.noarch.rpm
dnf config-manager --enable pgdg-rhel10-extras 2>/dev/null || true
dnf install -y postgresql17-server postgresql17-contrib patroni patroni-etcd

mkdir -p /data/pg
chown -R postgres:postgres /data/pg
```

`/etc/patroni/patroni.yml`（**仅两处与 gw01 不同**：`name: pg-gw02`、`connect_address: 192.168.110.205:5432`，其余含 `synchronous_mode`、`bin_dir`、etcd3 hosts、`pg_hba` 逐字一致）：

```bash
# 从 gw01 拷贝后改两处
scp dbapi-gw01:/etc/patroni/patroni.yml /etc/patroni/patroni.yml
sed -i 's/name: pg-gw01/name: pg-gw02/; s/connect_address: 192.168.110.204:5432/connect_address: 192.168.110.205:5432/' /etc/patroni/patroni.yml
chown postgres:postgres /etc/patroni/patroni.yml && chmod 600 /etc/patroni/patroni.yml

systemctl daemon-reload && systemctl enable --now patroni

# 验证：gw02 应为 Secondary（无需 pg_basebackup，Patroni 自动从主库拉基础备份建从库）
curl -s http://127.0.0.1:8008/primary -o /dev/null -w '%{http_code}\n'   # 预期 503（本机非主）
patronictl -c /etc/patroni/patroni.yml list    # pg-gw01 Leader、pg-gw02 Sync（同步从库）、pg-gw03 Async
```

**解释**：Patroni **自动完成建从库全过程**（基础备份 + 流复制 + 注册到 DCS），不再需要手工 `pg_basebackup` 与 `pg_hba.conf` 编辑——复制账号 `replicator` 由 patroni.yml 的 `authentication` 段自动创建，`pg_hba` 由 `bootstrap.dcs.postgresql.pg_hba` 自动下发。`/data/pg` 必须为空目录（见 §7-9），**且三台的 `scope`/`namespace` 与 etcd3 hosts 必须完全一致**，否则会形成互不相认的集群。

> **谁是 sync、谁是 async 由 Patroni 自动决定**（`synchronous_mode: true` 默认只维护**一个**同步从库，`synchronous_node_count` 保持默认 1）。**不要**为了"两个都同步"把它改成 2：那样任一台从库抖动或重启都会阻塞主库写入（`synchronous_commit` 等不到两个确认），把一个"提高可靠性"的参数变成可用性事故源。第二台的 async 从库的价值不在同步复制，而在**故障转移后立刻能接任同步角色**（见 §0.1 说明）。  
> **步骤 5：部署 Redis（从）+ 哨兵**

```bash
# 从库：同样 --net host，并显式声明本机通告地址
docker run -d --name redis-replica --restart unless-stopped --net host \
  -v /data/redis:/data \
  redis:7.4 redis-server \
  --bind 0.0.0.0 --port 6379 \
  --replicaof 192.168.110.204 6379 \
  --requirepass <强口令> \
  --masterauth <强口令> \
  --maxmemory 2gb --maxmemory-policy allkeys-lru \
  --appendonly yes --appendfsync everysec \
  --replica-announce-ip 192.168.110.205

# 本机哨兵（第 2 个，quorum 仍为 2；第 3 个在 gw03，见 §3.6 步骤 5）
# v1.8 修正：哨兵配置必须落成配置文件并挂载，不能用 SENTINEL SET 运行时下发
#   后者只存在于内存，容器/主机重启后 MONITOR 丢失，哨兵变成"不知道要监控谁"的空进程
mkdir -p /data/redis-sentinel
cat > /data/redis-sentinel/sentinel.conf <<'EOF'
port 26379
bind 0.0.0.0
protected-mode no                    # 哨兵默认 protected-mode=yes，跨节点通告会被拒（3 哨兵互相发现的前提）
sentinel monitor mymaster 192.168.110.204 6379 2
sentinel auth-pass mymaster <强口令>
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 60000
sentinel announce-ip <本机IP>          # 三台各填本机：gw01=.204 / gw02=.205 / gw03=.206
EOF
chmod 666 /data/redis-sentinel/sentinel.conf    # 容器内 redis(uid 999) 要能重写它（哨兵会回写 known-replica）

docker run -d --name redis-sentinel --restart unless-stopped --net host \
  -v /data/redis-sentinel/sentinel.conf:/usr/local/etc/redis/sentinel.conf:rw,z \
  redis:7.4 redis-server /usr/local/etc/redis/sentinel.conf --sentinel
```

**解释**：`--replicaof 192.168.110.204 6379` 让本 Redis 成为 dbapi-gw01 的副本，自动同步；`--replica-announce-ip 192.168.110.205` 保证**被提升为新主时通告的是真实 IP 而不是容器内网地址**（换成 `--net host` 后实际上已无此问题，但显式写是双保险）。本机哨兵是 3 个中的第 2 个，**`quorum=2`**（第 3 个在 dbapi-gw03，见 §3.6 步骤 5）。三哨兵同在应用层、同处 `ipset:app-nodes`，不再有任何跨层放行。

**步骤 6：部署 keepalived VIP**（同 §3.4 步骤 6，但 **`priority 120`**——低于 gw01 的 150、高于 gw03 的 100，保证 VIP 让位顺序为 gw01 → gw02 → gw03；`interface` 按本机网卡，`nopreempt` 一致，故障恢复后不抢回）

---

---

### 3.6 节点 dbapi-gw03（192.168.110.206 ｜ 应用服务层 · 第三节点）

> 本节点角色：APISIX + DBAPI + **PG 第二从库（Patroni 托管）** + Redis(从) + **etcd 集群成员(3)** + **哨兵(3)** + keepalived(`priority 100`)。
>
> **v1.11 新增节点**。它**不是"为 etcd 凑票而加的机器"**——三台应用节点是三个互相咬合的最低数量：① etcd 要奇数成员；② Redis 哨兵要奇数多数派；③ **PG 要 1 主 2 从**，才能在主库宕机后立刻有一台从库接任同步复制（§0.1 已展开）。哪一项都不是"多一点余量"，而是"少于三台就不成立"。反过来说，**三台同构部署也让这三项共用同一批机器，不额外增加故障域**。
>
> 与 dbapi-gw01/gw02 的差异只有 5 处：etcd `--name etcd-gw03` + IP 改 `.206`；PG `name: pg-gw03` + `connect_address: 192.168.110.206:5432`；Redis 为从（`replicaof` 指向 gw01）+ 第 3 个哨兵；keepalived **`priority 100`**。APISIX 与 DBAPI 配置**逐字一致**。

**步骤 1：确认 §2 系统准备（hostname 应返回 dbapi-gw03）**

**步骤 2：部署 etcd 集群成员（成员3）+ APISIX**

```bash
mkdir -p /data/etcd
docker run -d --name etcd --restart unless-stopped --net host \
  -v /data/etcd:/etcd-data \
  -e GOMEMLIMIT=512MiB \
  quay.io/coreos/etcd:v3.7.2 \
  etcd --name etcd-gw03 \
  --data-dir /etcd-data \
  --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://192.168.110.206:2379 \
  --listen-peer-urls http://0.0.0.0:2380 \
  --initial-advertise-peer-urls http://192.168.110.206:2380 \
  --initial-cluster "etcd-gw01=http://192.168.110.204:2380,etcd-gw02=http://192.168.110.205:2380,etcd-gw03=http://192.168.110.206:2380" \
  --initial-cluster-state new \
  --quota-backend-bytes=2147483648 \
  --auto-compaction-mode=periodic \
  --auto-compaction-retention=1h \
  --snapshot-count=10000 \
  --heartbeat-interval=100 \
  --election-timeout=1000

# 三个成员全部起完后，在任一台验证（三台应全部 true，且只有 1 个 IS_LEADER）
docker exec etcd etcdctl endpoint status --cluster -w table
docker exec etcd etcdctl endpoint health --cluster
```

APISIX 部署同 §3.4 步骤 2（compose 与 `config.yaml` **逐字一致**，`scp -r dbapi-gw01:/opt/apisix /opt/` 拷过来即可）。

> **gw03 的 APISIX 一定要起，不能"这台只做备胎不装 APISIX"**：VI_1 的 `chk_apisix` 探的是本机 9080。若 gw03 没有 APISIX，它的 priority 会从 100 掉到 20，平时无害；但 **gw01/gw02 同时不可用时（例如同一次变更窗口、同一台宿主机故障），三台里没有任何一台能接 VIP .199，API 入口直接黑洞**——而且此时 `ip a` 看着"VIP 不在任何机器上"，很容易被误判成网络问题。三台同构是这套拓扑的硬前提。

**步骤 3：部署 DBAPI**（同 §3.4 步骤 3；`application-prod.yml` 连接串逐字一致，均指向集群地址/VIP）

**步骤 4：部署 Patroni + PostgreSQL（第二从库候选）**

```bash
# 同上，装 PGDG 的 PG17 + Patroni（版本与包清单逐字一致）
dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-10-x86_64/pgdg-redhat-repo-latest.noarch.rpm
dnf config-manager --enable pgdg-rhel10-extras 2>/dev/null || true
dnf install -y postgresql17-server postgresql17-contrib patroni patroni-etcd

mkdir -p /data/pg
chown -R postgres:postgres /data/pg
# SELinux 上下文与 gw01/gw02 一致（见 §3.4 步骤 4，漏了这句 PG 直接 permission denied）
semanage fcontext -a -t postgresql_var_lib_t "/data/pg(/.*)?" 2>/dev/null || true
restorecon -Rv /data/pg
```

```bash
# 从 gw01 拷贝后改两处（注意 /data/pg 必须为空，Patroni 要自己 initdb）
scp dbapi-gw01:/etc/patroni/patroni.yml /etc/patroni/patroni.yml
sed -i 's/name: pg-gw01/name: pg-gw03/; s/connect_address: 192.168.110.204:5432/connect_address: 192.168.110.206:5432/' /etc/patroni/patroni.yml
chown postgres:postgres /etc/patroni/patroni.yml && chmod 600 /etc/patroni/patroni.yml

systemctl daemon-reload && systemctl enable --now patroni

# 验证：应为 Async Secondary（sync 由 Patroni 指定给 gw02，此处显示 async 属正常）
curl -s http://127.0.0.1:8008/primary -o /dev/null -w '%{http_code}\n'   # 预期 503
patronictl -c /etc/patroni/patroni.yml list    # Leader 1 台 + Sync 1 台 + Async 1 台
```

**解释**：三台全部注册到同一个 `scope: dbapi-pg` 后，Patroni 会**自动选一台当同步从库**（默认 `synchronous_node_count: 1`），其余为异步从库。gw03 平时是 async，它的价值在故障转移瞬间：主库宕机 → 同步从库升主 → **gw03 立刻被挂为新的同步从库**，追平后即恢复 RPO=0；若只有两台（v1.10），此时集群里已无从库可接，只能退化为异步直到旧主修回。

> **不要**在 three-node 场景下手写 `synchronous_standby_names` 或把 `synchronous_node_count` 改成 2：前者会与 Patroni 托管冲突（Patroni 每次重写 `postgresql.conf`），后者会让**任一从库重启/抖动都阻塞主库写入**——把可靠性参数变成可用性事故。

**步骤 5：部署 Redis（从）+ 哨兵（第 3 个）**

```bash
# 从库：与 gw02 相同，改为指向 gw01 的主库，并声明本机通告地址
docker run -d --name redis-replica --restart unless-stopped --net host \
  -v /data/redis:/data \
  redis:7.4 redis-server \
  --bind 0.0.0.0 --port 6379 \
  --replicaof 192.168.110.204 6379 \
  --requirepass <强口令> \
  --masterauth <强口令> \
  --maxmemory 2gb --maxmemory-policy allkeys-lru \
  --appendonly yes --appendfsync everysec \
  --replica-announce-ip 192.168.110.206

# 本机哨兵（第 3 个，凑满 3 哨兵 / quorum=2）
mkdir -p /data/redis-sentinel
cat > /data/redis-sentinel/sentinel.conf <<'EOF'
port 26379
bind 0.0.0.0
protected-mode no                    # 跨节点通告的前提，默认 yes 会让 3 个哨兵互相发现不了
sentinel monitor mymaster 192.168.110.204 6379 2
sentinel auth-pass mymaster <强口令>
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 60000
sentinel announce-ip 192.168.110.206
EOF
chmod 666 /data/redis-sentinel/sentinel.conf

docker run -d --name redis-sentinel --restart unless-stopped --net host \
  -v /data/redis-sentinel/sentinel.conf:/usr/local/etc/redis/sentinel.conf:rw,z \
  redis:7.4 redis-server /usr/local/etc/redis/sentinel.conf --sentinel
```

**解释**：三个哨兵（gw01/gw02/gw03）**全部在应用层**，`quorum=2`、多数派授权需 2 票。任一哨兵故障后仍剩 2 个、仍能完成故障转移；v1.10 时代第 3 个哨兵跨层放在 dw03，导致**数据节点一抖就可能连带 Redis 切换能力**，且 26379 要双向跨层放行。

**步骤 6：部署 keepalived VIP**（同 §3.4 步骤 6，但 **`priority 100`**——三台中最低，VIP 让位顺序 gw01(150) → gw02(120) → gw03(100)；`interface` 按本机网卡，`nopreempt` 一致）

---


## 4. 集群一次性初始化（在 doris-dw01 执行一次）

> 以上各台起了进程，但 Doris 还没"组网"、Flink 还没提交作业、库表还没建。本章命令**只在 doris-dw01 执行一次**（DS 调度配置在 DS 集群侧单独做）。
>
> **前置**：doris-dw01 须有 MySQL 客户端（连 Doris 9030 用）：`yum install -y mysql`；`doris_ddl.sql`、`api_ddl.sql` 从设计文档导出后放到本机。

```bash
# 1) doris-dw02 作为 Follower 加入
mysql -h 192.168.110.201 -P 9030 -u root -e "ALTER SYSTEM ADD FOLLOWER '192.168.110.202:9010';"
# 2) doris-dw03 作为 Follower 加入（与 dw02 相同，参与选举；不要用 ADD OBSERVER）
mysql -h 192.168.110.201 -P 9030 -u root -e "ALTER SYSTEM ADD FOLLOWER '192.168.110.203:9010';"
# 3) 注册三台 BE
mysql -h 192.168.110.201 -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND '192.168.110.201:9050';"
mysql -h 192.168.110.201 -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND '192.168.110.202:9050';"
mysql -h 192.168.110.201 -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND '192.168.110.203:9050';"
```

**解释**：`ADD FOLLOWER` 是"接纳"——先接纳，doris-dw02/3 的 FE 才真正入列。`ADD BACKEND` 把三台 BE 注册进集群。Doris 建表默认 `replication_num=3`，**至少 3 个 BE** 才满足——这正是数据层 3 台同构的原因。9010 是 FE 通信端口，9050 是 BE 心跳端口。

> ⚠️ **dw03 必须是 FOLLOWER，不能是 OBSERVER**。FE 的多数派按"**有投票权的 FE**"计算：3 台里有 1 台 Observer，投票成员只剩 2（Leader + 1 Follower）。Leader 宕机后剩 1 个 Follower，**1 < 2（多数派）**，选不出新 Leader，FE 变成读不了也写不了的僵死状态——等于整个 Doris 不可用。Observer 只用于读扩展，**不能用来凑高可用配额**。
>
> **验证**（必须 3 行且全是 FOLLOWER/LEADER）：
>
> ```sql
> SHOW PROC '/frontends'\G
> -- 期望：Role 为 FOLLOWER / FOLLOWER / LEADER（或 MASTER），Alive=true
> -- 反例：出现 OBSERVER → 此集群不具备 FE 故障转移能力
> ```

```bash
# 4) 建库并导入 Doris DDL（设计文档 2.4 节）
mysql -h 192.168.110.201 -P 9030 -u root -e "CREATE DATABASE IF NOT EXISTS dw_db;"
mysql -h 192.168.110.201 -P 9030 -u root dw_db < doris_ddl.sql
```

```bash
# 5) 初始化 DBAPI 元数据库 —— 注意：本步在 dbapi-gw01 执行（Patroni 主库所在节点）
#    Patroni initdb 只创建 postgres 默认库，dbapi_db 不存在；
#    psql 不能对不存在的库执行 DDL，必须先连 postgres 库建库
# v1.8：Patroni 已改宿主机 systemd 安装（不再是容器），下面用 sudo -u postgres 直连
sudo -u postgres psql -d postgres -c "CREATE DATABASE dbapi_db WITH ENCODING 'UTF8' TEMPLATE template0;"
sudo -u postgres psql -d dbapi_db < api_ddl.sql

# 验证：应看到 api_def / api_param / api_field / api_app / api_auth /
#       api_call_log / api_call_log_2026_10 / api_version / api_audit
sudo -u postgres psql -d dbapi_db -c "\dt"
```

**解释**：`dw_db` 与 DBAPI 连接串里的库名对应。PG 元数据库 DDL 来自设计文档 3.4 节；Patroni 主从自动同步，**无需再对 gw02/gw03 做任何初始化**——两台从库会通过流复制拿到建库与建表结果，手工去从库重建反而会破坏复制（从库只读，写入直接报错）。

> **旧稿的坑**：直接 `psql -d dbapi_db` 会报 `FATAL: database "dbapi_db" does not exist`——因为 Patroni 的 `initdb` 只建 `postgres` 库。必须先 `CREATE DATABASE`，且建库命令要连 `postgres` 库执行。

```bash
# 6) 创建 SILO 桶 + 容量硬上限 + 生命周期（v1.12：三类用途分桶隔离）
#    SILO 镜像自带 mcli，直接在容器内执行；不再需要单独跑 minio/mc 容器
docker exec silo mcli alias set local http://127.0.0.1:9000 <user> <pass>
docker exec silo mcli mb --ignore-existing local/flink      # checkpoint + savepoint + HA 元数据
docker exec silo mcli mb --ignore-existing local/exports    # DBAPI 异步导出
docker exec silo mcli mb --ignore-existing local/backup     # Doris ADS/DWS 物理备份

# 配额：按用途隔离，任何一桶写满都不得拖垮 checkpoint
docker exec silo mcli quota set local/flink   --size 100GiB
docker exec silo mcli quota set local/exports --size 160GiB
docker exec silo mcli quota set local/backup  --size  90GiB

# 生命周期：exports 7 天自动过期(导出件是临时产物)
docker exec silo mcli ilm add --expiry-days 7 local/exports
docker exec silo mcli admin info local
```

> **配额口径（v1.12 重算）**：`flink 100 + exports 160 + backup 90 = 350GiB`，EC 可用容量 ≈450G，**留 ≈100G 余量**给纠删码元数据、临时对象与写入抖动。三桶必须都设 quota——否则 `exports` 或 `backup` 任意一方暴涨都会把 `flink` 桶挤到写不进去，表现为 checkpoint 连续失败、HA 元数据写不进、甚至 ZK 选主异常，**故障现象完全不像"导出写满了盘"**。

```bash
# 6b) checkpoint / savepoint 清理策略（v1.12 新增；RETAIN_ON_CANCELLATION 的配套义务）
# 为什么必须有：externalized-checkpoint-retention=RETAIN_ON_CANCELLATION 意味着
# 取消作业后 checkpoint 不会被自动删除，长期发布/回滚会持续累积，最终吃满 flink 桶。

# ① 看当前占用（哪个作业目录最占空间）
docker exec silo mcli du -h local/flink

# ② 已下线作业的 checkpoint 目录：确认作业不再需要后手工删除整个 job-id 目录
docker exec silo mcli rm --recursive --force local/flink/checkpoints/<job-id>

# ③ 旧版本 savepoint：只保留"当前线上版本 + 上一可回滚版本"两份，其余按发布记录清理
docker exec silo mcli ls -r local/flink/savepoints/
docker exec silo mcli rm --recursive --force local/flink/savepoints/<旧版本目录>
```

> **保留窗口规则（写进发布流程）**：① 每个作业只保留 `num-retained=3` 份自动 checkpoint；② savepoint 保留**「当前线上版 + 上一可回滚版」两份**，第三个版本发布成功且稳定运行 7 天后，删掉最老的一份；③ 作业下线后 7 天内删除其全部 checkpoint/savepoint；④ `flink` 桶用量 > 80% 触发告警、> 90% 强制清理（`docker exec silo mcli du -h local/flink` 可用 cron 每日采集）。**不做第 ④ 步，RETAIN_ON_CANCELLATION 迟早会把桶写满。**

```bash
# 6c) Doris 备份仓库 + 每周全量（v1.12 新增；此前 backup 桶只有配额、没有作业）
#     备份产物通过 SILO VIP .207 写入，dw01 故障不影响备份链路
mysql -h 192.168.110.201 -P 9030 -u root

CREATE REPOSITORY `silo_backup`
WITH S3 ON LOCATION "s3://backup/dw_backup/"
PROPERTIES (
  "s3.endpoint"    = "http://192.168.110.207:9000",
  "s3.region"      = "us-east-1",          -- SILO 不校验 region，但 Doris 要求必填
  "s3.access_key"  = "<SILO 用户名>",
  "s3.secret_key"  = "<SILO 强口令>",
  "use_path_style" = "true"                -- 必填，否则 Doris 会拼成 bucket.域名 形式
);

-- 只备 ADS / DWS。ODS/DWD 走"可重跑"，不做物理备份（详细设计 §5.2）
BACKUP SNAPSHOT dw_db.snap_20261025_ads
TO `silo_backup`
ON (ads_order_daily, ads_shop_summary, dws_trade_shop_day)
PROPERTIES ("type" = "full");

SHOW BACKUP FROM dw_db;   -- SnapshotName 与 State 列：须为 FINISHED

# 6d) 容量核对 + 保留 3 份（配额 90GiB，单份 ≈25G → 3 份 ≈75G）
docker exec silo mcli du -sh local/backup
mysql -h 192.168.110.201 -P 9030 -u root -e "SHOW SNAPSHOT ON silo_backup;"

# 第 4 份产生前删除最老一份（语法随 FE 版本略有差异，以 HELP DROP SNAPSHOT; 为准）
mysql -h 192.168.110.201 -P 9030 -u root -e \
  "DROP SNAPSHOT snap_20261004_ads ON (ads_order_daily, ads_shop_summary, dws_trade_shop_day) FROM silo_backup;"
```

```bash
# 6e) 恢复验证（不做这一步，等于没有备份）
#     恢复到临时库名，验证完立刻删库，不要覆盖生产表
mysql -h 192.168.110.201 -P 9030 -u root <<'SQL'
RESTORE SNAPSHOT dw_db.snap_20261025_ads
FROM `silo_backup`
ON (ads_order_daily)
PROPERTIES ("backup_timestamp" = "<从 SHOW SNAPSHOT 复制的 Timestamp>", "replication_num" = "3");
SQL
mysql -h 192.168.110.201 -P 9030 -u root -e "SHOW RESTORE FROM dw_db;"   -- State 须到 FINISHED

# 抽查行数与生产表一致（备份时间点附近允许差异，量级必须对得上）
mysql -h 192.168.110.201 -P 9030 -u root -e \
  "SELECT count(*) FROM dw_db.ads_order_daily; SELECT count(*) FROM dw_db.ads_order_daily_restore;"
```

> **备份口径（v1.12 定稿，与详细设计 §5.2、技术方案 R12/R16 一致）**：ADS/DWS 单次全量约 **25GB**，**每周全量、保留 3 份 ≈75GB**，在 `backup` 90GiB 配额内。**旧稿"每日全量、保留 14 天"是错的**——25×14=350GB，远超配额，且会把 `flink`/`exports` 一起挤爆（表现为 checkpoint 连续失败，完全看不出根因在备份）。
>
> **三条边界**：① `backup` 桶**不设 ILM 过期**（它是唯一副本，`ilm add` 只对 `exports` 用）；② **14 天以上的长期保留必须外挂 NAS / 独立对象存储 ≥1TB**，SILO 只做"最近三周误删能回滚"的短期恢复仓；③ ODS/DWD 不做物理备份，恢复路径 = 重跑 CDC 全量 + DS 加工，RTO 数小时~1 天，这条须在上线评审里明确写进 SLA。

```bash
# 7) 创建 Kafka 入湖 topic（topic 名与 ODS 表同名：ods_trade_order）
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 192.168.110.201:9092 \
  --create --if-not-exists --topic ods_trade_order --partitions 3 --replication-factor 3
# 验证
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 192.168.110.201:9092 --describe --topic ods_trade_order
```

**解释**：步骤 6 的 `flink` 桶必须存在，否则 Flink 写 checkpoint 失败、HA 无法选主（SILO 镜像**内置 `mcli`**，直接在容器内执行即可；不要再用已下架的 `minio/mc` 镜像）。**三个桶都设配额（v1.12）**：`flink` 100GiB（3 份 checkpoint + savepoint 累积）、`exports` 160GiB + 7 天生命周期（否则 DBAPI 导出的 CSV 永久堆积）、`backup` 90GiB；合计 350GiB，EC 可用容量（6 drive 默认 EC:3 → ≈450G）留 ≈100G 余量。**分桶的意义不只是"看得清"，而是让 `exports`/`backup` 的暴涨无法把 `flink` 桶挤爆**——checkpoint 写不进去时，现象是 HA 选主异常、作业连续重启，完全看不出根因在导出。

> **命名必须统一**：**topic 名 = ODS 表名 = `ods_trade_order`**。旧稿 topic 写 `ods_orders`、表写 `ods_trade_order`，消费端对不上——这类"名字不一致"不会报错，只会让你在 Routine Load 里看到一个永远没有数据的作业。步骤 7 的副本因子 3 与 3 broker 匹配，保证入湖缓冲无单点。

```bash
# 8) 提交 Flink CDC 作业（实时入湖：业务库 → Kafka）
cat > /data/flink/cdc_orders.sql <<'EOF'
-- CDC 靠 checkpoint 记录 binlog 位点，必须开启
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
-- 提交后与客户端解绑：否则关掉 sql-client 终端会把作业一起取消
SET 'execution.attached' = 'false';

CREATE TABLE mysql_orders (
  order_id     BIGINT,
  order_no     STRING,
  user_id      BIGINT,
  shop_id      BIGINT,
  order_amt    DECIMAL(20,2),
  order_status INT,
  create_time  TIMESTAMP(3),
  op           STRING METADATA FROM 'op' VIRTUAL,   -- binlog 操作类型：I/U/D
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector'='mysql-cdc','hostname'='<源库IP>','port'='3306',
  'username'='<cdc只读用户>','password'='<密码>',
  'database-name'='<源库名>','table-name'='orders',
  'server-time-zone'='Asia/Shanghai',
  'scan.incremental.snapshot.enabled'='true'
);

CREATE TABLE kafka_orders (
  order_id     BIGINT,
  order_no     STRING,
  user_id      BIGINT,
  shop_id      BIGINT,
  order_amt    DECIMAL(20,2),
  order_status INT,
  create_time  TIMESTAMP(3),
  op           STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector'='upsert-kafka',
  'topic'='ods_trade_order',
  'properties.bootstrap.servers'='192.168.110.201:9092,192.168.110.202:9092,192.168.110.203:9092',
  'key.format'='json','value.format'='json',
  'value.json.fail-on-missing-field'='false',
  -- 可靠性：默认 acks=1，broker 抖动就可能丢消息；CDC 入湖必须 acks=all + 幂等
  'properties.acks'='all',
  'properties.enable.idempotence'='true',
  'properties.transaction.timeout.ms'='900000',
  'properties.max.in.flight.requests.per.connection'='5',
  'sink.parallelism'='3'
);

INSERT INTO kafka_orders
SELECT order_id, order_no, user_id, shop_id, order_amt, order_status, create_time,
       -- MySQL CDC 的 op metadata 取值是 '+I' / '+U' / '-U' / '-D'，
       -- 这里归一化成 I / U，与 ODS 表 op_type 的取值域、DS 对账脚本保持一致
       CASE WHEN op = '+I' THEN 'I' ELSE 'U' END AS op
FROM mysql_orders;
EOF

/data/flink/bin/sql-client.sh -f /data/flink/cdc_orders.sql
```

**解释**：Flink SQL Client 不支持 `-e "SQL"` 直传语句，须**写成 `.sql` 脚本用 `-f` 提交**。

- `execution.checkpointing.interval=30s` 必须有——`mysql-cdc` 靠 checkpoint 记录 binlog 位点，不开 checkpoint 作业无法推进。30s 是与磁盘/共享盘权衡后的值（见 §3.1 步骤 9），故障最多重放 30s。
- **全量阶段的内存与 state 风险（v1.10 新增，最容易在"同步大表"时炸）**：`taskmanager.memory.process.size=4096m` 扣掉 JVM/network 后，`managed.fraction=0.4` 大约只给 RocksDB **1.3~1.5G**。**CDC 全量快照阶段会把已读分片缓存在 state 里**，千万行级的表会直接把这块吃满 → checkpoint 反复失败 → 作业不断重启 → 全量永远跑不完（与 §7-38 的 PAUSED 是两种不同的"全量卡住"，不要混）。
  - 前置动作：`scan.incremental.snapshot.chunk.size` 调到 **8096**（默认 8096，大宽表降到 2048~4096），控制单批快照内存；
  - 首次全量期间，把 3 台 TM 的 `taskmanager.memory.process.size` **临时提到 8G**、`parallelism.default` 提到 6，跑完再降回（数据节点内存账本余量约 19G，够临时用）；
  - 表特别大（>5000 万行）时**按表拆成多个作业**分别提交，不要一个作业同步几十张表 —— 单作业 state 是共享的，一张大表会把所有表的 checkpoint 一起拖垮。
- `execution.attached=false`：**流式 INSERT 默认 attached**，客户端退出（关终端）时会把作业一起取消，表现为"作业跑了一会儿就没了"。设为 false 后提交即解绑，作业在 HA 集群上独立运行。
- **列必须与 ODS 表一一对应**：`order_id, order_no, user_id, shop_id, order_amt, order_status, create_time, op` 与设计文档 §2.4.1 的 DDL 顺序一致（`stat_date`/`etl_time` 由 Routine Load 侧计算，不从 Kafka 取）。旧稿字段是 `id/cust_id/amount/ts`，与 ODS 表名/列全对不上，是"跑得起来但入不了仓"的典型。
- `op STRING METADATA FROM 'op' VIRTUAL` 把 binlog 事件类型带进消息体，Routine Load 的 `jsonpaths` 用 `$.op` 映射到 `op_type`。**注意取值不是 `I/U` 而是 `+I/+U/-U/-D`**（Flink MySQL CDC 的约定），所以 INSERT 里用 `CASE` 归一化成 `I/U` 再写出——否则 ODS 的 `op_type` 会全是 `+I/+U`，DS 对账脚本按 `op_type='I'` 过滤会一条都匹配不上（v1.7 修正）。
- **`properties.acks=all` + `enable.idempotence=true` 必加**（v1.7 新增）：Flink Kafka sink 默认 `acks=1`，broker 抖动或切换时存在丢消息窗口；CDC 是唯一数据源，丢了就是永久缺口。幂等生产者要求 `acks=all` + `max.in.flight≤5`，三条一起写才有意义。
- 该作业运行在 Flink HA 集群上——Leader JM 宕机由 ZK 重选，从 SILO checkpoint 恢复并续读 binlog，入湖不中断。CDC 连接器 jar 需提前放入 `/data/flink/lib/`（`flink-sql-connector-mysql-cdc-*.jar`）。

> ⚠️ **DELETE 的语义边界（必须知道）**：`upsert-kafka` 对 `-D`（删除）写的是 **tombstone（value=null）**，Doris Routine Load 消费到空消息会**直接跳过**。所以**源库删除在 ODS 里不留痕迹**，DWD 里那条数据会一直存在、报表长期偏大。本方案的处理：**ODS 只承接 I/U**（`op_type` 记录 I/U）；删除由 DS 每日"删除对账"在 DWD 层识别（抽源库当日主键快照 → 与 DWD 当日分区比对 → 源库不存在的主键置 `is_deleted=1`），DWS/ADS 汇总统一 `WHERE is_deleted=0`。详见设计文档 §2.7.2。若业务要秒级删除，需自研 `KafkaSerializationSchema` 输出带 `op":"D"` 的普通消息（不用 tombstone），代价是自研代码。

```bash
# 9) 在 Doris 建 Routine Load，消费 Kafka → ODS（完整可执行，列映射与 jsonpaths 一一对应）
cat > /data/rl_ods_trade_order.sql <<'EOF'
CREATE ROUTINE LOAD dw_db.rl_ods_trade_order ON ods_trade_order
COLUMNS(
    order_id, order_no, user_id, shop_id, order_amt, order_status, create_time, op_type,
    stat_date = curdate(),          -- ★ v1.10：必须是入湖日期，不能用 create_time（否则全量同步历史订单必报 Partition not found）
    etl_time  = now()
)
PROPERTIES (
    "desired_concurrent_number" = "3",
    "max_batch_interval"        = "10",
    "max_batch_rows"            = "200000",
    "max_batch_size"            = "104857600",
    "max_error_number"          = "1000",
    "strict_mode"               = "false",
    "format"                    = "json",
    "jsonpaths" = "[\"$.order_id\",\"$.order_no\",\"$.user_id\",\"$.shop_id\",\"$.order_amt\",\"$.order_status\",\"$.create_time\",\"$.op\"]"
)
FROM KAFKA (
    "kafka_broker_list" = "192.168.110.201:9092,192.168.110.202:9092,192.168.110.203:9092",
    "kafka_topic"       = "ods_trade_order",
    "property.group.id" = "doris_rl_ods_trade_order",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
EOF

mysql -h 192.168.110.201 -P 9030 -u root < /data/rl_ods_trade_order.sql

# 验证：State=RUNNING、ErrorRows=0、Lag 稳定在个位数/几十
mysql -h 192.168.110.201 -P 9030 -u root -e "SHOW ROUTINE LOAD FOR dw_db.rl_ods_trade_order\G"
```

**解释**：Routine Load 是 Doris 持续消费 Kafka 的导入作业，把 `ods_trade_order` 落进 ODS 层。它与 Flink CDC 形成"双跳解耦"：Flink 只负责抓取 + 写 Kafka，Doris 负责消费入仓，任一侧抖动互不影响。

**旧稿这一步是跑不通的，四个问题都在本版修掉**：

| 问题                   | 后果                                       | 本版处理                               |
| -------------------- | ---------------------------------------- | ---------------------------------- |
| `COLUMNS(...)` 是省略占位 | 直接执行是语法错误，等于这步没写完                        | 给出 8 个字段 + 2 个计算字段的完整映射            |
| `ON ods_orders`      | Doris 里没有这张表（表名是 `ods_trade_order`）      | 目标表改为 `ods_trade_order`            |
| topic 名 `ods_orders` | 与 Flink 写入的 topic 对不上，作业 RUNNING 但永远 0 行 | 统一为 `ods_trade_order`              |
| 缺 `jsonpaths`        | 不指定字段映射，JSON 字段顺序一变就整体错位且**不报错**（静默写脏）   | 显式给出 8 条 jsonpath，与 Flink 输出字段顺序一致 |

补充两个易错点：`max_batch_interval` 单位是**秒**，写 `"10s"` 会报错；`stat_date`/`etl_time` 用计算列生成，不要从 Kafka 取（Flink 侧没这两个字段）。若 `ErrorRows` 持续增长，先看 `ReasonOfStateChanged` 与 `ErrorLogUrls`，绝大多数是 `jsonpaths` 与实际消息字段不匹配。

> **⚠️ `stat_date` 为什么必须是 `curdate()` 而不是 `date_format(create_time,...)`（v1.10，这是会卡死首次上线的那一条）**
>
> 表是 `PARTITION BY RANGE(stat_date)` 且 `dynamic_partition.start = -30`，即 Doris 只保证存在"最近 30 天 + 未来 3 天"的分区。若分区值取业务时间：
>
> - Flink CDC 首次全量会把源库里**一年前的订单**也发出来；
> - 这些行的 `stat_date` 落在 30 天以前 → **那个分区不存在，也不会被自动创建**；
> - Routine Load 逐条报 `Partition not found`，`ErrorRows` 几秒内突破 `max_error_number=1000` → **作业转 PAUSED**；
> - 表现是"作业建好了、跑了几秒就暂停、Lag 一直不动、全量永远完成不了"。
>
> 改成 `curdate()` 后，任何历史数据都落进今天的入湖分区，"保留 30 天"也回归成它本来该有的含义：**最近 30 天入湖的数据**（重跑窗口）。业务日期留在 `create_time` 列，由 DWD 加工时取用，并在那边加 `date(create_time) >= curdate()-90` 过滤（设计文档 §2.7.1）。
>
> **`strict_mode=false` 与 `ErrorRows=0` 的关系（别被它骗了）**：非严格模式下，字段类型转换失败**不进 ErrorRows**，会静默写成 `NULL` 继续导入。所以 `ErrorRows=0` 只说明 JSON 能解析、字段名对得上，**不说明数据干净**。真正的巡检是每日跑空值率：
>
> ```sql
> SELECT count(*) total, sum(order_id IS NULL) null_id, sum(order_amt IS NULL) null_amt
> FROM dw_db.ods_trade_order WHERE stat_date = curdate() - 1;
> ```

> ⚠️ **`OFFSET_BEGINNING` 只在首次初始化时用**（v1.7 补充）：ODS 是 **Duplicate 模型**，同一批消息被消费两次就是两行数据。首次建作业时 Kafka 里通常没有历史（或正好需要回灌 48h），用 `OFFSET_BEGINNING` 合理；**重建/重提 Routine Load 时必须改成 `OFFSET_END`**，或先用 `ALTER ROUTINE LOAD ... FROM KAFKA ("kafka_partitions"="0,1,2","kafka_offsets"="OFFSET_END")` 跳过存量，否则会整段重灌、报表翻倍。判断是否重复：`SELECT order_id, count(*) FROM ods_trade_order GROUP BY order_id HAVING count(*)>1`。

---

```bash
# 10) 安全加固：root 强口令 + 最小权限账号（v1.12 重写）
# ⚠️ 这是"初始化窗口的最后一分钟"，不是可延后的收尾项：
#    Doris 默认 root 空口令，一旦 9030 对 app-nodes/ds-nodes 放行，
#    任何能连 9030 的主机都能删库。窗口应控制在 1 小时内，且期间不对办公网开放。

# 10.1 root 设强口令（执行后，本手册后续所有 -u root 命令都要加 -p"${DORIS_PW}"）
export DORIS_PW='<强口令>'
mysql -h 192.168.110.201 -P 9030 -u root -e "SET PASSWORD FOR 'root' = PASSWORD('${DORIS_PW}');"

# 10.2 业务只读账号：DBAPI 查 ADS 用，禁止 DBAPI 使用 root
mysql -h 192.168.110.201 -P 9030 -u root -p"${DORIS_PW}" <<'EOF'
CREATE USER IF NOT EXISTS 'dbapi_ro' IDENTIFIED BY '<dbapi_ro强口令>';
GRANT SELECT_PRIV ON dw_db.* TO 'dbapi_ro'@'%';
EOF

# 10.3 DS 加工账号：ODS→DWD→DWS→ADS 需要读+写+建表改表，但不给节点管理权限
mysql -h 192.168.110.201 -P 9030 -u root -p"${DORIS_PW}" <<'EOF'
CREATE USER IF NOT EXISTS 'ds_rw' IDENTIFIED BY '<ds_rw强口令>';
GRANT SELECT_PRIV, LOAD_PRIV, ALTER_PRIV, CREATE_PRIV, DROP_PRIV ON dw_db.* TO 'ds_rw'@'%';
EOF

# 10.4 校验三个账号可用
mysql -h 192.168.110.201 -P 9030 -u dbapi_ro -p'<dbapi_ro强口令>' -e "SHOW TABLES FROM dw_db;"
mysql -h 192.168.110.201 -P 9030 -u ds_rw    -p'<ds_rw强口令>'    -e "SHOW TABLES FROM dw_db;"
```

> **账号分工（v1.12 新增，P1-15）**：**root 只用于 DBA 运维**（建库建表、`ALTER SYSTEM ADD BACKEND`、创建 Routine Load），**不进入任何应用配置文件**。`dbapi_ro` 只读（DBAPI 只查 ADS）、`ds_rw` 限 `dw_db` 库读写且**不含 `NODE_PRIV`/`ADMIN_PRIV`**（不能加删节点、不能改集群配置）。权限按业务再收紧时，把 `dw_db.*` 收到具体库表。
>
> **为什么原来写"放到最后"**：全文命令都用 `-u root` 无口令执行，先设口令会让后面每一步都要改。这是**妥协不是正确姿势**——所以必须保证窗口极短，且**在设口令之前不要开放防火墙的 9030**。若现场无法一次做完，宁可先建账号再继续其余初始化。
>
> 设口令后：本文所有 `mysql -h ... -u root` 命令都要加 `-p"${DORIS_PW}"`；**已创建的 Routine Load 不受影响**（作业由 FE 内部调度，不以 root 身份重新鉴权）。

> **多 FE 连接口径（v1.12 新增，P1-16）**：初始化阶段用单 FE `.201:9030` 可以接受，**生产配置必须避开单 FE**——
>
> - **JDBC（DBAPI / DS）**：用 loadbalance 写法把三台 FE 都写上，避免 FE Leader 切换后连不上：`jdbc:mysql:loadbalance://192.168.110.201:9030,192.168.110.202:9030,192.168.110.203:9030/dw_db?user=dbapi_ro&password=<口令>`。
> - **Stream Load**：URL 用**任一 BE 的 8040**，不要在代码里写死某一台 BE。
> - **DataX doriswriter**：`jdbcUrl` 同样写多 FE；`loadUrl` 按其版本文档支持的形式配置多 BE，不要只写单台。
> - **原理**：9030 是 FE 的 MySQL 协议端口，任意 Follower 都能接查询并转发给 Leader。客户端配了多 FE 时单台 FE 宕机不影响可用性；**只写 `.201` 的配置在 Leader 切换后会直接连不上**，这是最容易被忽略的单点。

> **SILO TLS 落地要求（v1.12 新增，P1-06）**：测试环境用 `http://` 可以，**生产必须 HTTPS**，而且"把 scheme 改成 https"远不算完成。切生产前逐项确认：
>
> | 检查项 | 要求 |
> |---|---|
> | CA 与 server 证书 | CA 证书固定路径存放（如 `/data/store/silo/certs/ca.crt`）；server 证书的 **SAN 必须覆盖 Flink 与 DBAPI 实际访问的地址/域名**（用 IP 访问就得把 IP 写进 SAN） |
> | 客户端信任 | Flink 侧把 CA 导入 JVM truststore（或在 S3 配置里指定信任方式）；DBAPI/DS 侧同样导入，**不要用"跳过校验"** |
> | 证书权限 | 私钥 `chmod 600`、属主为容器运行用户；证书目录 `chmod 700` |
> | 到期监控 | 到期前 **30 天告警、7 天强制更换**；`openssl x509 -enddate -noout -in <cert>` 纳入日常巡检 |
> | 更新流程 | 先在**非 Leader** 节点换证并验证 → 滚动重启其余节点 → 复核 `mcli admin info` 与 checkpoint 可写 |
> | 回滚 | 保留旧证书与旧 endpoint 配置各一份，换证失败能在 15 分钟内退回 HTTP |
>
> ⚠️ 换证后**必须重跑一次"checkpoint 可写 + HA 元数据可读"验证**：TLS 握手失败时 Flink 只会报 `Checkpoint failed`，**不会告诉你根因是证书**，极易被误判成磁盘/SILO 故障（对应 §7-53）。

---


## 5. 验证清单

| 检查项                       | 命令 / 方法                                                                                                              | 期望                                                                                                      | 不通过时先查                                                                           |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| FE 集群                     | `SHOW PROC '/frontends';`（数据层）                                                                                       | **3 行，Role = 2×FOLLOWER + 1×LEADER**，Alive=true                                                         | 出现 `OBSERVER` → 不具备 FE 故障转移，见 §4                                                 |
| BE 集群                     | `SHOW PROC '/backends';`                                                                                             | 3 台 SystemAlive=true，TabletNum 均衡                                                                       | ADD BACKEND 是否漏、9050 是否放行                                                        |
| **BE 内存**                 | `SHOW BACKEND CONFIG LIKE 'mem_limit';` + `ps -o rss= -p $(pgrep doris_be)`                                          | Value=**40%**；RSS 稳定 **20~25G，不超 27G**                                                                  | 仍是 90% → `be.conf` 未生效或被默认值覆盖                                                    |
| Kafka 集群                  | `docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server 192.168.110.201:9092`              | 3 broker 可达                                                                                             | 环境变量用了 `KAFKA_CFG_*`（官方镜像不认）、`CLUSTER_ID` 三台不一致                                  |
| ZooKeeper 集群              | ZK 四字命令 `srvr`（`echo srvr \| nc 192.168.110.201 2181`，三台各查）                                                          | 1 台 `Mode: leader`、2 台 `Mode: follower`                                                                 | `ZOO_MY_ID` 重复或未设、2888/3888 未放行                                                  |
| **SILO 纠删码**              | `docker logs silo 2>&1 \| grep -iE "erasure\|drive"`；`docker exec silo mcli admin info local`                        | **6 drives**，有 EC 集合（默认 EC:3）；可用容量 ≈裸盘 **1/2**（不是 2/3）                                                  | 仍是 3 drive → 未按 §3.1 步骤 7 划 d1/d2；只有 2 drive → 没用 `--net host`（容器连不上宿主 drive 地址） |
| **容器网络**                  | `docker inspect silo apisix --format '{{.Name}} {{.HostConfig.NetworkMode}}'`                                        | 两者均为 **host**                                                                                           | 显示 bridge → 会撞 §7-28（防火墙拦 docker0 网关）                                            |
| **docker 网段**（v1.11）      | `ip -4 addr show docker0 \| grep inet`；`docker network ls`                                                           | docker0 = **`172.31.0.1/24`**；无任何 `172.17.x` 网络                                                         | 仍是 `172.17.0.1/16` → `daemon.json` 没生效或 docker 未重启（§2.8）                         |
| **op_type 取值**            | `SELECT op_type, count(*) FROM dw_db.ods_trade_order GROUP BY op_type`                                               | 只有 `I` 与 `U`                                                                                            | 出现 `+I`/`+U` → Flink INSERT 缺 `CASE` 归一化（§4 步骤 8）                                |
| **分区落点**（v1.10）           | `SHOW PARTITIONS FROM dw_db.ods_trade_order`                                                                         | 最新分区 = **今天**（`p<今天日期>`）                                                                                | 数据落在 30 天前的分区名上 → `stat_date` 还在用 `date_format(create_time)`（§7-38）              |
| **空值率**（v1.10）            | `SELECT count(*) t, sum(order_id IS NULL) n FROM dw_db.ods_trade_order WHERE stat_date=curdate()-1`                  | `n/t < 0.1%`                                                                                            | 明显偏高 → 字段映射错位或 `strict_mode=false` 静默写 NULL（§7-40）                               |
| **故障自愈水位**（v1.10）         | `SHOW DATA` 取单副本总量；`df -h /data/doris`                                                                               | 单副本 < **768G**，且每节点用量 < **53%**                                                                         | 已超 → 单盘故障后副本补不回来，见设计文档 §2.8                                                      |
| **数据盘挂载**（v1.11 补充）       | 三台各执行 `findmnt /data/doris /data/store`；`df -hT /data/doris /data/store`                                             | 两个独立挂载点，`SOURCE` 为 `sdb1`/`sdc1`，容量各 **≈466G**、选项含 `noatime`                                            | 显示 200G 或 `sda*` → 挂载没生效，数据写在系统盘（§2.4.10 第 1 行）                                  |
| **Flink 拓扑**（v1.12）       | 三台各 `cat /data/flink/conf/masters`；`ssh <host> jps \| grep StandaloneSession`                                          | 三台 `masters`/`workers` **逐字一致**；三台上各 1 个 `StandaloneSessionClusterEntrypoint`                            | 只有一台有 JM → 用了方式 B 却没逐台起；masters 某台漏建（§3.1 步骤 9）                            |
| Flink HA                  | `curl -s 192.168.110.201:8081/overview`（三台）                                                                          | 返回 JSON；Leader JM 的 UI 显示 TaskManagers=3                                                                | `taskmanager.host` 未写、6123 未放行                                                   |
| **Flink `high-availability.jobmanager.port`**（v1.12） | 三台 `grep jobmanager.port /data/flink/conf/flink-conf.yaml`；`ss -lntp \| grep 6123`                                  | 三台都是 **6123**，且每个 JM 进程确实在监听 6123                                                              | 未配置 → JM 用**随机端口**，防火墙拦掉后 TM 切换后连不回来（§7-52）                            |
| **JM 故障切换演练**（v1.12，P0）   | ① `docker exec zk zkCli.sh -server 192.168.110.201:2181 get /flink/dbapi-flink/leader/rest_server_lock` 查当前 Leader；② `ssh <Leader主机> /data/flink/bin/jobmanager.sh stop`；③ 30s 后再查该 znode | Leader 迁到另两台之一；**全局仅 1 个 Leader**；作业自动恢复为 RUNNING；WebUI Checkpoints 页出现新的 Completed | 30s 内无新 Leader → `high-availability.type` 未生效或 ZK 三台未选出 leader；作业卡 RESTARTING → TM 连不上新 Leader（查 6123） |
| **TM 故障演练**（v1.12，P0）      | `ssh doris-dw02 /data/flink/bin/taskmanager.sh stop` → 查 Leader 的 `/overview`                                             | 30s 内 `taskmanagers` 3→2；作业自动重分配到剩余 2 台；TM 重启后重新注册 → 3                                                  | 作业挂死 → `taskmanager.host` 写错导致注册不上，或 slot 不足（2 台 × 4 slots 仍应容纳并行度 4）        |
| **SILO 单节点故障演练**（v1.12，P0） | `ssh doris-dw01 docker stop silo` → 观察 leader WebUI 的 checkpoint 状态 1~2 个周期 | checkpoint 最多失败 **1~2 次**（VIP 漂移窗口），随后**自动恢复成功**；作业保持 RUNNING；`echo srvr` 之外再确认 `curl http://192.168.110.207:9000/minio/health/live` 已切到 dw02/dw03 | VIP 没漂 → 数据层 keepalived 未装/未起、VRRP 112 未放行（§6.3）；checkpoint 持续失败 → `s3.endpoint` 仍写单节点 `.201` |
| **SILO VIP 归属**（v1.12） | 三台 `ip a \| grep 192.168.110.207`；`arping -c 3 -I eth0 192.168.110.207` | 同一时刻**只在一台**出现；`curl -sf http://192.168.110.207:9000/minio/health/live` 返回 200 | 三台都有 → `virtual_router_id` 与传统/组播问题形成多主；一台都没有 → `weight -80` 写错或 healthcheck 恒失败（`docker exec silo silo healthcheck ready` 手测） |
| **备份可恢复**（v1.12，P0） | `SHOW BACKUP FROM dw_db`；`SHOW SNAPSHOT ON silo_backup`；按 §4 步骤 6e 做一次 `RESTORE SNAPSHOT` | State=FINISHED；`docker exec silo mcli du -sh local/backup` **≤ 3 份且 < 90GiB**；恢复出的表行数与生产量级一致 | 只有配额没作业 → 没执行 §4 步骤 6c；`du` 超 90G → 未按 3 份滚动删除（§4 步骤 6d） |
| Flink 作业                  | Leader JM WebUI → Running Jobs                                                                                       | `mysql_orders→kafka_orders` 作业 **RUNNING 且不因关终端消失**                                                     | 缺 `execution.attached=false`                                                     |
| **Routine Load**          | `SHOW ROUTINE LOAD FOR dw_db.rl_ods_trade_order\G`                                                                   | State=RUNNING、ErrorRows=0、Lag 个位数~几十                                                                    | jsonpaths 与消息字段不匹配、topic 名不一致                                                    |
| 端到端实时                     | 源库改一行 → 数秒后查 ODS 表                                                                                                   | 变更已落到 Doris ODS                                                                                         | Kafka topic 无数据 → Flink 作业侧                                                      |
| etcd 集群                   | `docker exec etcd etcdctl endpoint status --cluster -w table`（应用层任一台执行）                                              | 3 endpoint 全 true，1 个 IS_LEADER                                                                         | 2380 未在 `ipset:app-nodes` 内放行、三台 `--initial-cluster` 不一致                         |
| **etcd 成员分布**（v1.11）      | `docker exec etcd etcdctl member list`                                                                               | 恰好 3 个：`etcd-gw01/.204`、`etcd-gw02/.205`、`etcd-gw03/.206`，**不含任何 `doris-dw*`**                          | 出现 `etcd-dw01` → 数据节点上残留 v1.10 的 etcd 容器，必须 `docker rm -f` 并清 `/data/store/etcd` |
| **DS 白名单粒度**（v1.9）        | `firewall-cmd --permanent --ipset=ds-nodes --get-entries`（数据层三台）                                                     | 恰好 3 条：`192.168.110.131`、`.132`、`.133`                                                                  | 不足 3 条或含占位 → §6.1 未正确执行；DS 连不上 9030 先查这里（§7-37）                                  |
| **无网段级放行**（v1.9）          | `firewall-cmd --zone=datanode --list-all \| grep 192.168.110.0/24`                                                   | **无输出**                                                                                                 | 有输出说明某条规则按网段放行，隔离已失效（§7-36）                                                      |
| PG（Patroni）               | `patronictl -c /etc/patroni/patroni.yml list`                                                                        | **3 行**：pg-gw01 Leader、pg-gw02 Sync、pg-gw03 Async                                                       | etcd 不健康、`/data/pg` 非空、`bin_dir` 未写；只有 2 行 → gw03 的 patroni 没起来                  |
| PG 主从                     | gw02/gw03 上 `SELECT pg_is_in_recovery();`                                                                            | 均返回 `t`（从库）；gw01 返回 `f`                                                                                 | 同步复制未建立（Sync State 为空）                                                           |
| **PG 复制连接数**（v1.11）       | 主库上 `SELECT count(*) FROM pg_stat_replication;`                                                                      | **2**（1 sync + 1 async）                                                                                 | 只有 1 → gw03 未加入复制；`pg_hba` 漏了 `.206` 的 replication 行                             |
| **故障转移演练**                | dbapi-gw01 上 `systemctl stop patroni` → 60s 内任一台查 `patronictl -c /etc/patroni/patroni.yml list`                      | gw02 变 Leader、VIP .200 已漂移、**gw03 从 Async 变 Sync**；DBAPI 查询不中断。演练后 `systemctl start patroni`（旧主自动降级为从库） | `use_pg_rewind`、VI_2 探针 8008。**若 gw03 没接上 Sync，说明 3 台没起到 3 台的作用**（§3.5 说明）       |
| **Doris file cache**      | `SHOW BACKEND CONFIG LIKE 'enable_file_cache';` + `du -sh /data/doris/be/storage`                                    | Value=**false**；三天内不异常增长                                                                                | 4.1.5 默认 true，`be.conf` 未显式关                                                     |
| Redis 主从                  | `docker exec -i redis-sentinel redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster`                         | 返回当前 master（正常 192.168.110.204:6379，**是真实 IP 不是 172.17.x.x**）                                           | 未用 `--net host` → 通告容器内网 IP                                                      |
| **Redis 哨兵数量**            | `docker exec -i redis-sentinel redis-cli -p 26379 SENTINEL master mymaster \| grep -E "num-other-sentinels\|quorum"` | `num-other-sentinels=2`、`quorum=2`                                                                      | 只有 2 哨兵 → 永远无法切换（假高可用）                                                           |
| **APISIX 读配置**            | `docker logs apisix 2>&1 \| grep -i etcd`；`curl -H 'X-API-KEY:<key>' 127.0.0.1:9180/apisix/admin/routes`             | 无 etcd 连接错误；返回 200                                                                                      | 写了 `endpoints` 而非 `host`                                                         |
| **keepalived 归属**         | `ip a \| grep -E '192.168.110.199\|192.168.110.200'`（**三台各查**）                                                       | **两个 VIP 都在 gw01(.204)**，gw02/gw03 无                                                                    | priority 相同 → 按大 IP 落到 gw03；priority 没写成 150/120/100                             |
| **keepalived 让位链**（v1.11） | gw01 上 `systemctl stop keepalived` → 查三台 `ip a`                                                                      | VIP 先到 gw02（120）；再停 gw02 的 keepalived → 到 gw03（100）                                                     | 停在原地 → `weight` 未满足"降级后 < 100"（§3.4 步骤 6）                                        |
| 端到端查询                     | 外部调用方 → VIP `192.168.110.199:9080` → APISIX → DBAPI → Doris                                                          | 签名调用返回 code=0                                                                                           | 路由/上游/鉴权插件                                                                       |
| 限流                        | 超出 qps_quota 持续请求                                                                                                    | 返回 42901                                                                                                | limit-req 插件配置                                                                   |
| 缓存                        | 同参数重复调用                                                                                                              | 第二次延迟显著下降                                                                                               | Redis 连接、sentinel 地址                                                             |
| **系统层**                   | `cat /sys/kernel/mm/transparent_hugepage/enabled`；`sysctl vm.dirty_bytes`                                            | `[never]`；`1073741824`                                                                                  | THP 单元顺序错（§2.5）、脏页仍用比例制                                                          |
| **`/data/store` 共享盘**（v1.12，P1-14） | `df -h /data/store`；`df -i /data/store`；`iostat -x 1 3 \| grep -E 'sd[a-z]'`；`docker exec silo mcli du -sh local/*`        | 已用 **<70% 告警 / <80% 强告警扩容 / <90% 紧急**；inode <70%；`%util` 不持续 >80%、`await` <20ms                    | Kafka 120G + SILO 三桶配額 350G + Flink 本地状态 + ZK 10G **共用一块 500G 盘**，任一暴涨都会连锁拖垮其他组件 |

> **`/data/store` 必须按"正式容量风险"监控（v1.12 新增，P1-14）**：这块 500G 盘由 Kafka（120G 上限）、SILO（三桶配额合计 350G）、Flink 本地状态（rocksdb + tmp）、ZK（10G）**四类用途共享**，是整套方案剩余的主要物理约束。日常巡检至少采集：`disk_used`、`disk_free`、`disk_util`、`inode 使用率`、`%util`、`await`、**Flink checkpoint duration**、**Kafka log size**、**SILO bucket size**。告警阈值统一为 **70% 告警 / 80% 强告警并触发扩容 / 90% 紧急处置**（具体以 `DBAPI_Doris硬件配置与参数调优.md` 为准）。判读要点：`%util` 与 `await` 同时升高且 checkpoint duration 变长，说明 Flink 与 Kafka/SILO 在抢 I/O，**此时加 quota 无用，只能错峰或分盘**。

---

## 6. 防火墙分区与端口矩阵（firewalld 持久化）

> 原则（对应 §0 两层拆分）：**数据层端口只对数据节点集互通**；跨层仅放行"应用层→Doris 查询/SILO"与"调度层(DS)→数据层"两类白名单；对外只有 APISIX 的 9080/9443。
>
> **v1.11 起不再有 etcd 与 Redis 哨兵的跨层规则**：它们全部收在应用层，走 `ipset:app-nodes` 层内互通。跨层规则从 v1.10 的 6 条降到 3 条（9030/8030/9000），**数据节点的入方向暴露面只剩"应用层查数"与"DS 抽数"两件事**。
>
> **本方案 6 台与 DS 集群同处 `192.168.110.0/24` 单网段（2026-10-23 确认），无法按子网分区，全部改用 ipset 按主机 IP 分组实现同样的隔离效果**——因此全文**禁止**出现 `source address=192.168.110.0/24` 形式的放行规则，一律用 `ipset:data-nodes` / `ipset:app-nodes` / `ipset:ds-nodes`。

### 6.1 主机分组 ipset（6 台全执行一次）

```bash
firewall-cmd --permanent --new-ipset=data-nodes --type=hash:ip
firewall-cmd --permanent --ipset=data-nodes --add-entry=192.168.110.201
firewall-cmd --permanent --ipset=data-nodes --add-entry=192.168.110.202
firewall-cmd --permanent --ipset=data-nodes --add-entry=192.168.110.203
firewall-cmd --permanent --new-ipset=app-nodes --type=hash:ip
firewall-cmd --permanent --ipset=app-nodes --add-entry=192.168.110.204
firewall-cmd --permanent --ipset=app-nodes --add-entry=192.168.110.205
firewall-cmd --permanent --ipset=app-nodes --add-entry=192.168.110.206    # v1.11 第三台
# 调度层：DS 集群 3 个节点（v1.9 新增；与平台同网段，只能按主机 IP 收，不能按 /24 收）
firewall-cmd --permanent --new-ipset=ds-nodes --type=hash:ip
firewall-cmd --permanent --ipset=ds-nodes --add-entry=192.168.110.131
firewall-cmd --permanent --ipset=ds-nodes --add-entry=192.168.110.132
firewall-cmd --permanent --ipset=ds-nodes --add-entry=192.168.110.133
firewall-cmd --reload
```

**解释**：ipset 把"哪些 IP 属于数据层 / 应用层 / 调度层"定义成可复用的集合，后面 zone 与 rich-rule 用 `ipset:名字` 引用——以后增删节点只改 ipset，不必逐条改规则。若机器尚未起 firewalld，先 `systemctl enable --now firewalld`。

> **`ds-nodes` 的三条必须替换成 DS 节点的真实 IP（`192.168.110.0/24` 内）**，不能用网段代替。原因：DS 集群与平台同网段，若写成 `source address=192.168.110.0/24`，则 Doris 9030/8030/8040 与 SILO 9000 对**该网段内所有机器**（含办公终端与未来新增主机）全部放行——叠加"Doris `root` 默认空口令"（§4 步骤 10 之前），等于内网可直连删库。
>
> 替换后复核：`firewall-cmd --permanent --ipset=ds-nodes --get-entries` 应恰好 3 条。

### 6.2 端口矩阵

| 分区   | 组件                       | 端口                                                | 开放范围                                                      |
| ---- | ------------------------ | ------------------------------------------------- | --------------------------------------------------------- |
| 数据层内 | Doris FE                 | 8030(http) 9010(editlog) 9020(rpc) 9030(query)    | ipset data-nodes；9030/8030 另对 **app-nodes 与 ds-nodes** 放行 |
| 数据层内 | Doris BE                 | 8040(web) 8060(brpc) 9050(heartbeat) 9060(thrift) | ipset data-nodes；8040 另对 **ds-nodes** 放行（Stream Load 重定向） |
| 数据层内 | Kafka                    | 9092 9093                                         | ipset data-nodes                                          |
| 数据层内 | SILO                     | 9000 9001                                         | ipset data-nodes；9000 另对 **app-nodes 与 ds-nodes** 放行      |
| 数据层内 | ZooKeeper                | 2181 2888 3888                                    | ipset data-nodes                                          |
| 数据层内 | Flink                    | 6123(JM IPC，**同时由 `high-availability.jobmanager.port` 固定**) 6124(TM) 8081(REST)                  | ipset data-nodes                                          |
| 数据层内 | VRRP（v1.12）              | protocol 112                                      | ipset data-nodes（**SILO VIP `.207` 漂移必需**；漏放行会形成多主）            |
| 应用层内 | DBAPI                    | 8080                                              | ipset app-nodes（APISIX 上游，三台互为备份）                         |
| 应用层内 | PG（Patroni）              | 5432 8008(REST)                                   | ipset app-nodes（8008 供 keepalived 本机探针，跨机无需）              |
| 应用层内 | Redis/哨兵                 | 6379 26379                                        | ipset app-nodes（三台主从 + 3 哨兵互相通告，quorum=2）                 |
| 应用层内 | etcd（gw01/gw02/gw03 三成员） | 2379 2380                                         | ipset app-nodes（v1.11：全应用层，**无跨层规则**）                     |
| 应用层内 | VRRP                     | protocol 112                                      | ipset app-nodes（两个 VIP 漂移必需）                              |
| 对外   | APISIX                   | 9080 9443                                         | 全部外部调用方                                                   |

### 6.3 数据节点（doris-dw01/02/03 各执行）

```bash
firewall-cmd --permanent --new-zone=datanode
firewall-cmd --permanent --zone=datanode --add-source=ipset:data-nodes
# 兜底：本机容器访问宿主端口时，源地址是 docker0 网关 172.31.0.1，不属于 data-nodes
# （SILO/APISIX 已改 host 网络不需要它，但 ZK 仍是 bridge，后续加容器也走这条）
# 两段都要放：172.31.0.0/24 = docker0（bip）；172.31.1.0/24 = 自定义网络地址池
firewall-cmd --permanent --zone=datanode --add-source=172.31.0.0/24
firewall-cmd --permanent --zone=datanode --add-source=172.31.1.0/24
# 数据层内部全通（etcd/哨兵已于 v1.11 迁出数据节点，2379/2380/26379 不在此列）
for p in 8030 9010 9020 9030 8040 8060 9050 9060 9092 9093 9000 9001 2181 2888 3888 6123 6124 8081; do
  firewall-cmd --permanent --zone=datanode --add-port=${p}/tcp
done
# 跨层白名单：应用节点集 → Doris 查询/FE http/SILO
firewall-cmd --permanent --zone=datanode --add-rich-rule='rule family=ipv4 source ipset=app-nodes port port=9030 protocol=tcp accept'
firewall-cmd --permanent --zone=datanode --add-rich-rule='rule family=ipv4 source ipset=app-nodes port port=8030 protocol=tcp accept'
firewall-cmd --permanent --zone=datanode --add-rich-rule='rule family=ipv4 source ipset=app-nodes port port=9000 protocol=tcp accept'
# v1.11：etcd（2379/2380）与 Redis 哨兵（26379）已全部迁到应用层，
#   数据节点上不再监听这三个端口，因此也不再需要任何对应放行规则。
#   若曾按 v1.10 配过，用下面两条清掉（否则规则留着但无进程监听，纯噪音）：
#   firewall-cmd --permanent --zone=datanode --remove-rich-rule='rule family=ipv4 source ipset=app-nodes port port=2379 protocol=tcp accept'
#   firewall-cmd --permanent --zone=datanode --remove-rich-rule='rule family=ipv4 source ipset=app-nodes port port=26379 protocol=tcp accept'
# 跨层白名单：DS 集群节点集 → Doris SQL / Stream Load(http) / BE 重定向 / SILO
# v1.9 修正：按 ipset 逐主机放行，不再按网段。DS 与平台同处 192.168.110.0/24，
#   写 source address=192.168.110.0/24 会对网段内所有机器开放这些端口，隔离失效。
for dsp in 9030 8030 8040 9000; do
  firewall-cmd --permanent --zone=datanode --add-rich-rule="rule family=ipv4 source ipset=ds-nodes port port=${dsp} protocol=tcp accept"
done
# v1.12 新增：SILO VIP .207 的 VRRP 通告（协议号 112），只对数据节点集放行
# 漏这一条的症状：keepalived 起得来、ip a 能看到 VIP，但三台互相收不到通告 → 多主或根本不漂
firewall-cmd --permanent --zone=datanode --add-rich-rule='rule family=ipv4 source ipset=data-nodes protocol value=112 accept'
firewall-cmd --reload
```

> **`.207` 不在任何 ipset 里，也不需要加**：它是 VIP，由 keepalived 在数据节点上"借用"地址；客户端（Flink/DBAPI）连 `.207:9000` 时，报文的目标地址在 ARP 层被解析为持有 VIP 的节点 MAC，源地址仍是各自真实 IP，因此**沿用既有的 `data-nodes`/`app-nodes` 放行规则即可**。这一点很容易反着做——给 ipset 里加个 `.207`，结果规则永远不命中。

> Flink CDC 访问业务源库（3306 等）是**出方向**流量，firewalld 默认放行，无需配置；若源库侧另有防火墙，须在源库侧放行数据节点 IP。

### 6.4 应用节点（dbapi-gw01/02/03 各执行）

```bash
firewall-cmd --permanent --new-zone=appnode
firewall-cmd --permanent --zone=appnode --add-source=ipset:app-nodes
# 兜底：容器（bridge 网络）访问宿主 DBAPI/PG 等端口时源地址是 172.31.0.1
firewall-cmd --permanent --zone=appnode --add-source=172.31.0.0/24
firewall-cmd --permanent --zone=appnode --add-source=172.31.1.0/24
for p in 8080 5432 8008 6379 26379 2379 2380; do
  firewall-cmd --permanent --zone=appnode --add-port=${p}/tcp
done
# v1.11：etcd 与 Redis 哨兵全在应用层，2379/2380/26379 走上面那条 ipset:app-nodes 的层内全通即可，
#   不再需要任何针对 doris-dw01 / doris-dw03 的 address 级规则（这也是收回去的主要收益之一）。
# VRRP 协议放行（两个 VIP 漂移必需，漏掉则 §3.4 步骤 6 的 keepalived 不工作）
firewall-cmd --permanent --zone=appnode --add-protocol=vrrp
# 对外入口：全网开放 APISIX
firewall-cmd --permanent --zone=public --add-port=9080/tcp --add-port=9443/tcp
firewall-cmd --reload
```

**解释**：数据节点上 `datanode` zone 只对 `ipset:data-nodes` 生效——层内全通、层外按 rich-rule 白名单精确放行（应用层仅 9030/8030/9000，**Doris BE 端口、Kafka、ZK、Flink 对应用层与外部一律不通**，落实"DBAPI 只经 9030 查 ADS"）。应用节点同理：DBAPI/PG/Redis/etcd/哨兵全部只对 `ipset:app-nodes` 可达，**再无一条针对数据节点的 address 级规则**（v1.11 收回 etcd 与哨兵带来的净简化）；仅 9080/9443 对外。注意：单网段下 `192.168.110.199/.200` 两个 VIP 与业务 IP 同网段（已确认与现网无冲突），arp 由 VRRP 管理，无需额外路由。

> **同网段带来的两处收窄（v1.9）**：DS 集群在本网段，因此下面两处原本写 `192.168.110.0/24` 的地方也必须收窄，否则 DS 节点（乃至网段内任何机器）都能直连：  
> ① **PG `pg_hba`**（§3.4 步骤 4）：已从 `/24` 收为 `.204/.205/.206/.200` 四个具体地址（DS 不需要连 PG）；  
> ② **APISIX `allow_admin`**（§3.4 步骤 2）：已从 `/24` 收为 `127.0.0.1` + `.204/.205/.206`，9180 不再对本网段全网开放。

验证：`firewall-cmd --zone=datanode --list-all`、`firewall-cmd --zone=appnode --list-all`、`firewall-cmd --permanent --ipset=ds-nodes --get-entries`。

---




## 7. AlmaLinux 易错点与常见问题

1. **firewalld 跨层未放行**：DBAPI（应用层）连不上 Doris（数据层），先查 §6.1 的 ipset 是否含对端 IP、rich-rule 是否加了对应端口；Flink/ZK 端口只在数据层内放，层间无需互访。**v1.11 起跨层规则只剩 3 条**（9030/8030/9000），etcd 与 Redis 哨兵都在应用层走层内互通——如果按 v1.10 的印象去找 2379/26379 的跨层规则，会发现根本没有，这是对的。
2. **SELinux enforcing 下的目录标签（v1.7 修正，按运行形态区分）**：**不是所有 `/data/*` 都该标 `container_file_t`** —— 标错会让宿主机进程直接被拒。
   - **容器挂载目录**（`/data/store`、`/data/redis`、`/data/etcd`）：`chcon -Rt container_file_t /data/store /data/redis /data/etcd`，或在 `-v` 上加 `:z`；
   - **Doris 目录**（`/data/doris`，FE/BE 是**宿主机进程**，不是容器）：**禁止**标 `container_file_t`，否则 `doris_be`/`doris_fe` 读不了自己的数据目录。用 `chcon -Rt var_lib_t /data/doris`；
   - **PG 目录**（`/data/pg`，PGDG rpm + systemd）：需注册 fcontext 再 restorecon，只跑 `restorecon` 不会生效（默认策略里没有 `/data/pg` 的规则，它只会把目录标回 `default_t`）：
     ```bash
     semanage fcontext -a -t postgresql_var_lib_t "/data/pg(/.*)?"
     restorecon -Rv /data/pg
     ```
   排查手段：`ls -Zd /data/doris /data/pg /data/store` 看类型，`ausearch -m avc -ts recent` 看拒绝记录。**不要**用 `setenforce 0` 绕过。
3. **THP / 交换未关**：Doris/Flink GC 停顿、Redis 延迟抖动；按 §2.3/2.4 处理**并重启一次复验**（THP 单元写 `Before=sysinit.target` 会静默失效）。
4. **yum 即 dnf5 别名**：EL10 下 `yum` 命令底层即 dnf5，本手册统一用 `yum`。
5. **单 BE 不能设 3 副本**：`replication_num=3` 至少需 3 个 BE（数据层 3 台）。
6. **时钟不齐**：本环境**已完成 chrony 配置**，无需重复处理；若后续新增节点（如扩 BE、加 DS）须同样接入，否则 CDC binlog 位点、Kafka 时间戳、ZK 会话都会出问题。核对用 `timedatectl status` 是否 `synchronized: yes`。
7. **FE 加不进集群 / 出现 OBSERVER**：确认 doris-dw01 已 `ADD FOLLOWER` 接纳、9010 防火墙已开、helper 地址正确。**dw03 用 `ADD OBSERVER` 是最危险的误配**——FE 多数派只算有投票权的成员，3 台里 1 台 Observer 后 Leader 一宕就选不出新 Leader，整个 Doris 僵死。用 `SHOW PROC '/frontends'` 确认是 2 Follower + 1 Leader。
8. **BE 内存压不住 / 被 OOM Kill**：BE 是 **C++ 进程，`be.conf` 里的 `JAVA_OPTS=-Xmx` 完全无效**，真正旋钮是 `mem_limit`（默认 90%）。64G 共置机必须 `mem_limit=40%`。若 `OOMKilled` 先杀的是 SILO/Kafka，表现为"checkpoint 写失败"这种看似无关的故障——先查 `ps -o rss= -p $(pgrep doris_be)` 是否超 27G。
9. **PG 从库起不来 / Patroni 建从库失败**：`/data/pg` 必须为**空目录**（Patroni 首次启动自动拉基础备份建从库），残留旧数据先清空；`etcd3.hosts` 三台必须可达，etcd 集群不健康时 Patroni 无法选主。
10. **VIP 不漂移**：三台 `interface` 网卡名一致、`virtual_router_id`（VI_1=51 / VI_2=52）与 `auth_pass` 三台相同、`nopreempt` 都开；VI_2 需本机 Patroni **服务**（systemd，非容器）在跑且 `/primary` 可探；另注意 `weight` 必须让**降级后的优先级低于全场最低优先级**（不是"低于某一台备机"），否则探测失败也不会漂移（§3.4 步骤 6）。
11. **VIP 初始落在 gw03**：三台 `priority` 相同 + `state BACKUP` 时 VRRP 按**最大 IP** 胜出，会落到 .206。本版 gw01=150 / gw02=120 / gw03=100 修正（§3.4 步骤 6）。
12. **Kafka 其他 broker 连不上 / KRaft 起不来**：`ADVERTISED_LISTENERS` 必须写各 broker 真实 IP；`PROCESS_ROLES=broker,controller` 时**必须**配 `CONTROLLER_LISTENER_NAMES=CONTROLLER`、`INTER_BROKER_LISTENER_NAME=PLAINTEXT` 与 listener 协议映射（§3.1 步骤 6）。
13. **Flink 选不出 Leader / 作业起不来**：检查 ZK 三台 `Mode` 是否为 1 leader+2 follower；`flink` SILO 桶是否存在；`high-availability.storageDir` 与 `execution.checkpointing.dir` 的 s3:// 端点可达；**S3 Hadoop filesystem plugin 是否部署到 `plugins/s3-fs-hadoop/`**（v1.12 起不再放 `lib/`）；`high-availability.jobmanager.port: 6123` 是否已配且 6123 已放行。
14. **Flink RPC 地址错**：`jobmanager.rpc.address` 必须写**本机主机名**且 /etc/hosts 全节点可解析，否则远端 TM 连不上 Leader JM。另两个高频坑：`rest.address: 0.0.0.0` 是语义错误（监听地址用 `rest.bind-address`，`rest.address` 写本机主机名）；缺 `taskmanager.host` 会导致 TM 注册不上、作业卡 INITIALIZING。
15. **Flink SQL 作业"跑一会儿就没了"**：`sql-client` 提交流式 INSERT 默认 attached，关掉终端会把作业一起取消。加 `SET 'execution.attached' = 'false';`（§4 步骤 8）。
16. **Redis 容器内网 IP 问题**：主从/哨兵跑在 Docker 桥接网络时，故障转移后哨兵会把**容器内部 IP**（如 172.17.x.x；本方案改为 172.31.x.x 后同理）通告给客户端，DBAPI 连不上新主。本版统一用 `--net host` + `--replica-announce-ip <本机IP>` 根治。
17. **Redis 哨兵 2 个永远不切换**：故障转移需要"多数派授权"，2 个哨兵时多数派=2，任一宕机就无法切换。必须 3 哨兵 quorum=2（v1.11 起全在应用层 gw01/gw02/gw03）。验证：`SENTINEL master mymaster` 里 `num-other-sentinels=2`（**若是 3，说明数据节点上残留了 v1.10 的哨兵，见第 43 条**）。
18. **对象存储 checkpoint 不可读**：3 节点 × 1 目录 = 3 drive，**不满足纠删码最低 4 drive**，而 Flink checkpoint 就存在这里。本版改为每节点 2 目录（6 drive）；仍有"整盘损坏"风险，故 checkpoint 保留 3 份 + 桶配额一起上（§3.1 步骤 7）。
19. **Patroni 切换后旧主未回归**：旧主 Patroni 重启后需 `use_pg_rewind` 追平新主；若长期停在 `stopped` 状态，查 `patronictl list` 的 timeline 与 etcd 连通性，勿手工删 `/data/pg`（会丢 rewind 基线）。
20. **etcd 集群丢仲裁**：3 成员最多坏 1 台；**任何两台同停（如 gw01+gw02 同窗口维护）都会让 APISIX 读不到配置、Patroni 选不了主**——维护窗口须保证至少 2 台 etcd 成员在线（v1.11 三个成员都在应用层，意味着**应用层的维护必须逐台做**，不能"两台一起重启"）。etcd 单成员数据损坏时，先 `etcdctl snapshot save` 再重建；`--quota-backend-bytes` 写满会直接拒绝写入（APISIX 改不了路由），需监控 db 体积。
21. **APISIX 连不上 etcd / 路由永远为空**：`config.yaml` 里键名是 **`host`**，不是 `endpoints`（后者是 Dashboard 的写法）。写错时 APISIX 静默回退到 `127.0.0.1:2379`，两台都读不到配置。
22. **写入周期性毛刺（Routine Load Lag 抖动）**：脏页仍用比例制时，64G 机器允许攒到 12.8G 再集中刷盘，与第二块盘上的 Kafka/SILO/Flink 争抢。改用 `vm.dirty_bytes=1G` / `dirty_background_bytes=256MB`（§2.3），注意与 `dirty_ratio` 互斥。
23. **Kafka 换了镜像但没换变量名**（v1.6 新增，最隐蔽）：官方 `apache/kafka` 用 **`KAFKA_*`**，Bitnami 用 **`KAFKA_CFG_*`**。沿用旧名不会报错——容器正常启动，但副本因子、保留策略、controller 配置全部回落到默认值，表现为"能连上、topic 建不出 3 副本"。另外 **`CLUSTER_ID` 三台必须一致**，不一致则选不出 controller。
24. **SILO 混用旧 `mc` 客户端**（v1.6 新增）：SILO 与未改动的上游 `mc` 只保证 best-effort 兼容。统一用镜像内置的 `mcli`（`docker exec silo mcli ...`），且**禁止把旧 MinIO 二进制与新 SILO 混部滚动升级**——要么整集群同版本全停全起，要么不动。
25. **Patroni 起不来：找不到 initdb / PG 拒绝以 root 启动**（v1.6 新增）：改用 PGDG rpm 后，① `patroni.yml` 必须写 `postgresql.bin_dir: /usr/pgsql-17/bin`；② `/data/pg` 属主必须是 `postgres`；③ **不要**手工执行 `postgresql-17-setup initdb`（Patroni 自己 initdb，data_dir 非空会直接失败）。
26. **Doris 4.1.5 升级后磁盘被 file cache 吃掉**（v1.6 新增）：`enable_file_cache` 在 4.1.5 默认从 false 改为 true，BE 会开始占本地盘。必须在 `be.conf` 显式写 `enable_file_cache = false`，否则 480G 的磁盘账本失效。
27. **etcd 3.7 拉不到镜像**（v1.6 新增）：3.7 起只发多架构镜像，**架构专用 tag（如 `v3.7.2-linux-amd64`）已取消**；另外全部 `--experimental-*` 参数被移除，进程遇到即拒绝启动。
28. **容器访问本机端口被防火墙拦**（v1.7 新增，最容易被误判为"组件 bug"）：bridge 网络下容器访问**宿主 IP** 时源地址是 docker0 网关 `172.31.0.1`（本方案已把 docker 网段从默认 `172.17.0.0/16` 改到 `172.31.x`，见 §2.8），不在 `ipset:data-nodes`/`app-nodes` 里，会被 `datanode`/`appnode` zone 拒绝。典型症状：SILO 起得来但 6 drive 认不全、APISIX 报 etcd 连接失败、上游 502。**两头都要处理**：① 需要访问本机其它端口的组件（SILO、APISIX）一律 `--net host`；② 两个 zone 都加 `--add-source=172.31.0.0/24`（docker0）与 `172.31.1.0/24`（地址池）兜底。**注意**：如果你自己改过 `bip` 或扩过地址池，这里的放行段必须跟着改，否则桥接容器一律被拦。
29. **SELinux 把宿主机进程目录标成 `container_file_t`**（v1.7 新增）：`/data/doris`（Doris FE/BE）、`/data/pg`（Patroni+PG）都是**宿主机进程**，标 `container_file_t` 后进程读不了自己的数据目录。只有容器挂载目录（`/data/store`、`/data/redis`、`/data/etcd`）才用 `container_file_t`；PG 用 `semanage fcontext -t postgresql_var_lib_t` + `restorecon`，Doris 用 `chcon -Rt var_lib_t`。
30. **`op_type` 存成了 `+I/+U`**（v1.7 新增）：Flink MySQL CDC 的 `op` metadata 取值是 `+I/+U/-U/-D`，直接写 Kafka 会让 ODS 的 `op_type` 变成 `+I/+U`，DS 对账按 `I` 过滤时全不匹配。必须在 Flink INSERT 里 `CASE` 归一化（见 §4 步骤 8）。
31. **重复提交 Routine Load 导致数据翻倍**（v1.7 新增）：ODS 是 Duplicate 模型，重建作业若沿用 `OFFSET_BEGINNING` 会整段重灌。重建时改 `OFFSET_END`，并用 `GROUP BY order_id HAVING count(*)>1` 自查。
32. **VIP 永远不漂移（v1.8 新增，最隐蔽的"假高可用"）**：`vrrp_script` 的 `weight` 必须让**降级后的优先级低于全场最低优先级**，否则探测失败后本机仍最高，VIP 原地不动。本方案三台 **gw01=150 / gw02=120 / gw03=100**，**weight 取 −80**（gw01 降级后 70，低于 120 与 100，必然切走）。旧稿 −20/−30 会让：APISIX 挂了 VIP .199 仍留在死机上；PG 切主了 VIP .200 仍留在从库上。复核：`ip a` 看 VIP 在哪台，再手动 `systemctl stop apisix` 复验。
33. **哨兵重启后"忘了"监控谁**（v1.8 新增）：`SENTINEL MONITOR/SET` 是**运行时**命令，只存在于内存，容器或主机重启后全部丢失，哨兵变成空进程（表现：`num-other-sentinels=0`、主库真挂也不切）。必须写成 `sentinel.conf` 挂载进容器。另外哨兵默认 `protected-mode yes`，不加 `bind 0.0.0.0` + `protected-mode no` 会导致跨节点通告被拒、3 个哨兵互相发现不了。
34. **Patroni 配置文件路径不一致**（v1.8 新增）：全文统一为 `/etc/patroni/patroni.yml`。旧稿 §3.4 写 `/etc/patroni/patroni.yml`、§3.5 与 `patronictl -c` 写 `/etc/patroni.yml`，照抄会让 systemd 找不到配置。落地先 `systemctl cat patroni | grep ExecStart` 确认单元里的路径。
35. **SILO 可用容量按 2/3 算是错的**（v1.8 新增）：默认 parity 表 6~7 drive → **EC:3**，可用 = 裸容量 × 3/6 = **50%**（900G 裸 → ≈450G），不是 600G。容量规划、桶配额都要按 450G 算；`mcli admin info local` 可看到实际 EC 值。
36. **同网段下用 `/24` 放行等于没做隔离**（v1.9 新增）：DS 集群与平台同处 `192.168.110.0/24`，若写 `source address=192.168.110.0/24` 为 DS 放行 9030/8030/8040/9000，实际是把这些端口对**网段内所有机器**（办公终端、临时虚机、未来新增主机）开放。必须用 `ipset:ds-nodes` 逐主机放行。同一错误还出现在两处非 firewalld 的地方：**PG `pg_hba`** 与 **APISIX `allow_admin`**——这两处 v1.9 也已从 `/24` 收窄到具体地址。排查方法：`firewall-cmd --zone=datanode --list-all` 里不应出现任何 `192.168.110.0/24` 形式的 source。
37. **改了 ipset 但忘了 `--reload`**（v1.9 新增）：`--permanent` 写的 ipset 与 rich-rule 都要 `--reload` 才生效；而 `rich-rule` 引用的 ipset 若不存在，firewalld 会**静默忽略该规则**（不报错），表现为"规则看着都在，DS 就是连不上 9030"。排查：`firewall-cmd --permanent --ipset=ds-nodes --get-entries` 确认有条目，再 `firewall-cmd --reload`，再 `firewall-cmd --zone=datanode --list-all`。
38. **ODS 分区列取了业务时间，导致全量同步永远跑不完**（v1.10 新增，**首次上线必踩**）：`stat_date = date_format(create_time,...)` 配 `dynamic_partition.start=-30`，历史订单落进不存在的分区 → `Partition not found` → ErrorRows 破 1000 → 作业 PAUSED。必须用 `curdate()`。症状识别：作业 State 从 RUNNING 变 PAUSED、`ReasonOfStateChanged` 里能看到分区不存在的报错、Lag 卡住不动。
39. **Flink 全量阶段把 TM 的 managed memory 撑爆**（v1.10 新增）：4G 的 TM 只给 RocksDB 约 1.4G，千万行表全量快照会把 state 顶满 → checkpoint 连续失败 → 作业重启循环。对策：调小 `scan.incremental.snapshot.chunk.size`、全量期间临时把 TM 提到 8G、大表拆作业。**与 38 的区别**：38 是 Doris 侧 PAUSED，本条是 Flink 侧 checkpoint 一直红。
40. **只看 `ErrorRows=0` 就以为数据干净**（v1.10 新增）：`strict_mode=false` 下类型转换失败静默写 NULL 且不计入 ErrorRows。必须额外跑空值率巡检（§4 步骤 9），否则脏数据会一路带到 DWS/ADS 的指标里，等业务发现时已经没法回溯。
41. **DataX `doriswriter.column` 顺序与表列顺序不一致**（v1.10 新增，**静默错位，最难发现**）：`doriswriter` 的 `column` 数组是**按位置填值**，不是按列名映射。`ods_trade_order` 的列序是 `order_id, order_no, stat_date, user_id, shop_id, order_amt, order_status, create_time, op_type, etl_time`，而旧稿按"reader 顺序 + 函数"写成 `..., user_id, shop_id, order_amt, ...`，导致 `user_id` 落进 `stat_date`、金额落进 `shop_id`，**10 列整体错位**。`strict_mode=false` 下 Doris 静默接受，报表照出、数全错。自检：补数后抽样看 `shop_id` 是否等于源库值、`order_amt` 是否是金额量级；若 `order_status` 出现日期就是又错位了。设计文档 §2.7.3 已给出逐位比对表。
42. **应用层 3 台却当成"2 台 + 1 个备胎"用**（v1.11 新增，**等于白花钱**）：三种典型误配——① **gw03 不装 APISIX**（理由通常是"它排最后，平时用不上"）→ gw01/gw02 同时不可用时 VIP .199 无人可接；② **gw03 的 patroni 没起**（`patronictl list` 只有 2 行）→ 主库宕机后仍退化为"没有从库可接"，RPO 降级窗口回到小时级；③ 为了"更强"把 `synchronous_node_count` 改成 **2** → 任一从库重启都会阻塞主库写入。**判断方法**：`patronictl list` 必须 3 行、`ip a` 三台都查、`pg_stat_replication` 计数必须为 2。
43. **数据节点上残留 v1.10 的 etcd / 哨兵容器**（v1.11 新增，**文档改了机器没改**）：删章节 ≠ 删容器。残留的 `etcd-dw01` 会以旧成员身份不断重连，日志刷 `etcdserver: member not found`；残留的 `redis-sentinel` 会继续当第 4 个哨兵参与投票，把 `num-other-sentinels` 从 2 变成 3，**并且它会用旧的 `sentinel monitor` 目标覆盖新配置**。迁移必做：数据节点三台各执行 `docker rm -f etcd redis-sentinel 2>/dev/null; rm -rf /data/store/etcd /data/redis-sentinel`，再按 §5 的两条命令复核。
44. **数据盘没挂上就开始装**（v1.11 补充，**损失最大的一条**）：`mkdir -p /data/doris` 成功 ≠ 挂载成功。若 fstab 没生效或忘了 `mount -a`，`/data/doris` 只是 200G 系统盘上的普通目录，Doris 会安静地把 BE 数据写进去——**没有任何报错**，直到 `/` 写满（通常在第 3 章装 Kafka/SILO 前后），此时 docker、Doris 日志、系统日志一起挂，抢救顺序很被动。三道防线：① 装 Doris 前 `df -hT /data/doris` 必须是 **≈466G 的 xfs**，不是 200G；② `findmnt` 的 `SOURCE` 必须是 `sdb1`；③ **重启一次再验**（`mount -a` 能过但 fstab 写错的情况只有重启才暴露）。已经踩了的补救：停服务 → `umount` → `rm -rf` 系统盘上那份 → 重新挂载后再装，不要试图把已写入的数据"搬"到新盘。

45. **按盘符认盘 + 手抄 UUID（v1.11 补充，2026-10-23 现场实证）**：这台机器**同一块系统盘在不同次启动会换盘符**——同一个 UUID `824af977-…` 第一次启动挂在 `/dev/sda2`、第二次启动变成 `/dev/sdc2`（`EXT4-fs (sdX2)` 内核日志直接可见）。由此产生两个连环坑：
    - **坑一：开机卡 90 秒进 emergency**。fstab 里手抄的"UUID"实际是 `sdb1-ae19c0bf-c580--418792dee760` 这种残缺串（带设备名前缀、中间两段丢了）。UUID 只允许十六进制字符，出现 `s` 就不可能是合法值，`/dev/disk/by-uuid/<串>` 这个符号链接永远不会被创建；systemd 的 `.device` 单元等满 `DefaultDeviceTimeoutSec=90s` 才判失败（屏幕显示 `(1min 21s / 1min 30s)`），`local-fs.target` 失守 → emergency shell。**这 90 秒不是磁盘慢，是在等一个不存在的东西**，重试多少次都一样。
    - **坑二：按 `sdX` 认盘做 `mkfs` 会打错盘**。盘序既然会漂移，`/dev/sdb` 这个"名字"就不代表固定的那块盘。认盘必须**容量 + 型号 + 序列号**三重确认（`lsblk -o NAME,SIZE,MODEL,SERIAL`），200G 是系统盘、两块 500G 是数据盘，对不上就停下来查槽位。
    - **占位符的歧义（本次真正的原因）**：旧版写 `UUID=<sdb1-uuid>`，本意是"`/dev/sdb1` 这个设备的 UUID"，尖括号整体只是占位标记；现场却理解成了"保留 `sdb1-` 前缀、后面拼上 UUID"，于是写成 `UUID=sdb1-ae19c0bf-c580-4cdb-aa67-418792dee760`——**后面 36 位完全正确，只多了一个前缀**。所以这类故障的抢救顺序是：先去掉前缀试试（`sed -i -E 's#^UUID=(sdb[0-9]|sdc[0-9])-#UUID=#' /etc/fstab`），**确认 `blkid` 值与去前缀后的值一致就说明盘没打错，不必重新格式化**。
    - **正确姿势**：UUID 一律用变量自动写入 fstab（§2.4.5 已改成 `UD=$(blkid -s UUID -o value /dev/sdb1)` + 正则校验 + `<<EOF` 展开），**一个字符都不经手**。已经写错的排查：`lsblk -o NAME,SIZE,FSTYPE,UUID` 看实际值，`cat -A /etc/fstab` 看有没有隐藏字符（`^M`、中文空格）。
    - **为什么不等一下就好**：fstab 这两行**故意不加 `nofail`**（见 2.4.5 选项表）。加了 `nofail`，盘挂不上系统照常启动、Doris 把数据写进系统盘，故障推迟到 `/` 写满才爆发（就是第 44 条）。**宁可开不了机，也不要静默写错地方。**

46. **Kafka 数据目录没改属主，容器无限重启（v1.11 补充，2026-10-24 上机实测）**：`apache/kafka` 官方镜像以 **uid:gid=1000(appuser)** 运行，而 `/data/store/kafka/data` 是 root 建的（属主 0:0）。bind mount 进去后容器内 1000 没有写权限，每次启动在 `kafka-storage format` 写 `meta.properties` 时抛 `java.nio.file.AccessDeniedException: /var/lib/kafka/data/bootstrap.checkpoint.tmp` → broker 退出 → 因 `--restart unless-stopped` 被不断拉起，日志里 `Using provided cluster id … Formatting metadata directory … AccessDeniedException` 反复刷。**修复**：`rm -rf /data/store/kafka/data/* && chown -R 1000:1000 /data/store/kafka/data` 后重新 `docker run`；卷挂载加 `:z` 给 SELinux 打标签（`getenforce` 为 Enforcing 时必需）。三台数据节点都要做，且 `CLUSTER_ID` 三台必须一致。这是文档原 §3.1 步骤 6 只 `mkdir` 未 `chown` 的遗漏，已补。

47. **只起一台 Kafka 就以为坏了，其实在等另外两台（v1.11 补充，2026-10-24 上机实测）**：KRaft controller quorum 要 **2/3 多数票**才选得出 leader。只起 dw01（node 1）时，日志会一直刷 `initializeNewPublishers: … still catching up because we still don't know the high water mark yet`，并伴 `Connection to node 2/3 (…:9093) could not be established`、`voterStates={1=GRANTED, 2=UNRECORDED, 3=UNRECORDED}`、`Election timed out before receiving sufficient vote responses`。**这跟第 46 条不是一回事**——format 已过、进程没崩，纯粹是另外两台没起来。别去动 dw01 的配置，直接把 dw02/dw03 的 Kafka 也拉起来（**两台都要先 `chown -R 1000:1000`**，`CLUSTER_ID` 与 dw01 一致），三台互连后几秒自愈。若三台都起了仍连不上，用 `nc -vz <对端IP> 9093` 区分"拒绝=对端没监听"还是"超时=防火墙挡"，再核 §6.3 的 9092/9093 放行是否三台都 `--reload` 过、三台 `CLUSTER_ID` 是否一致。

48. **为排查便利 `systemctl disable firewalld`（永久关防火墙），重启后隔离永久失效（v1.11 补充，2026-10-24）**：本方案六台与 DS 三台同处 `192.168.110.0/24` 单网段，层间隔离**完全依赖 firewalld 的 ipset 按主机分组**（§6.1~§6.4）。`disable` 是开机不自启，比 `stop`（当前停、重启恢复）更彻底——一旦执行，下次重启后 Doris 9030/8030/8040、SILO 9000、Kafka 9092/9093、PG 5432、Redis 6379、etcd 2379 全部对网段内所有机器裸奔，且叠加 Doris `root` 空口令（§4 步骤 10 设口令之前）即内网可直连删库。**正确做法**：排查连通性用 `nc -vz <对端IP> <端口>` 即可（§7-47）；确需临时放开，用 `systemctl stop firewalld` 测完立刻 `systemctl start firewalld` 恢复，**绝不 `disable`**；若运维嫌 ipset 繁琐，应在上游网络层（硬件防火墙 / 交换机 ACL）做隔离，而不是在主机层关 firewalld。§1 注记 1 已明确此禁令。

49. **ZooKeeper 用了 bridge 而非 `--net host`，选举端口 3888 必 `BindException`（v1.11 补充，2026-10-24 上机实测）**：官方 `zookeeper` 镜像默认 `quorumListenOnAllIPs=false`，ZK 只监听 zoo.cfg 中**本机** `server.N` 对应的那个 IP（如 `192.168.110.201`）。若 ZK 容器跑在 bridge 模式（`-p 2181:2181 -p 2888:2888 -p 3888:3888`、无 `--net host`），容器内根本没有宿主 IP，于是 3888 选举端口 `java.net.BindException: Cannot assign requested address` → `Leaving listener thread … won't be able to participate in leader election` → `Exiting JVM with code 14`，因 `--restart unless-stopped` 无限重启。client 端口 2181 默认监听 0.0.0.0 所以不报错，极易误判。**这与 Kafka 那次"只起一台转圈（KRaft 2/3 票不够）"是两码事**——Kafka 那个 format 已过、是等另外两台；ZK 这个是网络模式错配、本机地址不对。**修复**：ZK 命令加 `--net host` 并删掉三行 `-p`（与同层 Kafka/SILO 一致），三台都改；重启后 `docker logs zk | grep -E 'binding to port|LEADING|FOLLOWING'` 应见监听成功且选出 leader（§3.1 步骤 8 命令已改）。

50. **Flink CDC 连接器拿错包：SQL 连接器 vs Pipeline 引擎包，且坐标必须带 `-1.20` 后缀（v1.11 补充，2026-10-24 上机实测）**：Flink CDC 3.6.0 同时发两套——**Pipeline 引擎包** `flink-cdc-3.6.0-1.20-bin.tar.gz`（YAML 管道的 `flink-cdc.sh` + `lib/flink-cdc-pipeline-*.jar`，**里面没有** `flink-sql-connector-mysql-cdc` 这个 jar）和 **SQL 连接器** `flink-sql-connector-mysql-cdc-3.6.0-1.20.jar`（发布在 Maven Central / GitHub Release 的 "Source Connector Jars" 段）。本方案是 **SQL 模型**（§4 步骤 8：`mysql-cdc` 源 + `upsert-kafka` 汇、`sql-client.sh -f` 提交），**只需要把 SQL 连接器 jar 丢进 Flink `lib/`，Pipeline 引擎包根本用不上**。两个典型错误：① 照着官方下载页 `flink.apache.org/downloads` 下成 `flink-cdc-3.6.0-1.20-bin.tar.gz`，解压后搜不到 SQL 连接器 jar——因为它压根不在里面；② jar 名写成 `flink-sql-connector-mysql-cdc-3.6.0.jar`（缺 `-1.20` 后缀）——3.6.0 对 Flink 1.20 的正确坐标是带 `-1.20` 的，另有 `-2.2` 给 Flink 2.2，无后缀的不是 3.6.0 发布物。**连带漏装**：`upsert-kafka` 汇端还要 `flink-sql-connector-kafka-3.4.0-1.20.jar`（Flink 不自带），少了会报 `Connector 'upsert-kafka' does not exist`。正确坐标（§3.1 步骤 9 已改）：`org.apache.flink:flink-sql-connector-mysql-cdc:3.6.0-1.20`、`org.apache.flink:flink-sql-connector-kafka:3.4.0-1.20`，均从 Maven Central 取，不要从 Pipeline 引擎包里找。

51. **Flink 缺 `conf/masters` / `conf/workers`，集群拓扑没人声明（v1.12 新增，P0-01）**：官方原文——`start-cluster.sh` 与 `stop-cluster.sh` "**rely on `conf/masters` and `conf/workers`** to determine the number of cluster component instances"，且 HA 章节明确 "In order to start an HA-cluster configure the **masters** file"。`masters` 每行 `<host>:<该 JM 的 web UI 端口>`（本方案三台都 8081），`workers` 每行一个 TM 主机。**缺它的真实后果不是 HA 失效**（`jobmanager.sh start` 只起本机、不读 masters，三台逐台起照样组成 3 JM + ZK 选主），而是：① 不能用 `start-cluster.sh` 一键起停；② 丢掉 `Starting HA cluster with 3 masters…` 这行唯一的拓扑自检；③ 三台"配置一致"没有强制约束物。**两套机制不可并用**：要么建 masters/workers + 三台免密 SSH + 只在 dw01 跑 `start-cluster.sh`（本版采用），要么不建文件、三台各自 `jobmanager.sh start`——既建文件又逐台手起会重复起 JM（§3.1 步骤 9）。

52. **HA 模式没固定 `high-availability.jobmanager.port`，JM 用的是随机端口（v1.12 新增，P0-02）**：这是最隐蔽的一条。`jobmanager.rpc.port: 6123` 在非 HA 场景下决定 JM 的 IPC 端口，但**官方明确**："By default, the JobManager will pick a **random port** for inter process communication. You can change this via the `high-availability.jobmanager.port` key." 也就是说 HA 模式下不写这一项，每个 JM 起在哪个端口是随机的——防火墙只放了 6123 也没用，故障切换后 TM 重新注册连不上新 Leader，表现为作业无限 RESTARTING、`Could not connect to JobManager`。**必须显式写 `high-availability.jobmanager.port: 6123`**，并在数据层内放行 6123（§6.2），用 `ss -lntp | grep 6123` 逐台确认真的在监听。

53. **SILO 用 HTTP 冒充生产、TLS 只写不落地（v1.12 新增，P1-06）**：配置里 `s3.endpoint: http://…` 是测试形态。生产若沿用 HTTP，checkpoint/savepoint/HA 元数据（含作业状态）全程明文过内网，与设计文档"生产启用 TLS"的结论矛盾。切 HTTPS 不只是把 `http` 改 `https`：需要 CA 与 server 证书、客户端 truststore（Flink 侧 `s3.ssl.verify` 或 JVM truststore）、证书文件权限（`600`）、**到期时间监控**、更新与 reload 流程、以及回滚步骤。**至少做到**：证书到期前 30 天告警；换证先在非 Leader 节点验证再滚动全集群；回滚时保留旧 endpoint 与旧证书一份。未完成这些之前，文档不得写"生产就绪"。

54. **S3 插件塞进 `lib/` 而不是 `plugins/s3-fs-hadoop/`（v1.12 新增，P0-03）**：Flink 1.20 用 **plugin 机制**管理 filesystem 插件，把 self-contained 的 `flink-s3-fs-hadoop-*.jar` 放进 `plugins/s3-fs-hadoop/`（目录名须与 jar 前缀对应），依赖被隔离在独立 classloader，能避免与 `lib/` 下其他 jar 的类冲突。放 `lib/` 通常也能跑，但属于旧做法，一旦后续往 `lib/` 加连接器就可能撞依赖。**注意**：改目录后，验证项与易错点措辞要同步改成"插件是否部署到 `plugins/s3-fs-hadoop`"，不要再写"S3 插件是否拷入 lib"——否则现场照旧措辞检查会误判。

55. **checkpoint 配置键新旧两代混用（v1.12 新增，P1-02）**：Flink 1.20（FLINK-34255）把状态/检查点配置按前缀重组，现行键是 `execution.checkpointing.dir` / `.savepoint-dir`（**连字符**，不是 FLIP 提案里的点号 `savepoint.dir`）/ `.num-retained` / `.storage` / `.incremental`，`state.backend.type` 取代 `state.backend`。**旧键 `state.checkpoints.dir` 等已被 `withDeprecatedKeys` 标记 @Deprecated、当前仍兼容生效**——所以写旧键**不会报错、不会回落堆内存、也不会丢 checkpoint**，只是会在 2.0 强制失效。真正的坑是"混用两代风格"：一半新键一半旧键，日后排查时分不清哪些已迁移。本版已全部统一为新键，改配置时不要又抄回旧键。

56. **`RETAIN_ON_CANCELLATION` 没有配套清理，flink 桶迟早写满（v1.12 新增，P1-09）**：`execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION` 的语义就是"取消作业后**不删** checkpoint"。加上 `num-retained: 3` 只管"自动保留几份"，管不到"取消后遗留的历史作业目录"。长期发布/回滚会持续累积，把 `flink` 桶（100GiB 配额）写满，之后新作业 checkpoint 直接失败、HA 元数据写不进，现象是"集群莫名其妙不选主了"。**必须**：① 作业下线 7 天内删其 `checkpoints/<job-id>`；② savepoint 只留「当前线上版 + 上一可回滚版」两份；③ `flink` 桶用量 >80% 告警、>90% 强制清理（§4 步骤 6b）。

57. **`s3.endpoint` 写单节点 IP 而不是 VIP `.207`（v1.12 新增，P0-02）**：SILO 是三节点 EC 集群，但**客户端只认一个 endpoint**。写 `http://192.168.110.201:9000` 时，dw01 整机故障 → 数据仍在 dw02/dw03，Flink 却连不上 → checkpoint 连续失败、HA 元数据读不到，作业反复重启。**这不是"数据丢了"，但也不是"没事"**：恢复点会退到最后一份成功 checkpoint，且必须人工介入。**正确做法**：数据层单独起 keepalived 提供 VIP `.207`（§3.1 步骤 7b），`s3.endpoint`、Doris `CREATE REPOSITORY`、DBAPI 导出三处**全部**写 `.207`。**注意三个坑**：① VIP 的 `virtual_router_id` 必须与应用层 51/52 区分（用 53）；② 数据节点要放行 VRRP 112（§6.3），否则多主；③ **不要把 `.207` 加进 ipset**——它是 VIP，源地址仍是节点真实 IP，加了规则也不会命中。

58. **备份策略与桶配额各写一套，上线就被打回（v1.12 新增，P0-03）**：曾经三份文档三个口径——技术方案写"每周全量"、详细设计写"每日全量保留 14 天"、手册 `backup` 配额 90GiB 且**根本没有备份作业**（只有 `mcli mb` 建了个桶）。简单算一下：ADS/DWS 单份 ≈25G，每日 ×14 天 = **350G**，是配额的四倍——备份任务第一天就会报空间不足。**v1.12 定稿口径**：**每周全量、保留 3 份 ≈75G**，`backup` 90GiB 足够；14 天以上长期保留必须外挂 NAS/独立对象存储（技术方案 R16）。**并且"有 `BACKUP` 语句 ≠ 有备份"**：必须同时具备 `CREATE REPOSITORY`（§4 步骤 6c）、每周作业编排、按 3 份滚动删除（6d）、**以及一次真实的 `RESTORE SNAPSHOT` 验证（6e）**。只做语句不做恢复验证，等于把风险留到真出事那天。

---

## 8. 变更记录与默认决策

> **阅读说明**：本节及以下各小节是**历史变更存档**，用于追溯"为什么现在是这样"。其中出现的旧文件名（如 `AlmaLinux10部署实施手册.md`）、旧镜像 tag（如 `ghcr.io/zalando/patroni:4.0.4`）、旧组件名（MinIO）、**旧 VIP 号段 `.210/.211`**（2026-10-23 二次调整为 `.199/.200`）、旧节点 IP `.199/.200`（v1.11 起改为 `.204~.206`）**均已失效**，不可照抄；现行口径一律以正文各章与 `DBAPI_Doris组件版本基线.md` 为准。旧版文档原件存于 `.workbuddy/backup/`。
>
> **[HISTORY — DO NOT COPY]** 本节 §8.1 及其后全部小节、以及 §9 修订记录，**均为历史存档**。现场人员搜索关键字（`MinIO`、`.210`、`.199`、旧镜像 tag、旧配置键）时会命中这些段落，**任何命令、配置、IP、版本号都不得直接复制执行**；需要落地时只以 §0~§7 的现行内容与 `DBAPI_Doris组件版本基线.md` 为准。

### 8.1 v1.11 相对 v1.10 的变更（应用服务层 2 台 → 3 台：不是加机器，是三处结构性调整）

起因：`ODS/DWD 暂定可重跑`（备份分层约束放松），应用节点增加一台。IP 重分配为 `dbapi-gw01/02/03 = .204/.205/.206`（原 `.199/.200` 作废），三台规格一致（8C/32G/500G）。

| # | 调整 | 为什么必须跟着改 | 落地位置 |
|---|---|---|---|
| 1 | **PG 改 1 主 2 从** | 2 台时主库宕机→唯一从库升主→**集群里再没有从库能确认同步复制**，Patroni 只能退化为异步：从切换那一刻到旧主修回，**RPO 全程不为 0**；新主再坏一次就全丢。3 台后剩余从库立刻接任同步，**降级窗口从小时级压到秒级** | §3.4 步骤 4（pg_hba 加 `.206`）、§3.5 步骤 4、**§3.6 步骤 4** |
| 2 | **etcd 从跨层投票改为全应用层** | 原 `gw01+gw02+dw01` 是 2 台应用节点时的折中，代价是 dw01 一次故障同时带走 Doris 副本与 1/3 仲裁。现在故障域与应用层完全一致，**数据层怎么坏都不影响控制面** | 删 §3.1 步骤 11、改 §3.4/§3.5 步骤 2、**新增 §3.6 步骤 2** |
| 3 | **Redis 第 3 哨兵从 dw03 撤回应用层** | 原来只为凑 `quorum=2` 才跨层，导致 26379 双向跨层放行、且 dw03 一宕就把"容忍 1 台"降为"容忍 0 台" | 删 §3.3 步骤 10、改 §3.4 步骤 5、**§3.6 步骤 5** |
| 4 | **keepalived priority 150/120/100（原 150/100）** | 三台必须有可排序的让位链。判定规则也从"大于两台之差"改为"**降级后低于全场最低优先级**"：`150−80=70 < 100` | §3.4 步骤 6 重写推导、§3.5 步骤 6 改 120、§3.6 步骤 6 |
| 5 | **新增 §3.6 节点 dbapi-gw03（.206）** | v1.10 只有 gw01/gw02 两节，加机器不补章节 = 现场拿着文档少装一台 | §3.6 |
| 6 | **防火墙跨层规则 6 条 → 3 条** | etcd/哨兵收回后，2379/2380/26379 不再需要跨层；`app-nodes` 增 `.206` | §6.1/§6.2/§6.3/§6.4 |
| 7 | **新增 2 条易错点（42/43）** | 42：3 台被当"2 台 + 备胎"用（三种误配）；43：文档改了但数据节点上残留旧 etcd/哨兵容器 | §7 |
| 8 | **验证清单 30 → 34 项** | 新增 etcd 成员分布、PG 复制连接数、keepalived 让位链，以及数据盘挂载核对 | §5 |

| 9 | **新增 §2.4 数据节点磁盘分区与挂载** | 原文只有 10 行占位式提示（"按 §0.3.2 挂成两个独立挂载点"+ 两行 fstab 示例），没有认盘、分区、格式化、UUID、SELinux 标签的可执行步骤。现场照着做会漏掉"重启复验"，而 `mount -a` 通过并不代表 fstab 写对 | **新增 §2.4（12 个小节）**；原 §2.4~§2.7（THP / 句柄 / JDK / Docker）顺延为 §2.5~§2.8，全文引用同步改写 |
| 10 | **新增易错点 44** | 数据盘未挂载就安装 → 数据静默写进 200G 系统盘，直到 `/` 满才暴露 | §7 |
| 11 | **VIP 号段 `.210/.211` → `.199/.200`** | 原号段与节点 IP（`.204~.206`）挤在同一区间，现场容易把 VIP 当成某台主机；而 `.199/.200` 在 v1.11 已从节点释放、正好空出。改后 **VIP 低段、节点高段**，一眼可辨 | §0.1 VIP 表 + 号段语义说明、§3.4 步骤 6 `virtual_ipaddress`、§3.4 步骤 4 `pg_hba`、§8.5 附录、技术方案 R7、详细设计 §1.2/§7、架构图 |
| 12 | **docker 网段 `172.17.0.0/16` → `172.31.x`** | 默认段与使用 `172.16.0.0/12` 的企业内网重叠时，宿主机访问内网会被 docker0 抢路由，症状是"ping 通一会儿就断"，极难定位。`default-address-pools` 当前为 `base 172.31.1.0/24`，**只够 1 个自定义网络**（本方案全 host 网络，暂不影响）；需要第 2 个时改 `/20` 并同步防火墙 | §2.8 `daemon.json`、§6.3/§6.4 两个 zone 的兜底放行段、§7-28、§5 核对项 |
| 13 | **Doris 二进制包名 `bin-x86_64` → `bin-x64`** | 旧写法 `apache-doris-4.1.5-bin-x86_64.tar.gz` 经实测返回 **HTTP 404**（官方 4.1 线二进制包架构段命名为 `x64` 而非 `x86_64`）。no-AVX2 为 `-bin-x64-noavx2.tar.gz`、ARM 为 `-bin-arm64.tar.gz`。同步补官方 SHA-512 校验步骤与下载页溯源 | §3.1 步骤 2 下载段（`curl`/`sha512sum -c -`/注释）、版本基线文档 Doris 下载注记 |
| 14 | **Kafka 段补"单节点转圈是正常过渡态"警示 + 易错点 47** | 上机实测：只起 dw01 时 KRaft 因 2/3 多数票不足，日志持续刷 `still catching up because we still don't know the high water mark` + `Connection to node 2/3 … could not be established`，易被误判为卡死或第 46 条权限坑（format 已过、非一回事）。明确"先起齐 dw02/dw03（`chown` + 同 `CLUSTER_ID`）即自愈"，并给 `nc -vz` 区分拒绝/超时的排障顺序 | §3.1 步骤 6 验证后警示块、§7 易错点 47 |
| 15 | **§1 注记 1 强化"禁用 firewalld"禁令 + 易错点 48** | 用户询问 `systemctl disable firewalld` 是否可行。明确：`disable`（开机不自启 = 永久关）比 `stop`（重启恢复）更彻底，**绝对不可取**——本方案六台 + DS 三台同处 `192.168.110.0/24`，层间隔离全靠 firewalld ipset 按主机分组；关掉后 Doris 9030/8030/8040、SILO 9000、Kafka 9092/9093、PG 5432、Redis 6379、etcd 2379 全部对网段内所有机器裸奔，且叠加 Doris `root` 空口令（§4 步骤 10 之前）即内网可直连删库。排查连通性用 `nc -vz` 即可，确需临时放开用 `stop` 测完 `start` 回来，**绝不 `disable`** | §1 注记 1 重写、§7 易错点 48 |
| 16 | **ZooKeeper 步骤 8 由 bridge 改为 `--net host`（删 `-p`）** | 上机实测：ZK 容器原用 bridge + `-p 2181/2888/3888`、无 `--net host`，但 zoo.cfg 中 `server.N` 写宿主真实 IP；官方镜像默认 `quorumListenOnAllIPs=false` 只监听本机 server.N 对应 IP，容器内无宿主 IP → 3888 选举端口 `BindException: Cannot assign requested address` → JVM 退码 14、`--restart` 死循环。client 2181 默认 0.0.0.0 不报错，极易误判。与同层 Kafka/SILO 统一为 `--net host`，删掉三行 `-p` | §3.1 步骤 8（命令）、步骤 8 解释段新增 v1.11 网络模式 blockquote、§7 易错点 49 |
| 17 | **Flink CDC 连接器拿错包：SQL 连接器 vs Pipeline 引擎包 + 坐标补 `-1.20` + 补 kafka 连接器** | 上机实测：照官方下载页 `flink.apache.org/downloads` 下成 `flink-cdc-3.6.0-1.20-bin.tar.gz`（Pipeline 引擎包，内含 `flink-cdc-pipeline-*.jar`、**不含** SQL 连接器），解压后找不到 `flink-sql-connector-mysql-cdc` jar；且原稿 jar 名缺 `-1.20` 后缀（3.6.0 对 Flink 1.20 正确坐标是 `flink-sql-connector-mysql-cdc-3.6.0-1.20.jar`，另有 `-2.2`）。本方案是 SQL 模型（§4 步骤 8 `mysql-cdc` 源 + `upsert-kafka` 汇），只需 SQL 连接器 jar 放 Flink `lib/`，Pipeline 引擎包用不上；`upsert-kafka` 汇端还需 `flink-sql-connector-kafka-3.4.0-1.20.jar`（Flink 不自带，原稿漏装）。两个 jar 改从 Maven Central 取 | §3.1 步骤 9（wget + cp 四行）、§7 易错点 50 |

> **VIP 换号不影响 HA 逻辑**：priority 150/120/100、`weight -80`、`virtual_router_id`、探针与让位链**一个都没动**——VIP 只是 VRRP 别在网卡上的浮动地址，换号不改变"服务挂了谁接"。三处连带改动：① `pg_hba` 里作为兜底的 VIP 行改 `.200`（正常连接的源地址仍是节点 IP）；② 文档中凡"节点 `.199/.200`"都是 v1.11 前的旧节点 IP，已全部改为 `.204~.206`，**这两串地址现在只有 VIP 一种含义**；③ **冲突复检必须重做**——"`.210/.211` 与现网无冲突"的确认只对旧号段成立，新号段须重新 `arping`。
>
> 旧号段仍出现在 §8.3 / §8.4 的历史记录里（记录的是当时状态，不追改），**不可照抄**，现行值以 §0.1 为准。

> **没有变的**：组件版本（全部沿用 v1.10 基线）、容量双水位（53%/768G，Doris 侧完全未动）、ODS 入湖日期分区、DataX 列序、网络边界收窄原则。数据层三台一根线都没改。

### 8.2 v1.10 相对 v1.9 的变更（容量与数据正确性复核：2 项必现故障 + 3 项容量/监控缺陷）

前两轮解决"装得上"和"切得动"，本轮解决"**跑得完、装得下、坏了能自愈**"。

| # | 问题 | 后果 | 修正 |
|---|---|---|---|
| 1 | Routine Load 的 `stat_date = date_format(create_time,...)`，而 ODS 动态分区窗口只有 -30 天 | **CDC 全量同步历史订单 100% 失败**：`Partition not found` → ErrorRows 破 1000 → 作业 **PAUSED**，全量永远跑不完 | 改 `stat_date = curdate()`（§4 步骤 9）；设计文档 §2.4.1 同步，DWD 加工加业务日期过滤 |
| 2 | Flink TM 4G、managed 约 1.4G，而 CDC 全量快照要缓存分片 | 千万行表全量时 checkpoint 连续失败 → 重启循环 | §4 步骤 8 补全量阶段对策：调小 chunk size、临时提到 8G、大表拆作业 |
| 3 | DWS/ADS 用日分区 × 10 桶保留 730 天 | 7300 tablet 承载 ~15GB → 2MB/tablet，元数据与 compaction 白受压 | 设计文档改月分区 + 6 桶（tablet 降至 234） |
| 4 | 容量只算稳态，未算单盘故障后副本重建需 1.5 倍空间 | 单副本 295G × 1.5 = 442G > 384G → **副本补不回来，长期停 2 副本** | 新增自愈水位 256G/53%，扩容判据改两条（设计文档 §2.8、硬件文档 §9、§5 验证） |
| 5 | `ErrorRows=0` 被当作"数据干净"；备份"每日全量 14 天"装不下 | 脏数据静默流过 / 备份第一天就失败 | 补空值率巡检（§4 步骤 9 + §5）；备份改分层（设计文档 §5.2） |

§7 易错点扩到 **41 条**（新增 38 ODS 分区取业务时间、39 Flink 全量撑爆 managed memory、40 只看 ErrorRows 的假象、41 DataX 列序错位）。

### 8.3 v1.9 相对 v1.8 的变更（网络边界定稿：1 项确认 + 2 项收窄）

用户已确认两项待办：① VIP `.199/.200` 与现网不冲突；② DS 集群网段 = `192.168.110.0/24`。第 ① 项直接定稿；第 ② 项引出一个原设计未覆盖的矛盾——**DS 与平台同网段，按网段放行即等于放弃隔离**。

| # | 问题 | 后果 | 修正 |
|---|---|---|---|
| 1 | VIP 规划仍带"若与现有分配冲突请改"保留措辞 | 落地时不敢确定 | 定稿为 `.199/.200`，补 `arping` 无冲突复检命令（§0.1） |
| 2 | DS 白名单写 `source address=<DS网段/24>`，填入后即 `192.168.110.0/24` | **Doris 9030/8030/8040 + SILO 9000 对网段内所有机器开放**；叠加 Doris `root` 空口令可直连删库 | 新建 ipset `ds-nodes`，按 **DS 节点 IP** 放行（§6.1 / §6.3），端口矩阵同步（§6.2） |
| 3 | 同网段事实未传导到非 firewalld 的两处白名单 | PG 5432 与 APISIX Admin 9180 对 DS 及网段内任意机器可达 | PG `pg_hba` 收为 `.199/.200/.211`（§3.4 步骤 4）；APISIX `allow_admin` 收为 `127.0.0.1` + `.199/.200`（§3.4 步骤 7） |

**`ds-nodes` 已填定**（2026-10-23）：`192.168.110.131 / .132 / .133`。落地后 `firewall-cmd --permanent --ipset=ds-nodes --get-entries` 应恰好这 3 条。

### 8.4 v1.8 相对 v1.7 的变更（HA 生效性复核：7 项修正）

v1.7 解决的是"组件能不能起来"，v1.8 复核的是"**起来之后高可用会不会真的生效**"。这类问题在单机测试里根本暴露不出来——只有真正宕机时才会发现 VIP 不动、哨兵不切、rewind 失败。

| # | 问题 | 后果 | 修正 |
|---|---|---|---|
| 1 | keepalived `weight -20 / -30` < 两台 priority 差(50) | **两个 VIP 永不漂移**：APISIX 挂了 .210 留在死机；PG 切主了 .211 留在从库 | weight 统一 **−80**，并加 `fall 2`；补 unicast_peer 建议（§3.4 步骤 6） |
| 2 | Patroni 配置路径两处写法不一致（`/etc/patroni/patroni.yml` vs `/etc/patroni.yml`） | systemd 找不到配置、Patroni 起不来 | 全文统一 `/etc/patroni/patroni.yml` + 落地前 `systemctl cat patroni \| grep ExecStart` 复核（§3.4/§3.5） |
| 3 | 哨兵用 `SENTINEL SET` 运行时下发 | 重启后配置丢失 → 哨兵空转，主库真挂也不切 | 改 `sentinel.conf` 文件挂载（3 处）；补 `bind 0.0.0.0` + `protected-mode no` |
| 4 | SILO 6 drive 可用容量按 2/3（600G）记 | **容量规划虚高 33%**；实际 6~7 drive 默认 **EC:3** → 只有 **450G** | 修正三处账本，附默认 parity 表（§3.1 步骤 7 / §4 步骤 6 / §5） |
| 5 | PG 缺 `wal_log_hints` | PGDG initdb 默认不开 checksums，`use_pg_rewind` 在旧主回归时失败 | 补 `wal_log_hints: on` + `hot_standby_feedback: on` |
| 6 | Kafka 未配事务参数，且 `transaction.timeout.ms` 顶到 broker 上限 | Flink upsert-kafka 在 EXACTLY_ONCE 下自动启用事务生产者 → 事务协调器卡住 / `InvalidTxnTimeoutException` | 补 `KAFKA_TRANSACTION_STATE_LOG_*` 三项 + `KAFKA_TRANSACTION_MAX_TIMEOUT_MS=1800000` |
| 7 | Flink S3 插件用 `flink-s3-fs-presto` | presto 版缺 RecoverableWriter，且 FLIP-555 已把它列为待弃用 | 改 `flink-s3-fs-hadoop`（键名同为 `s3.*`，无需改 flink-conf.yaml） |

**另补 3 项**：Doris `root` **默认空口令** → 新增 §4 步骤 10 设口令并给出后续命令的连带改动；FE/BE 补 `priority_networks`（多网卡防绑错）；§4 步骤 5 删除已失效的 `docker exec patroni psql`。

### 8.5 v1.7 相对 v1.6 的变更（命名统一 + 5 项落地缺陷修复）

v1.7 有两类改动：**一类是文档命名与口径**（不影响执行），**一类是会影响"能不能跑起来"的配置缺陷**（必须按新版执行）。

**A. 命名与口径**
1. **文档更名**：`AlmaLinux10部署实施手册.md` → `DBAPI_Doris数据平台部署实施手册.md`。理由是交付物是"DBAPI + Doris 数据平台"，AlmaLinux 10 只是运行时底座；原名字让文档看起来像一份 OS 操作手册，也无法与设计文档、参数文档形成命名族。
2. **全篇对象存储统一**：叙述层一律称 SILO（仅在解释来源时提"MinIO 分支"），目录 `/data/store/minio` → `/data/store/silo`。DBAPI 配置里的 `minio:` 键名保留（那是 DBAPI 的既有配置项，不是组件名），已加注释说明。
3. **版本结论收敛**：`组件版本决策_v1.6.md` 与 `组件版本核查_v1.5.md` 合并为 `DBAPI_Doris组件版本基线.md`，其它文档不再各自写版本号。

**B. 配置缺陷（这 5 条是真会导致部署失败的）**

| # | 问题 | 后果 | 修正位置 |
|---|---|---|---|
| 1 | SILO 用 `-p 9000:9000`（bridge） | 容器访问自己声明的 drive 地址 `192.168.110.201:9000` 时源 IP 是 `172.17.0.1`，被 `datanode` zone 拒绝 → **6 drive 认不全、EC 池建不起来** | 改 `--net host`（§3.1 步骤 7） |
| 2 | APISIX `ports: 9080/9443`（bridge） | 同样原因连不上宿主 etcd 2379 与 DBAPI 8080 → **启动即报 etcd 失败 / 上游 502** | 改 `network_mode: host`，删 `extra_hosts`（§3.4 步骤 2） |
| 3 | 防火墙未考虑容器网段 | 上述两条的根因；即使改了 host 网络，后续新增 bridge 容器仍会踩 | 两个 zone 各加 `--add-source=172.17.0.0/16`（§6.3/§6.4）+ §7-28 |
| 4 | SELinux 把 `/data/doris`、`/data/pg` 标成 `container_file_t` | Doris FE/BE、Patroni/PG 是**宿主机进程**，标成容器类型后读不了自己的数据目录 | 按运行形态区分标签，PG 用 `semanage fcontext`（§7-2、§3.4 步骤 4） |
| 5 | CDC 的 `op` 直接写出、Kafka sink 默认 `acks=1` | ① ODS 的 `op_type` 变成 `+I/+U`，DS 对账按 `I` 过滤全不匹配；② broker 抖动时丢消息 | INSERT 加 `CASE` 归一化 + `properties.acks=all` 与幂等（§4 步骤 8） |

另补两条使用边界：`OFFSET_BEGINNING` 仅限首次初始化（重建会整段重灌导致数据翻倍）；`§2.4.1` 引用改为明确指向**设计文档**（v1.11 起手册自身也有 §2.4.1 磁盘认盘，两者不要混）。

### 8.6 v1.6 相对 v1.5 的变更（组件生态校准）

本版不碰架构与参数账本，只解决"**版本与镜像在 2026-10 还能不能用**"。逐项依据见 `DBAPI_Doris组件版本基线.md`。

1. **MinIO → SILO**（§0.2 / §3.1 步骤 7 / §4 步骤 6 / §5）：上游 `minio/minio` 镜像 2026-09-11 被删除、`minio/mc` 同步下架，原命令必然失败。改用 Pigsty 维护的 MinIO 分支 `pgsty/silo:RELEASE.2026-10-18T00-00-00Z`（保留 S3 API、`.minio.sys` 存储格式、`MINIO_*` 变量、`/minio/*` 路由）；建桶改用镜像内置 `mcli`，健康检查用 `silo healthcheck ready`。
2. **Kafka 换官方镜像并升 4.3.2**（§3.1 步骤 6 / §3.2 / §3.3）：Bitnami 版本化 tag 已迁至 `bitnamilegacy` 并从公共目录移除；3.x 已 EOL。改用 `apache/kafka:4.3.2`，**环境变量前缀 `KAFKA_CFG_*` → `KAFKA_*`**，新增必填 `CLUSTER_ID`（三台一致），改用 `--net host`，日志目录 `/var/lib/kafka/data`。
3. **ZooKeeper 换官方镜像**（§3.1 步骤 8 / §3.2 / §3.3）：`bitnami/zookeeper:3.8` 下架，改 `zookeeper:3.9.6`；变量 `ZOO_SERVER_ID` → **`ZOO_MY_ID`**，数据目录 `/bitnami/zookeeper` → `/data` + `/datalog`，堆改用 `JVMFLAGS`，关 AdminServer。
4. **etcd 3.5.15 → 3.7.2**（§3.1 步骤 11 / §3.4 / §3.5）：3.5.15 带 CVE-2026-33413。3.7 有三处破坏性变更（v2 全移除、`--experimental-*` 全移除、**只发多架构镜像**），我们均不受影响；`--snapshot-count` 保留但 3.8 会删。
5. **Patroni 改 PGDG rpm 宿主机安装**（§3.4 步骤 4 / §3.5 / §4 / §5 / §7-19）：`ghcr.io/zalando/patroni` 当前网络不可达。改用 PostgreSQL 社区为 AlmaLinux 出的 `patroni` + `patroni-etcd` 包（4.1.5），systemd 托管；`patroni.yml` 新增必填 `postgresql.bin_dir: /usr/pgsql-17/bin`；`psql`/`patronictl` 命令由 `docker exec` 改为直接执行。**etcd 与 Patroni 必须成对升级**（etcd 3.6.9+ 收紧认证，Patroni 4.1.1 才适配）。
6. **Doris 4.0.8 → 4.1.5**（§3.1 步骤 2/3）：4.0 线已收官。新增必改项 `enable_file_cache = false`——4.1.5 把该默认值从 false 改成 true，不关会吃掉 sdb 的 480G 余量。
7. **Flink 1.20.1 → 1.20.5 LTS + CDC 3.6.0-1.20**（§3.1 步骤 9）：`flink-conf.yaml` 无改动；新增 `env.java.home: /usr/lib/jvm/temurin-17-jdk`（**Flink 对 Java 21 只是实验性支持，上一版写成"正式兼容"是错的**）；`lib/` 增补 CDC 连接器，并注明必须取 1.20.x 那一套包（3.6.0 同时发 1.20 与 2.2 两套）。
8. **APISIX 3.9 → 3.19.0**（§3.4 步骤 2）：3.9 自 2024-04 起已停止支持，而它是唯一对外入口。
9. **易错点扩到 27 条**：新增 Kafka 变量前缀、SILO 混用旧 mc、Patroni bin_dir/属主/勿手搓 initdb、Doris file cache 占盘、etcd 3.7 镜像 tag（§7-23~27）。

### 8.7 v1.5 相对 v1.4 的变更（实机规格重算）

v1.5 的主要变更——**全部围绕"照着实机规格能不能跑起来"**：

1. **按实机重算全部参数**（§0.3 整节重写）：数据节点 16C/64G + 200G+500G+500G、应用节点 8C/32G/500G。旧稿按 128G + 四块盘（≥2.7T）写，实机挂不上、参数必然全错。分盘改为 **BE 独占 sdb（480G）、Kafka/MinIO/Flink/ZK 共 sdc**，内存账本数据节点 ≈44.3G（余 19.7G）、应用节点 ≈18.7G（余 13.3G）。
2. **BE 内存参数纠错**（§3.1 步骤 3）：删除无效的 `JAVA_OPTS="-Xmx32g"`（BE 是 C++，无 JVM 堆），改为 `mem_limit=40%`（25.6G）+ `soft_mem_limit_frac=0.8`；FE 堆 8G → 4G。
3. **dw03 的 FE 由 Observer 改 Follower**（§0.1 / §3.3 / §4）：Observer 不投票，3 台里 1 台 Observer 会导致 Leader 宕机后无多数派、Doris 整体僵死。
4. **MinIO 从 3 drive 改 6 drive**（§3.1 步骤 7）：每节点在 sdc 上划 `d1`/`d2` 两个目录，满足纠删码最低 4 drive；同时补桶配额与 7 天生命周期。残留"整盘损坏"风险已写明。
5. **Redis 从 2 哨兵改 3 哨兵 + host 网络**（§3.3 步骤 10 / §3.4 / §3.5）：2 哨兵时多数派=2，任一宕机永远无法切换；全部容器改 `--net host` + `--replica-announce-ip`，根治切换后通告容器内网 IP。
6. **其余配置修正**：Kafka 补 KRaft 三件套（`CONTROLLER_LISTENER_NAMES` / `INTER_BROKER_LISTENER_NAME` / 协议映射）；APISIX etcd 键名 `endpoints`→`host` + 补 role/admin_key；keepalived gw01 优先级 150（避免初始 VIP 落到 gw02）；Patroni 镜像固定 `ghcr.io/zalando/patroni:4.0.4` + `pg_hba` 收窄到本网段 + 补 PG 内存参数；Flink `rest.address`→`rest.bind-address` 拆分 + 补 `taskmanager.host` + 本地状态落 sdc；脏页比例制→字节制；THP 单元 `After=sysinit.target`。
7. **命令可执行化**：补 `CREATE DATABASE dbapi_db`（旧稿必报库不存在）；Routine Load 从 `COLUMNS(...)` 占位改为完整列映射 + `jsonpaths`；topic/表名统一 `ods_trade_order`；Flink SQL 列与 ODS 表对齐并加 `execution.attached=false`（否则关终端作业被取消）。
8. **文档卫生**：§0.2.1 表名 `call_log`→`api_call_log(called_at)`、§0.4.3 乱码实体、§5 表格错列、§8 版本叙述、§9 补 v1.3 条目。

以下为本次采用的默认决策，如与你的实际意图不符请指出：

- **IP 规划（已落定，2026-10-23 二次调整）**：**应用层三台** dbapi-gw01/02/03 = `192.168.110.204/.205/.206`（原 `.199/.200` 作废，地址让给 VIP）；**数据层三台** doris-dw01/02/03 = `192.168.110.201/.202/.203`；DS 三台 = `.131/.132/.133`。六台同处 `192.168.110.0/24` 单网段，防火墙用 ipset 分区（§6.1）。
- **硬件规格（2026-10-22 用户确认）**：数据节点 16C/64G/200G+500G+500G；应用节点 8C/32G/500G。全部参数以此为准（`DBAPI_Doris硬件配置与参数调优.md`）。
- **VIP 规划（2026-10-23 定稿，同日二次调整号段）**：`192.168.110.199`（API 入口，VI_1）+ `192.168.110.200`（PG 主库，VI_2）——原为 `.210/.211`，改为 `.199/.200` 后与节点段（`.204~.206`）在地址上分开，便于现场识别。上线前仍须用 `arping -c 3 -I <网卡> <VIP>` 复检一次（须无回包）；这两个地址原为应用节点旧 IP，v1.11 已释放，**但要确认现网没有别的设备已经占用**。
- **时间同步**：**已由用户配置完成，本版不重复处理**（§2.2 保留命令供新增节点参考）。
- **Flink HA**：3 数据节点各 1 JM+TM，ZK 3 实例 quorum=2，checkpoint/HA 落 MinIO（**已确认需高可用**）。
- **Kafka**：3 broker（每数据节点 1 个），KRaft 模式，48h + 5G/分区双闸。
- **MinIO**：分布式 3 节点 × 2 目录 = **6 drive**（满足 EC 最低 4 drive），承载 Flink checkpoint 与 DBAPI 导出；桶配额 flink 80GiB / exports 160GiB。
- **Redis 哨兵**：**3 哨兵，全部在应用层**（gw01/gw02/gw03），`quorum=2`；容器统一 `--net host`，配置落 `sentinel.conf` 文件挂载（不用 `SENTINEL SET` 运行时下发）。**v1.11 起 dw03 不再有哨兵**——原跨层只为凑奇数 quorum，已由 gw03 承担。
- **etcd（v1.4 落地，v1.11 起改为全应用层）**：3 成员集群 **gw01 + gw02 + gw03**（quorum=2），**不再跨层到 dw01**——跨层会让 dw01 一次故障同时带走 Doris 副本与 1/3 仲裁；双用户——APISIX 路由配置中心（单写，只有 gw01 的 APISIX 写）+ Patroni DCS。
- **PG 高可用（v1.4 落地，v1.11 起 1 主 2 从）**：Patroni 托管 **1 主 2 从**，`synchronous_mode: true`（不开 strict），RPO=0；DBAPI 经 VIP `192.168.110.200` 连接，主库宕 30~60s 自动切换、旧主 pg_rewind 自动回归。**必须 3 个实例**：只有 1 个从库时，主库一宕、唯一从库升主后集群再无从库可确认同步，Patroni 会退化为异步、RPO 失守到旧主修回为止。
- **调用日志保留（v1.4 落地）**：`api_call_log` 按月 RANGE 分区（主键含分区列）+ 保留 90 天，DS 月度任务 DROP 到期分区（§0.2.1）。
- **CDC 删除语义**：ODS 只接 I/U，删除靠 DWD 每日对账 + `is_deleted` 软删（§4 步骤 8）。
- **DS 集群（2026-10-23 用户确认）**：独立 3 节点 + 自有 MySQL，不计入 2+3 平台拓扑，**与平台同处 `192.168.110.0/24`**。因此防火墙**按节点 IP** 放行（ipset `ds-nodes`），不按网段——若 DS 与平台不同网段时另论。节点 IP 已确认：**DolphinScheduler-1 = .131、-2 = .132、-3 = .133**，已填入 §6.1。

---

## 9. 修订记录

> **[HISTORY — DO NOT COPY]** 本节全部为历史版本存档，含已失效的 IP、VIP、镜像 tag、组件名与配置键。**不要复制其中任何命令或配置**；现行口径见 §0~§7。

### v1.12 二轮（2026-10-25：第二轮外部复核 P0 闭环）

- **SILO 入口 VIP `192.168.110.207`（P0-02）**：§3.1 新增**步骤 7b**（数据层 keepalived，`virtual_router_id 53`、priority 150/120/100、`weight -80`、健康检查 `docker exec silo silo healthcheck ready`）；§3.2/§3.3 只改 priority；`s3.endpoint`、DBAPI `minio:` 导出地址改指 `.207`；§6.2 矩阵与 §6.3 规则新增 **VRRP 112** 放行；§5 新增「SILO VIP 归属」验证项。
- **备份策略与配额统一（P0-03）**：§4 新增 **6c**（`CREATE REPOSITORY` + `BACKUP SNAPSHOT`）、**6d**（容量核对 + 按 3 份滚动删除）、**6e**（`RESTORE SNAPSHOT` 恢复验证）；口径统一为**每周全量、保留 3 份 ≈75G**（原"每日全量 14 天 ≈350G"与 90GiB 配额矛盾）；§5 新增「备份可恢复」验证项。
- **ODS 语义纠正（P0-01）**：删除"幂等写入 ODS 可接受""ODS/DWD 幂等去重"等错误措辞，明确 ODS 为 `DUPLICATE KEY`、**本身不幂等**，去重责任在 DWD `UNIQUE KEY` + DS 对账。
- **口令与占位符（P1-02 最低要求）**：新增 **§0.5**，明确尖括号内容均为占位符、禁止作生产默认值；给出配置文件 `chmod 600`、`~/.my.cnf` 免明文、交付前 `grep` 扫示例口令三项落地动作。**环境变量化仍不采纳（用户决策）**。
- **示例口令清理**：应用层 keepalived 的 `auth_pass dbapi123/dbapi456` 改为 `<VRRP认证口令A/B>`。
- **易错点**：新增第 57 条（`s3.endpoint` 写单节点而非 VIP，含 VIP 三坑）、第 58 条（备份策略与桶配额各写一套）。

### v1.12（2026-10-25 生产整改版：按外部评审清单关闭 P0/P1）

- **Flink HA 拓扑（P0-01）**：新增 `conf/masters`（`doris-dw01:8081` / `:dw02:8081` / `:dw03:8081`）与 `conf/workers`，三台逐字一致；新增三台免密 SSH 步骤；启动方式由逐台 `jobmanager.sh start` 改为只在 dw01 执行 `start-cluster.sh`（并明确"两套机制不可并用"）。
- **Flink JM 端口（P0-02）**：新增 `high-availability.jobmanager.port: 6123`。官方默认是**随机端口**，不固定则防火墙放行失效、故障切换后 TM 注册不上。
- **S3 插件（P0-03）**：`lib/` 改为 `plugins/s3-fs-hadoop/`，同步修正验证项措辞。
- **残留字符（P0-04）**：删除步骤 9 中孤立的 `r`。
- **SILO 入口单点（P0-05）**：配置注释与解释段明确"数据层 3 节点 ≠ 入口无单点"，`.201` 故障时无法 checkpoint，须经演练确认 RTO。
- **"不丢数据"表述（P0-06）**：改为"至少一次 / 可能重复"，明确 ODS Duplicate 语义与去重责任。
- **故障演练（P0-07）**：§5 新增 JM 切换、TM 故障、SILO 单节点故障三项演练与判定标准。
- **配置键统一（P1-01/02）**：`high-availability` → `high-availability.type`；`state.checkpoints.dir`/`state.savepoints.dir`/`state.checkpoints.num-retained`/`state.backend`/`state.backend.incremental` → `execution.checkpointing.dir`/`.savepoint-dir`/`.num-retained`/`.storage`/`.incremental` + `state.backend.type`；timeout 10min → 3min。
- **rpc.address 语义（P1-03）**：明确其**不是** Leader discovery 机制，HA 由 `high-availability.*` 完成。
- **目录与资源（P1-04/05、P2-01~03）**：RocksDB 与 io.tmp 分目录；新增"资源基线与 4G→8G 策略"表（触发条件、是否重启、是否三台同调、何时恢复、验收阈值）；4 slots / 4G / parallelism 4 明确标注为初始基线而非最佳值。
- **SILO 空间与清理（P1-08/09）**：新增 `backup` 桶，三桶配额 100/160/90GiB；新增 6b checkpoint/savepoint 清理策略与保留窗口规则。
- **Doris 安全与连接（P1-15/16）**：root 口令步骤重写为"窗口最后一分钟"，新增 `dbapi_ro`（只读）与 `ds_rw`（库内读写、无节点权限）账号；新增多 FE 连接口径（JDBC loadbalance / Stream Load / DataX）。
- **共享盘监控（P1-14）**：§5 新增 `/data/store` 容量与 IO 行，明确 70/80/90% 三档阈值与九项采集指标。
- **文档一致性（P1-17/18）**：首页易错点计数 41→44 修正为 50→56、验证清单 34→41；§8/§9 加 `[HISTORY — DO NOT COPY]` 标记。
- **未采纳**：P1-07（S3 Secret 环境变量化）按用户要求不改，配置仍为占位符 `<SILO 用户名>`/`<SILO 强口令>`。

### v1.11（2026-10-23 应用服务层 2 台 → 3 台）

- **IP 重分配**：`dbapi-gw01/02/03 = 192.168.110.204/.205/.206`（原 `.199/.200` 全部作废，全文 63 处已替换）；「五台」→「六台」14 处；§2.1 hosts 增 gw03。
- **PG 1 主 2 从**：`pg_hba` 复制与业务访问均增 `.206`；`patronictl list` 期望 3 行（Leader / Sync / Async）；明确**不设** `synchronous_node_count: 2`（会把可靠性参数变成可用性事故）。
- **etcd 收全应用层**：删除 §3.1 步骤 11（原 dw01 跨层成员）、`--initial-cluster` 三台统一为 `etcd-gw01/gw02/gw03`、APISIX `config.yaml` 的 `etcd.host` 与 Patroni `etcd3.hosts` 均改 `.204/.205/.206`。
- **Redis 哨兵收全应用层**：删除 §3.3 步骤 10（原 dw03 哨兵），第 3 哨兵移至 gw03；`application-prod.yml` 的哨兵列表改 `.204/.205/.206`。
- **keepalived**：priority `150/120/100`；`weight -80` 的推导从"大于两台差值"改写为"降级后低于全场最低优先级"，并补"为什么 20 一档"与三节点 VRRP 的两个注意点。
- **新增 §3.6 节点 dbapi-gw03**（etcd 成员3 + 哨兵3 + PG 第二从库 + keepalived 100），并说明"三台不是加余量，是三项组件的下限"。
- **防火墙**：`app-nodes` 增 `.206`；删除数据节点上的 2379/2380/26379 跨层规则与 gw01/gw02 上针对 `.201/.203` 的 address 级规则，跨层规则由 6 条降为 3 条。
- **验证清单 30 → 33 项**：新增 etcd 成员分布、PG 复制连接数、keepalived 让位链三条，并改写 PG / keepalived 行。
- **易错点 41 → 43 条**：新增 42（3 台当 2 台用的三种误配）、43（数据节点残留旧 etcd/哨兵容器）。
- **§8 各小节重编号**（原 8.1~8.6 顺延为 8.2~8.7），新增 8.1 记录本轮变更。

### v1.10（2026-10-23 容量与数据正确性复核）

1. **Routine Load 的 `stat_date` 改为 `curdate()`**（§4 步骤 9）：修"CDC 全量同步历史订单必报 `Partition not found`、作业 PAUSED、全量永远跑不完"。这是首次上线 100% 触发的问题，只要源库有超过 30 天的数据。
2. **Flink 全量阶段内存对策**（§4 步骤 8）：补 `scan.incremental.snapshot.chunk.size` 调优、全量期临时提 TM 到 8G、大表拆作业；并说明它与 ① 是两种不同的"全量卡住"。
3. **`strict_mode=false` 边界与空值率巡检**（§4 步骤 9、§5）：`ErrorRows=0` 不代表数据干净，补每日空值率 SQL 作为真防线。
4. **新增 3 条验收项**（§5）：分区落点必须是今天、空值率 < 0.1%、单副本总量 < 768G 且每节点 < 53%。
5. **§7 易错点 37 → 41 条**：新增 38/39/40/41（41 为 DataX `doriswriter.column` 列序错位）。
6. **§8 重排**为 8.1(v1.10)/8.2(v1.9)/8.3(v1.8)/8.4(v1.7)/8.5(v1.6)/8.6(v1.5)。
7. 配套修订：设计文档 DWS/ADS 改月分区 + 6 桶、容量新增"故障自愈水位"、备份策略分层（§2.8/§5.2）；硬件文档扩容阈值为 53%/768G。

### v1.9（2026-10-23 网络边界定稿：VIP 确认 + DS 同网段引发的 3 项收窄）

1. **VIP `192.168.110.199/.200` 定稿**（§0.1、§8.5 附录）：用户确认与现网无冲突，去掉"若冲突请改"保留措辞，补 `arping -c 3 -I <网卡> <VIP>` 上线前复检（须无回包）。
2. **DS 白名单改按节点 IP**（§6.1 新增 ipset `ds-nodes`、§6.2 端口矩阵、§6.3 四条 rich-rule 合并为 ipset 循环）：DS 集群与平台同处 `192.168.110.0/24`，原 `source address=<DS网段/24>` 一旦填入即等于对网段内所有机器开放 Doris 9030/8030/8040 与 SILO 9000。改为 `source ipset=ds-nodes` 逐主机放行。
3. **PG `pg_hba` 由 `192.168.110.0/24` 收窄为 `.199/.200/.211`**（§3.4 步骤 4）：DS 不需要连 PG，同网段写 /24 会把 5432 暴露给 DS 及网段内任意机器。
4. **APISIX `allow_admin` 由 `192.168.110.0/24` 收窄为 `127.0.0.1` + `.199/.200`**（§3.4 步骤 7）：Admin API 9180 可增删路由与上游，暴露面按主机收而非按网段收。
5. **§6.3 原则段新增禁令**：全文禁止 `source address=192.168.110.0/24` 形式的放行，一律走 ipset。
6. §7 易错点扩到 **37 条**（新增 36 同网段 /24 放行、37 改 ipset 后未 `--reload`）；§8 重排为 8.1(v1.9)/8.2(v1.8)/8.3(v1.7)/8.4(v1.6)/8.5(v1.5)。
7. **DS 节点 IP 定稿并填入**（2026-10-23 同日补充）：`DolphinScheduler-1/2/3` = `192.168.110.131/.132/.133`，已写入 §0.1、§6.1 `ds-nodes`、§5 验证项；全文占位清零。

**占位已全部清零**：DS 节点 IP（`192.168.110.131/.132/.133`）已填入 §6.1 的 `ds-nodes`。至此全文不再有待填占位。

### v1.8（2026-10-23 HA 生效性复核：7 项修正 + 3 项补充）

1. **keepalived `weight` −20/−30 → −80**（§3.4 步骤 6）：修"探测失败后优先级仍高于备机、两个 VIP 永不漂移"的假高可用；补 `fall 2` 与 `unicast_peer` 建议。
2. **Patroni 配置路径统一为 `/etc/patroni/patroni.yml`**（§3.4/§3.5/§4/§5，共 15 处）：旧稿两处写法不一致，照抄必启动失败；新增 `systemctl cat patroni` 复核步骤。
3. **Redis 哨兵改配置文件挂载**（§3.3 步骤 10 / §3.4 步骤 5 / §3.5 步骤 5）：`SENTINEL SET` 只存内存，重启即丢；补 `bind 0.0.0.0` + `protected-mode no`（否则 3 哨兵跨节点互相发现不了）。Redis 镜像同时由 `redis:7` 钉为 `redis:7.4`（与版本基线一致）。
4. **SILO 容量账本 2/3 → 1/2**（§3.1 步骤 7 / §4 步骤 6 / §5）：6 drive 默认 EC:3，可用 ≈450G 而非 600G。
5. **PG 补 `wal_log_hints: on`**（§3.4 步骤 4）：pg_rewind 依赖它或 data checksums，PGDG initdb 默认不开 checksums。另补 `hot_standby_feedback: on`。
6. **Kafka 补 4 个事务参数**（§3.1 步骤 6）：`upsert-kafka` 在 EXACTLY_ONCE 下自动启用事务生产者，`__transaction_state` 的 min.insync 与 broker 事务超时上限必须显式给。
7. **Flink S3 插件 presto → hadoop**（§3.1 步骤 9）：presto 缺 RecoverableWriter 且已被 FLIP-555 列为待弃用。
8. 补充：§4 新增步骤 10 给 Doris `root` 设口令（默认空口令）+ 连带改动说明；FE/BE 补 `priority_networks`；§4 步骤 5 删除失效的 `docker exec patroni psql`；§5 的 redis-cli 命令补 `docker exec`；§7 易错点扩到 35 条（新增 32~35）。

### v1.7（2026-10-23 文档更名 + 容器网络/SELinux/CDC 语义 5 项缺陷修复）

1. **更名**：`AlmaLinux10部署实施手册.md` → `DBAPI_Doris数据平台部署实施手册.md`；跨文档引用同步（设计文档、硬件参数、版本基线）。
2. **口径统一**：全篇对象存储称 SILO、目录改 `/data/store/silo`；DBAPI 的 `minio:` 配置键保留并加注释。
3. **SILO 改 `--net host`**（§3.1 步骤 7）：修 bridge 模式下容器访问宿主 IP 被 `datanode` zone 拒绝、导致 EC 池初始化失败的问题。
4. **APISIX 改 `network_mode: host`**（§3.4 步骤 2）：修容器连不上宿主 etcd 2379 / DBAPI 8080 导致启动失败或 502 的问题；移除失效的 `extra_hosts`。
5. **防火墙补容器网段**（§6.3/§6.4）：两个 zone 各加 `--add-source=172.17.0.0/16`。
6. **SELinux 标签按运行形态区分**（§7-2 / §3.4 步骤 4）：Doris 用 `var_lib_t`、PG 用 `postgresql_var_lib_t` + `semanage fcontext`，不再一律 `container_file_t`。
7. **CDC 可靠性**（§4 步骤 8）：Kafka sink 加 `acks=all` + 幂等；`op` 归一化为 `I/U`。
8. **Routine Load 边界**：`OFFSET_BEGINNING` 仅限首次初始化，重建须改 `OFFSET_END`。
9. **易错点 27 → 31 条**；§8.1/8.2/8.3 重排。

### v1.6（2026-10-23 组件生态校准：换掉 3 个已下架镜像 + 4 个落后版本）

1. **对象存储 MinIO → SILO**（§0.2 / §3.1 步骤 7 / §4 步骤 6 / §5）：`minio/minio` 与 `minio/mc` 镜像已被删除，改用 `pgsty/silo:RELEASE.2026-10-18T00-00-00Z`；建桶改用容器内置 `mcli`；健康检查 `silo healthcheck ready`；6 drive 纠删码方案不变。
2. **Kafka `bitnami/kafka:3.7` → `apache/kafka:4.3.2`**（§3.1 步骤 6 / §3.2 / §3.3）：变量前缀 `KAFKA_CFG_*`→`KAFKA_*`，新增 `CLUSTER_ID`（三台一致），改 `--net host`，日志目录 `/var/lib/kafka/data`；删除"换 apache/kafka 后 KAFKA_CFG_* 同样支持"的错误说法。
3. **ZooKeeper `bitnami/zookeeper:3.8` → `zookeeper:3.9.6`**（§3.1 步骤 8 / §3.2 / §3.3）：`ZOO_SERVER_ID`→`ZOO_MY_ID`，数据目录改 `/data`+`/datalog`，堆走 `JVMFLAGS`，关 AdminServer，显式放开 4lw 白名单。
4. **etcd 3.5.15 → 3.7.2**（§3.1 步骤 11 / §3.4 / §3.5）：修 CVE-2026-33413；标注 3.7 的三处破坏性变更（v2 移除、`--experimental-*` 移除、只发多架构镜像）与 `--snapshot-count` 将在 3.8 删除。
5. **Patroni 由容器改 PGDG rpm 宿主机安装（4.1.5）**（§3.4 步骤 4 / §3.5 / §4 / §5 / §7-19）：ghcr 镜像不可达；新增 `postgresql.bin_dir`、`/data/pg` 属主、禁止手搓 initdb 三项；`psql`/`patronictl` 命令改为宿主机直接执行；补 venv 降级路径。
6. **Doris 4.0.8 → 4.1.5**（§3.1 步骤 2/3 / §5）：新增 `enable_file_cache = false`（4.1.5 默认值翻转为 true，会吃磁盘余量）与对应验证行。
7. **Flink 1.20.1 → 1.20.5（LTS）+ CDC 3.6.0-1.20**（§3.1 步骤 9 / §2.7）：新增 `env.java.home` 固定 Temurin 17；修正"Flink 1.19+ 正式兼容 Java 21"的错误表述（实为实验性）；`lib/` 增补 CDC 连接器并注明构建分支。
8. **APISIX 3.9-debian → 3.19.0-debian**（§3.4 步骤 2）：3.9 自 2024-04 停止支持。
9. **易错点 22 → 27 条**（§7）：新增 Kafka 变量前缀、SILO 混用旧 mc、Patroni bin_dir/属主、Doris file cache、etcd 3.7 镜像 tag。
10. **新增交付物**：`DBAPI_Doris组件版本基线.md`（版本锁定表 + Doris/Kafka/Flink 升级评估 + 四项上机实测清单）。

### v1.5（2026-10-23 按实机硬件重算参数 + 修复 19 项阻碍级问题）

1. **按实机规格重算参数**（§0.3 全节 / §2.3 / §3.1 各步）：16C64G×3（200G+500G+500G）、8C32G×2（500G）；分盘改为 BE 独占 sdb、其余共 sdc；内存账本与容量换算重写（BE 480G/节点，单副本 480G）。
2. **Doris**：`be.conf` 用 `mem_limit=40%` 取代无效的 `JAVA_OPTS -Xmx32g`（§3.1 步骤 3）；`fe.conf` 堆 8G→4G 并补 `JAVA_OPTS_FOR_JDK_17`；dw03 FE **Observer→Follower**（§0.1/§3.3/§4），并补 `SHOW PROC '/frontends'` 验证要求。
3. **MinIO 6 drive**（§3.1 步骤 7）：每节点 d1/d2 各 150G，满足 EC 最低 4 drive；补桶配额 80/160GiB 与 exports 7 天生命周期。
4. **Redis 3 哨兵 + host 网络**（§3.3 步骤 10 新增 / §3.4 / §3.5）：quorum 1→2；全部容器 `--net host` + `--replica-announce-ip`；DBAPI 哨兵地址补 dw03。
5. **Kafka KRaft 补全**（§3.1 步骤 6）：`CONTROLLER_LISTENER_NAMES`、`INTER_BROKER_LISTENER_NAME`、协议映射；heap 2G、日志目录 sdc、48h+5G/分区双闸、lz4。
6. **APISIX**（§3.4 步骤 2）：etcd 键名 `endpoints`→`host`、补 `deployment.role` 与 `admin_key`、`allow_admin` 限本网段。
7. **keepalived**（§3.4 步骤 6 / §3.5）：gw01 `priority 150`、gw02 `100`，修正初始 VIP 因"大 IP 胜"落到 gw02 的问题。
8. **Patroni**（§3.4 步骤 4 / §3.5）：镜像固定 `ghcr.io/zalando/patroni:4.0.4`；`pg_hba` 由 `0.0.0.0/0` 收窄到 `192.168.110.0/24`；补 PG 内存/连接/WAL/SSD 参数块。
9. **Flink**（§3.1 步骤 9）：`rest.address: 0.0.0.0` 拆为 `rest.bind-address` + `rest.address: <本机名>`；补 `taskmanager.host`；JM 2G/TM 4G、本地状态落 `/data/store/flink/tmp`、checkpoint 30s、`num-retained=3`。
10. **实时链路可执行化**（§4）：补 `CREATE DATABASE dbapi_db`；topic/表名统一 `ods_trade_order`；Flink SQL 列与 ODS 表对齐 + `op` 元数据列 + `execution.attached=false`；Routine Load 补完整列映射与 `jsonpaths`；新增 DELETE 语义边界说明（ODS 只接 I/U，DWD 对账置软删）。
11. **系统层**：脏页比例制→`dirty_bytes` 字节制（数据/应用节点分档）；THP 单元 `After=sysinit.target`；数据盘挂载改为 `/data/doris` + `/data/store` 两挂载点。**时间同步已由用户配置，本版不处理**。
12. **文档卫生**：§0.2.1 统一 `api_call_log(called_at)` + 主键含分区列；§0.4.3 修 HTML 实体乱码；§5 验证表重建（20 项，含 BE 内存/MinIO drive/哨兵数/VIP 归属）；§7 易错点扩到 22 条；§8 版本叙述修正。
13. **§5 验证清单**新增 etcd 集群、Patroni 状态、故障转移演练三行（v1.4 起）。

### v1.4（2026-10-22 高可用增强 + 实际 IP 落定）

1. **IP 按实际规划全量替换**：六台 → 192.168.110.204~203；单网段下防火墙分区由"双子网 zone"改为 **ipset 按主机分组**（§6.1）；`/etc/hosts` 同步。
2. **etcd 升 3 成员集群**（§3.1 步骤 11 / §3.4 步骤 2 / §3.5 步骤 2）：gw01+gw02+dw01 跨层投票，quorum=2；两台 APISIX 指向同一集群同一 prefix，路由**单写强一致**，消除双写漏写风险；§6 放行 2379/2380（含跨层到 dw01）。
3. **PG 引入 Patroni 自动故障转移**（§3.4 步骤 4 / §3.5 步骤 4）：替换手搓 `postgres:16 + pg_basebackup`；`synchronous_mode: true`（RPO=0，不开 strict）；自动建从库、`use_pg_rewind` 旧主自动回归；DBAPI 连接串改经 VIP。
4. **keepalived 加 VI_2**（§3.4 步骤 6）：新增 PG 专用 VIP `192.168.110.200`，探针指向 Patroni `:8008/primary`，主库切换 VIP 自动跟随。
5. **调用日志保留策略**（§0.2.1）：按月 RANGE 分区 + 保留 90 天 + DS 月度任务 DROP 到期分区。
6. **§5 验证清单**新增 etcd 集群、Patroni 状态、故障转移演练三行；**§7 易错点**更新（8/9 Patroni 化、14/15 新增 Patroni 回归与 etcd 仲裁维护项、删除"PG 无自动故障转移"已知限制）。

### v1.3.1（2026-10-22 全册复查）

1. **Doris 升级 4.0.8**：下载 URL/解释更新；Doris 官方要求 Java 17（2.1 及更早为 Java 8，3.0+ 为 Java 17；4.x 尚不支持 Java 21），新增 §2.7.2 Doris 专用 Temurin 17 与 §3.1 步骤 3 组件级 `JAVA_HOME`；新增 AVX2 检查（§3.1 步骤 2）。
2. **Flink 1.18.1 → 1.20.1**：1.18 不支持 Java 21，与系统 JDK 冲突；1.20.1 兼容（≥1.19 即可）。
3. **补齐缺失的 §1（AlmaLinux 10 特性速览）与 §6（防火墙分区与端口矩阵）**——原稿章节跳号且正文缺失，多处引用落空。
4. **修正命令错误**：Flink SQL 提交改为脚本文件 `-f` 方式（原 `-e "SQL"` 非法）并补 checkpoint 间隔；MinIO 建桶改用 `minio/mc` 镜像（`minio/minio` 镜像不含 mc）；MinIO 显式声明 ROOT 账号密码。
5. **补齐 PG 流复制前置**：主库创建 `repl` 复制账号 + `pg_hba.conf` 放行从库网段，`pg_basebackup` 改用该账号。
6. **修正 ZK 端口笔误**（2888 非 2288）、修复 §5 验证表损坏行、§4 补 mysql 客户端前置、Routine Load 目标表名占位符修正。
7. **内核调优落地**（§2.3/§2.5）：THP systemd 固化、`vm.zone_reclaim_mode=0`、swap 口径统一，§0.3.4 逐条标注落地章节。
8. **新增易错点 13/14**：Redis 容器 announce-ip 问题；PG 无自动故障转移的已知限制。

### v1.3（2026-10-22 实时入湖 + Flink HA 立项版）

1. **入湖由"纯批处理（DS+DataX）"升级为"实时 + 批处理混合"**：新增 Flink CDC → Kafka → Doris Routine Load 实时链路落 ODS；DS 继续负责 ODS→DWD→DWS→ADS 层间加工与对账。
2. **Flink CDC 由单实例升级为 HA 集群**：三台数据节点各 1 个 JobManager 候选 + 1 个 TaskManager，ZooKeeper 协调选主，HA 元数据与 checkpoint 落 MinIO。
3. **新增 ZooKeeper 组件**：3 实例，仅服务 Flink HA，部署在数据层。
4. **新增 §0.3 资源规划**：当时按数据节点 128GB 估算（**已被 v1.5 按实机 64G 取代**）。
5. **新增架构图**：`AlmaLinux10架构图_v1.3.svg`（后续版本已更新为 `_v1.4.svg`）。
