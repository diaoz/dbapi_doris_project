# DBAPI + Doris 数据平台技术方案

| 项目 | 内容 |
|---|---|
| 文档版本 | **v1.12**（2026-10-25 · 与手册/详细设计/硬件调优/组件版本基线 v1.12 同步）：**SILO 客户端入口改 VIP `.207`**（数据层 keepalived 第二实例，消除"数据层有 EC、入口是单节点"的残余单点）、**ODS 幂等表述纠正为"至少一次/可能重复"**、**备份口径统一为每周全量保留 3 份（≈75G，90GiB 配额内）**；v1.11 应用层 3 台、v1.10 容量双水位沿用 |
| 定位 | **方案总览**：讲"为什么这样设计、边界在哪、如何验收"，不重复命令 |
| 目标系统 | AlmaLinux 10 |
| 组件基线 | Doris 4.1.5 / Kafka 4.3.2 / Flink 1.20.5 LTS + CDC 3.6.0 / ZooKeeper 3.9.6 / etcd 3.7.2 / SILO 2026-10-18 / Patroni 4.1.5 + PG 17 / APISIX 3.19.0 / Redis 7.4 |
| 配套文档 | 实施命令 → `DBAPI_Doris数据平台部署实施手册.md`<br>表结构与 DDL → `DBAPI_Doris数据平台详细设计.md`<br>参数与账本 → `DBAPI_Doris硬件配置与参数调优.md`<br>版本锁定 → `DBAPI_Doris组件版本基线.md`<br>架构图 → `DBAPI_Doris架构图.svg`<br>评审结论 → `DBAPI_Doris方案与文档评审报告.md` |

---

## 1. 方案概览

### 1.1 建设目标

把数仓里的数据以**受控 API** 的形式开放给 BI、报表与业务系统，同时把数据标准沉淀在数仓侧，而不是散落在各系统的取数 SQL 里。

| 目标 | 落地方式 |
|---|---|
| 数据出口统一 | 外部只访问 APISIX VIP，不直连数据库；所有取数走 DBAPI 定义的 API |
| 数据标准统一 | Doris 四层建模（ODS→DWD→DWS→ADS），指标口径在 DWS/ADS 固化 |
| 明细不出域 | DBAPI 只读 ADS 层，ODS/DWD 明细不对外暴露（也是性能隔离） |
| 实时 + 批量并存 | Flink CDC 实时入湖 + DS 定时批处理加工，两条链路互补 |
| 无单点 | 每个组件都有明确的冗余形态与切换路径（见 §5）。**唯一例外是"数据层的存储入口"已补齐**：SILO 数据层 EC 与客户端入口 VIP `.207` 分离设计，两条都要成立才算无单点（v1.12） |

### 1.2 范围与边界

**在范围内**：6 台平台节点（3 数据 + 3 应用）上的 Doris、Kafka、SILO、Flink、ZooKeeper、etcd、PG/Patroni、Redis、APISIX、DBAPI、keepalived 的部署、配置、联通与验证。

**不在范围内**（需另行安排，避免被默认为"平台交付"）：

- DolphinScheduler 集群本身（独立 3 节点 + 自有 MySQL，只通过网络对接）
- 业务源库的账号授权、binlog 保留期设置（**Flink CDC 依赖 binlog 未过期**，需源库侧确认 `binlog_expire_logs_seconds ≥ 72h`）
- 域名、证书签发、外网映射、WAF
- 监控服务器（Prometheus/Grafana 实例）与告警通道
- 数据治理制度（指标定义、口径评审流程）

### 1.3 方案取舍（与"方案一"的差别）

用户提供的"方案一"是 Nginx 负载均衡 + DBAPI 节点 + 共享 MySQL/PG 三层集群。本方案不采用，原因：

| 维度 | 方案一 | 本方案 | 取舍理由 |
|---|---|---|---|
| 入口 | Nginx + 多级 LB | APISIX + keepalived VIP | APISIX 自带路由/限流/鉴权/可观测，无需再前置 Nginx 叠一层；少一跳少一个故障点 |
| 元数据库 | 共享 MySQL/PG 三层集群 | PG 17 + Patroni（主从 + VIP） | DBAPI 元数据库是**低频写、强一致**场景，Patroni 的自动故障转移 + 同步复制（RPO=0）比三层共享集群更简单可控 |
| 数仓 | 依赖外部数仓 | Doris（自建 3 节点） | 明细不出域、口径可固化；Doris 单集群即可覆盖 ODS→ADS |
| 实时性 | 仅批量 | 实时 + 批量混合 | 业务明确要求实时入仓 |

---

## 2. 架构与数据流

### 2.1 两层拓扑

| 层 | 节点 | 承载 | 设计意图 |
|---|---|---|---|
| 数据/入湖层 | doris-dw01/02/03（16C/64G，200G+500G+500G） | Doris FE+BE、Kafka、SILO、Flink HA、ZooKeeper、**keepalived（SILO VIP `.207`）** | 只做"写 + 存"；Kafka 与 Doris 同层保证 Routine Load 近网络；**不承载任何应用层组件** |
| 应用服务层 | dbapi-gw01/02/03（8C/32G，500G，同构） | APISIX、DBAPI、PG/Patroni（1 主 2 从）、Redis（1 主 2 从）、etcd（3 成员）、哨兵（3 个）、keepalived | 只做"读 + 出口"；与数据层用防火墙隔离；**三台互为备份，任一台故障职能完整** |
| 独立调度集群 | DolphinScheduler-1/2/3（192.168.110.131~133） | DolphinScheduler + 自有 MySQL | 不并入拓扑，只通过网络对接 |

**为什么把 Kafka/Flink/SILO 塞进数据节点而不单独成机**：它们是"写路径"的一部分，与 Doris 的数据交换最频繁（Routine Load 拉 Kafka、Flink 写 SILO）。单独成机会引入跨机带宽与故障域，而 3 台 64G 机器的余量（常驻 ≈43.8G）足以承载。

**反过来说，应用层组件一律不放在数据节点**：v1.10 及以前 etcd 的第 3 个投票成员在 dw01、Redis 第 3 个哨兵在 dw03，都是"只有 2 台应用节点、凑不出奇数"的妥协，代价是数据层一次故障会连带控制面与缓存切换能力。v1.11 应用层扩到 3 台后，这些组件全部收回，**两层的故障域彻底解耦**。

**代价必须承认**：共置意味着**资源争抢**（BE 的 Compaction 与 Kafka 追加、Flink RocksDB 抢同一层 I/O）。这是通过"BE 独占一块物理盘 + `mem_limit=40%` + Kafka 保留上限 + checkpoint 间隔 30s"四项硬约束压下去的，不是自然获得的余量。

### 2.2 两条数据流

**读路径**（蓝）：调用方 → VIP `.199` → APISIX → DBAPI → Doris ADS。
DBAPI 不碰业务库、不碰 ODS/DWD；APISIX 配置存 etcd，三台实例同源，改一次全网生效。

**写路径**（绿）：业务库 binlog → Flink CDC → Kafka → Doris Routine Load → ODS → DWD → DWS → ADS。
- Flink CDC 为 3 节点 Standalone HA（ZK 选主，checkpoint 经 **VIP `.207`** 落 SILO），消除单实例单点。
- 删除语义有硬边界：`upsert-kafka` 的删除是 tombstone，Routine Load 会跳过 → **ODS 只承接 I/U，删除靠 DS 每日对账在 DWD 置 `is_deleted=1`**（详见详细设计 §2.7.2）。这是本方案最重要的一个"已知不完美"，必须写进验收说明，避免报表口径争议。

**三个 VIP 分工（v1.12 新增 SILO VIP，务必区分）**：

| VIP | 承载 | 漂移范围 | 健康检查对象 |
|---|---|---|---|
| `.199` | APISIX 入口（对外 API） | 应用层 gw01/02/03（priority 150/120/100 + `weight -80`） | 本机 APISIX |
| `.200` | PG 主库（DBAPI 元数据库） | 应用层，随 Patroni Leader | 本机 Patroni 角色 |
| **`.207`** | **SILO S3 入口（Flink checkpoint / DBAPI 导出）** | **数据层 dw01/02/03（priority 150/120/100 + `weight -80`）** | 本机 SILO `:9000` |

> **为什么 SILO 必须单独一个 VIP，而不是"EC 了三副本就够了"**：EC 保证的是**数据不丢**，不保证**客户端连得上**。原设计 `s3.endpoint: http://192.168.110.201:9000` 把 Flink 的 checkpoint/HA 元数据绑在 dw01 一台机器上——dw01 整机故障时数据在、但 Flink 写不进 checkpoint，作业会连续失败直到人工改配置。VIP 只解决"入口"，不改变 SILO 自身的 EC 布局。

**批处理链**（紫）：DS 定时 → DataX 抽数 → Doris Stream Load → 层间 SQL 加工 → 对账。
与实时链的分工是：实时链保证"新鲜度"，批处理链保证"完整性"（补数、对账、删除识别、分区维护）。

---

## 3. 技术选型依据

完整版本锁定见 `DBAPI_Doris组件版本基线.md`，这里只记"为什么是这几个"：

| 决策 | 结论 | 关键依据 |
|---|---|---|
| 对象存储 | SILO 替代 MinIO | MinIO 社区版已停发二进制、镜像被删除；SILO 保留 S3 API 与 `.minio.sys` 格式，换二进制不迁数据 |
| etcd | 3.7.2 + Patroni 4.1.5 **成对升级** | etcd ≥3.6.9 收紧认证，只有 Patroni ≥4.1.1 才做对应认证与重试；单升任一都会坏 |
| Flink | 锁 1.20.5 LTS，不上 2.x | CDC 3.6.0 只有 1.20/2.2 两套构建；2.2 非 LTS，2.3 无构建。升 2.x 还要重写 `config.yaml` 且 savepoint 不兼容 |
| Kafka | 4.3.2（不追 4.4.0） | 4.4.0 发布即零补丁版；4.3.2 是最新补丁版。Kafka 3.x 早已 EOL |
| Doris | 4.1.5（不落 4.0.8） | 4.0 线收官；4.1 已迭代到第 5 个补丁。代价是 `enable_file_cache` 默认值翻转，必须显式关闭 |
| PG | 17 + Patroni 4.1.5（rpm） | ghcr 镜像不可达；PGDG 官方为 EL 出包，systemd 单元现成，还消除了容器网络与 VIP 通告问题 |
| APISIX | 3.19.0 | 3.9 已于 2024-04 停止支持，而它是唯一对外入口 |
| JDK | Doris/Flink 用 Temurin **17.0.21+9**（tarball），DBAPI 用系统 21 | Flink 对 Java 21 至今是实验性支持；Doris 4.x 要求 17（jdk-17.0.10+） |

---

## 4. 容量规划

### 4.1 内存（承诺值，不是峰值）

| 节点 | 规格 | 组件承诺合计 | 余量 |
|---|---|---|---|
| 数据节点 | 16C / 64G | ≈43.8 GB（BE 25.6 + FE 5 + Kafka 3 + Flink 6 + SILO 1.2 + ZK 1 + **keepalived 0.02** + OS 2） | ≈20.2 GB |
| 应用节点 | 8C / 32G | ≈18.7 GB（PG 9 + DBAPI 3.5 + Redis 2.5 + APISIX 1 + etcd 0.5 + OS 2） | ≈13.3 GB |

余量的用途是**吸收尖峰**（导入、Compaction、大查询），不是"还能再塞组件"。BE 的 `mem_limit=40%` 是硬约束：默认 90% 会让 BE 涨到 57.6G，导入峰值必被 OOM Kill，且先死的往往是 SILO/Kafka（表现为"checkpoint 写失败"这种看似无关的故障）。

### 4.2 磁盘

| 挂载点 | 容量 | 分配 |
|---|---|---|
| `/` | 200 GB | OS、Docker 镜像、组件日志（约剩 120G） |
| `/data/doris` | 500 GB | FE meta ≤20G + **BE 480G 整盘独占** |
| `/data/store` | 500 GB | Kafka 120G + SILO 300G（d1/d2 各 150G）+ Flink 本地状态 50G + ZK/etcd 10G |

**为什么 BE 要独占一块盘**：Compaction 是持续的随机读 + 顺序写，与 Kafka 追加、SILO checkpoint 抢同一块盘会把 `%util` 长期打满，表现为 Routine Load Lag 抖动、查询 P99 飙升。这是本方案唯一的物理 I/O 隔离手段。

**容量天花板**：BE 480G × 3 ÷ 3 副本 = **480G 单副本可用**。按当前 500G 原始量测算（压缩后三副本 ≈885G，每节点 ≈295G），可支撑约 1.6 倍增长；单节点用到 70%（≈336G）即触发扩容。

### 4.3 扩容优先级

1. **先加 `/data/store` 的盘**（Kafka 保留与 SILO 增长最频繁）
2. 再给 BE 加盘（`storage_root_path` 逗号追加，无需重建集群）
3. 最后才考虑加节点（Doris 加 BE 需重分布，成本最高）

---

## 5. 高可用设计（逐组件 RTO / RPO）

| 组件 | 冗余形态 | 故障切换 | RTO | RPO |
|---|---|---|---|---|
| APISIX | 3 实例 + keepalived VIP `.199` | VRRP 探活漂移（**生效前提：`weight` 必须让降级后的优先级低于全场最低优先级 100，本方案 priority 150/120/100 + `weight -80`**） | 3~10s | 0（无状态） |
| DBAPI | 3 实例，APISIX 上游轮询 | 上游摘除 | 秒级 | 0 |
| PG | Patroni **1 主 2 从** + 同步复制 + VIP `.200` | 自动 failover（30~60s） + pg_rewind 回归（依赖 `wal_log_hints=on`）。**主库宕机后剩一台从库立即接任同步复制**，降级窗口仅"追平差量"的秒级 | 30~60s | **0**（synchronous_mode；2 台时切换后无从库可接会退化为异步，RPO 失守——这是改 3 台的直接原因） |
| Redis | 1 主 2 从 + 3 哨兵（quorum=2，全在应用层） | 哨兵选主 | 5~15s | 缓存可回源，不承诺
| etcd | 3 成员**全在应用层**（gw01/gw02/gw03） | quorum 2/3 仍可写 | 秒级 | 0 |
| Doris FE | 1 Leader + 2 **Follower** | BDBJE 选举 | 10~30s | 0 |
| Doris BE | 3 副本 | 查询自动重试其他副本 | 秒级 | 0 |
| Kafka | 3 broker，副本 3，`min.insync.replicas=2` | ISR 选主 | 秒级 | 0（acks=all） |
| Flink | 3 JM 候选 + 3 TM，ZK 选主 | 重选 + 从 SILO checkpoint 恢复 | 10~30s | **≤30s 重放** |
| SILO | 数据层 3 节点 × 2 drive = 6 drive EC；**客户端入口 VIP `.207`**（数据层 keepalived） | 数据层降级读；入口按 VRRP 漂移到存活的 dw 节点 | 入口 3~10s | 0（EC 可恢复） |
| ZooKeeper | 3 实例 | quorum 2/3 | 秒级 | 0 |

**三处不能宣称 RPO=0**：Flink（checkpoint 间隔决定重放窗口，30s）、Redis（缓存）、**SILO 入口**（VIP 漂移窗口内 checkpoint 写入失败会被 Flink 重试，但若作业恰在两次 checkpoint 之间崩溃，仍需从上一份 checkpoint 恢复 → 回到"≤30s 重放"）。

**关于 ODS 的"幂等"——这是 v1.12 的一处纠正（原文写错了）**：ODS 采用 `DUPLICATE KEY` 模型，**本身不具备幂等能力**。正确表述是：

> checkpoint 间隔 30s，故障恢复存在**重放窗口**。整链语义是**至少一次 / 可能重复**：同一批 binlog 被消费两次，ODS 就会多出两行。最终"不重复"由 DWD 的 `UNIQUE KEY`（MoW）覆盖写 + DS 每日对账保证，**不等于端到端 Exactly-Once**，也**不得把 ODS 行数当作最终正确性依据**。

验收时必须走一遍：`kill Flink JM Leader → HA 恢复 → 比对 Kafka offset → 看 ODS 是否出现重复行（`GROUP BY order_id HAVING count(*)>1`）→ 确认 DWD/ADS 结果仍正确`。旧稿"写入 ODS 是幂等的、30s 重放不会产生重复行（Doris Unique/主键语义）"与 DDL 直接矛盾，已删除。

**必须做的演练**（不做等于没有）：
1. `systemctl stop patroni` → 60s 内新主起来、VIP `.200` 漂移、DBAPI 查询不中断
2. `kill -9` Flink 活动 JM → ZK 重选、作业从 checkpoint 恢复、ODS 继续入数
3. 停一台 Kafka broker → Routine Load Lag 恢复、无数据丢失
4. 停一台 BE → 查询不报错（副本读）
5. **停 dw01 的 SILO 容器** → VIP `.207` 漂到 dw02、Flink checkpoint 继续成功（验证入口 HA，不只是数据层 EC）
6. **`systemctl stop keepalived` 于 dw01** → `.207` 漂移，`curl -s http://192.168.110.207:9000/minio/health/live` 返回 200
7. **ODS 重放验收**：恢复后 `GROUP BY order_id HAVING count(*)>1` 若出现重复，确认 DWD `UNIQUE KEY` 已覆盖、ADS 指标未翻倍

---

## 6. 运维移交边界

部署完成 ≠ 可运营。以下是**方案内已设计、但需要额外实例或流程**的部分，移交时必须逐项确认归属：

| 项 | 状态 | 说明 |
|---|---|---|
| 指标暴露 | ✅ 各组件已开 | Doris FE/BE metrics、Kafka JMX、APISIX `/apisix/prometheus/metrics`、PG exporter 需另装、Redis `INFO` |
| 监控与告警 | ⚠️ **缺实例** | 无 Prometheus/Grafana 服务器与告警通道；建议先上 1 台 4C8G 节点 + 最小告警集（见下） |
| 日志 | ✅ 有轮转 | Docker `max-size 100m × 5`；Doris 日志在 `/data/doris/*/log`，需接入集中日志（未含） |
| 备份 | ⚠️ **短期策略已定、长期存储待排期** | Doris ADS/DWS：`BACKUP` 到 SILO `backup` 桶，**每周全量、保留 3 份（25G×3≈75G，配额 90GiB 内）**；PG：`pg_dump` 每日 + WAL 归档（落在应用节点之外的 `backup` 桶）；ODS/DWD 不做物理备份，靠"可重跑"；**长期（14/30 天以上）必须外挂 NAS 或独立对象存储 ≥1TB**——SILO EC 后的 ≈450G 已被 `flink 100 + exports 160 + backup 90` 分完，装不下 14 天日备（350G） |
| 回滚 | ✅ 有路径 | 二进制/镜像按版本基线回退；Doris 升级前 `BACKUP` 快照；Flink 作业可从 MySQL 重做全量快照 |
| TLS | ⚠️ **未落地** | APISIX 9443 已预留，需证书与 `ssl` 配置；内网 PG/Kafka 当前明文（靠 ipset 隔离） |
| 口令与密钥 | ⚠️ **需制度化** | 全部组件已要求强口令（无默认 minioadmin 之类），但无统一密钥管理；建议纳入密码库并定期轮换 |

**最小告警集（建议首批就上）**：Routine Load Lag > 10000 持续 5min、BE `mem_limit` 触发回收、PG 复制延迟 > 1s、Patroni 非 Leader/Secondary 状态、SILO 可用 drive < 6、磁盘 `/data/*` 使用率 > 75%、APISIX 5xx 比例 > 1%、Flink checkpoint 连续失败 2 次。

---

## 7. 实施计划与验收门

| 阶段 | 内容 | 完成判据（验收门） |
|---|---|---|
| P0 环境准备 | 6 台 OS、IP/hosts、时间同步（已完成）、防火墙 ipset | 六台互通、`ping` 主机名解析正常、`ipset app-nodes` 含 `.204/.205/.206` |
| P1 系统基线 | swap/内核参数、THP、ulimits、JDK 双版本、Docker | 重启后 THP 仍为 `[never]`、`docker info` 正常 |
| P2 数据层 | Doris（FE Follower 加入 + 3 BE）、Kafka、SILO、ZK、Flink | `SHOW PROC '/frontends'` 3 行且 1 Leader + 2 Follower；6 drive EC 成立；**数据节点上不应有 etcd/哨兵容器**（`docker ps`） |
| P3 应用层 | etcd（gw01/gw02/**gw03**）、PG/Patroni（1 主 2 从）、Redis + 3 哨兵、APISIX ×3、DBAPI ×3、keepalived | `patronictl list` **3 行**（1 Leader + 1 Sync + 1 Async）；主库 `pg_stat_replication`=2；VIP `.199/.200` 都在 gw01 |
| P4 链路打通 | 建库建表、SILO 桶、Kafka topic、Flink 作业、Routine Load、DBAPI 配置 | 源库改一行 → 秒级出现在 ODS；API 调用返回 code=0 |
| P5 验证与演练 | 手册 §5 的 **43 项**验证 + **7 项**故障演练（含 SILO VIP 漂移与 ODS 重放） | 全部通过，演练有记录；**VIP 类演练的验收点是「VIP 跟着服务走」**（`.199`/`.200`/`.207` 三个都要验），不只是「服务切了」；**PG 切主演练必须验到 gw03 从 Async 转 Sync** |
| P6 移交 | 口令交付、监控接入、备份作业、运维手册 | 接收方独立完成一次故障切换与一次 API 发布 |

**P4 是风险最集中的阶段**：跨 5 个组件的链路，任何一环配置不一致（topic 名、字段顺序、jsonpaths、S3 端点）都会表现为"作业 RUNNING 但没数据"。建议按"先单组件自测、再两两连通、最后端到端"的顺序推进，不要一次性全起。

---

## 8. 风险登记册

| # | 风险 | 影响 | 缓解措施 | 状态 |
|---|---|---|---|---|
| R1 | Doris 4.1.5 的 Routine Load 客户端与 Kafka 4.3.2 不兼容 | 实时链路不通 | 上机实测；退路是 Kafka 降 4.2.2 | **待实测** |
| R2 | PGDG EL10 仓库未发布 patroni 包 | P3 阻塞 | 降级 pip venv + 手写 systemd | **待实测** |
| R3 | SILO 6 drive 在同一物理盘，整盘损坏 | checkpoint 全失 | checkpoint 保留 3 份 + 桶配额 + 关键 savepoint 归档；扩容时按独立盘规划 | 已知并接受 |
| R4 | 源库 binlog 保留期 < CDC 中断时长 | 位点失效、需重做全量 | 源库侧设 `binlog_expire_logs_seconds ≥ 72h`；作业加 Lag 告警 | **需源库侧确认** |
| R5 | 删除操作在 ODS 无痕迹 | 报表偏大 | DS 每日对账在 DWD 置 `is_deleted=1`；这是设计选择，非缺陷 | 已知并接受 |
| R6 | 共置资源争抢（BE vs Kafka vs Flink） | 延迟抖动 | BE 独占盘 + `mem_limit=40%` + 保留上限 + checkpoint 30s | 已缓解 |
| R7 | VIP `.199/.200/.207` 与现网冲突 | 部署即失败（VRRP 抢占，或业务被误导到别的设备） | 上线前 `arping -c 3 -I <网卡> <VIP>` **三个都**复检，须无回包。号段已由 `.210/.211` 改为 `.199/.200` 并新增 `.207`，原确认作废 | **重新待确认**（v1.12 新增 `.207` 后须再复检一次） |
| R8 | DS 集群与平台同处 `192.168.110.0/24`，按网段放行会击穿隔离 | Doris 9030/8030/8040 与 SILO 9000 对网段内所有机器开放 | 防火墙改按 **DS 节点 IP**（ipset `ds-nodes`）放行；pg_hba 与 APISIX Admin 同步由 /24 收窄到具体地址 | **已关闭**（IP 已确认：.131/.132/.133，填入 ipset `ds-nodes`） |
| R9 | 监控/备份/TLS 未落地 | 可运营性不足 | 见 §6，移交前补齐 | **待排期** |
| R10 | CDC 全量同步历史数据撞上不存在的分区 | 首次接入即 PAUSED，全量跑不完 | 已修：ODS 按入湖日期分区（`stat_date=curdate()`），DWD 加工加业务日期下界 | **已关闭**（v1.10） |
| R11 | 单副本量超 768G 后单盘故障无法补齐副本 | 长期停 2 副本，再坏一台即丢数据 | 扩容判据改为"自愈线 768G / 53%"双阈值；超线时备好降副本 + 缩保留的应急脚本 | **需持续监控** |
| R12 | SILO 配额（100+160+90=350GiB）放不下"每日全量、保留 14 天"（25G×14≈350G，加 checkpoint/导出必然超） | 备份任务第 1 天就报仓库空间不足 | **v1.12 定稿**：SILO 只做短期恢复仓 → ADS/DWS **每周全量、保留 3 份（≈75G）**；14 天以上长期备份外挂 NAS/独立对象存储 ≥1TB（待排期）；ODS/DWD 不做物理备份，走"可重跑" | **已定稿**（2026-10-25 用户确认方案 A） |
| R13 | PG 只 1 台从库时，主库宕机后无从库接任同步复制，RPO 在"等旧主修回"的整段时间失守 | 切换窗口内的已提交数据可能丢；新主再坏一次即全丢 | v1.11 扩到 3 台应用节点，PG 改 1 主 2 从；**上线验收必须确认 `pg_stat_replication`=2** | **已关闭**（v1.11） |
| R14 | 应用层 3 台但被当"2 台 + 1 个备胎"用（gw03 不装 APISIX / patroni 没起 / 把 `synchronous_node_count` 改成 2） | 前两者让 3 台退化为 2 台能力；后者让任一从库抖动阻塞主库写入 | 手册 §7-42 列为显式易错点，验证清单加三条硬判据 | **需验收把关** |
| R15 | SILO 数据层 EC 正常，但客户端 endpoint 绑单节点（原 `.201:9000`） | dw01 整机故障 → 数据在、checkpoint 写不进 → 作业连续失败 | **v1.12 已修**：数据层 keepalived 第二实例提供 VIP `.207`，健康检查本机 `:9000`；`s3.endpoint` 全部改指 `.207`；须演练验证漂移 | **已关闭**（v1.12，待上机验证） |
| R16 | 备份作业与生命周期未自动化（谁跑、失败谁告警、到期谁删） | 备份"有语句没作业"，恢复时才发现没有可用集 | 手册 §3.7 给出 `CREATE REPOSITORY` + `BACKUP` + 保留 3 份清理 + 恢复验证四段；**作业编排与告警仍待接入监控**（R9） | **需排期** |

---

## 9. 结论

**方案合理**：两层各 3 台的拓扑与共置策略匹配"6 台机器 + 实时入仓"的约束；每个组件都有明确冗余形态与切换路径；版本选择避开了 EOL 与已下架镜像，并处理了成对升级约束。

**配置符合最佳实践**：BE 内存走 `mem_limit` 而非 JVM 堆、FE 用 Follower 而非 Observer、Redis 3 哨兵凑够多数派、SILO 6 drive 满足 EC、PG **1 主 2 从**使同步复制在故障转移后仍成立、Flink HA 消除单点、keepalived `weight -80` 对齐 150/120/100 的让位链——这些都是"错一步就假高可用"的位置，已逐条落实。

**仍有三类缺口**：① 四项技术验证必须上机实测（R1/R2 是阻断级）；② **三个 VIP `.199/.200/.207` 须逐个做 ARP 冲突复检**（R7，v1.12 新增 `.207` 后原确认再次作废）；③ 监控与长期备份存储需在移交前补齐（R9/R16）。

**v1.12 关闭的两项（本轮复核产生）**：SILO 客户端入口单点（R15，改 VIP `.207`）；备份"每日 14 天"与 90GiB 配额的矛盾（R12，改为每周全量 3 份 ≈75G，长期外挂）。

**已关闭的确认项**：DS 集群网段确认为 `192.168.110.0/24`，与平台同网段（R8 已按节点 IP 白名单改造，DS 节点 IP `192.168.110.131/.132/.133` 已确认填入）；ODS/DWD 备份口径确认为"可重跑"（R12）；应用层扩至 3 台与 PG 1 主 2 从（R13）。

**重新打开的一项**：**R7**——VIP 号段由 `.210/.211` 改为 `.199/.200` 并新增 `.207`（v1.12），原先"与现网无冲突"的确认只对旧号段成立，**三个新地址须重新 `arping` 复检**。复检通过后即可关闭。

**必须承认的一条语义边界**：整链是**至少一次 / 可能重复**，ODS 为 `DUPLICATE KEY` 模型、**不具幂等性**；最终正确性由 DWD `UNIQUE KEY` 覆盖写 + DS 每日对账保证。任何"ODS 幂等""端到端 Exactly-Once"的表述都是错的，验收不得以 ODS 行数作为正确性依据。

**剩余待办**：4 项技术验证上机实测 + 3 个 VIP 地址复检（R7）+ 长期备份存储排期（R16）。

详细评审过程与逐项打分见 `DBAPI_Doris方案与文档评审报告.md`。
