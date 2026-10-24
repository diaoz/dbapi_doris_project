# DBAPI + Doris 数据平台 · 组件版本基线

| 项目   | 内容                                                                                                                         |
| ---- | -------------------------------------------------------------------------------------------------------------------------- |
| 文档版本 | **v1.12（2026-10-25 定稿，与手册/详细设计/技术方案/硬件调优 v1.12 同步对齐）**｜**本版不涉及任何组件版本变更**：v1.12 两轮整改均为配置键、HA 拓扑（新增 SILO VIP `.207`）、备份策略与文档口径调整，Doris/Kafka/Flink/etcd/SILO/APISIX/Redis/PG/ZK/JDK 的锁定版本与 v1.11 完全一致。历史补充：v1.11 锁定 JDK `Temurin 17.0.21+9`；应用层 2→3 台、etcd/哨兵归位、VIP 号段调整均不涉及版本变更 |
| 定位   | **版本锁定的唯一依据**。所有部署命令、配置参数、文档叙述均以本表为准，其它文档不再各自写版本号                                                                          |
| 依据   | 各项目官方发布页与 EOL 清单（2026-10 核对）+ 用户确认的三条事实（etcd 官方最新 3.7.2 / MinIO 换 SILO / ghcr.io 不可达）                                      |
| 配套   | `DBAPI_Doris数据平台部署实施手册.md`、`DBAPI_Doris数据平台详细设计.md`、`DBAPI_Doris硬件配置与参数调优.md`                                              |

---

## 0. 版本锁定总表（部署即按此拉取）

| 组件               | 锁定版本                                                             | 部署形态                | 决策性质   | 理由（一句话）                                                                                       |                     |
| ---------------- | ---------------------------------------------------------------- | ------------------- | ------ | --------------------------------------------------------------------------------------------- | ------------------- |
| AlmaLinux        | 10.x（内核 6.12）                                                    | 裸机/VM               | 既定     | RHEL 10 兼容，Doris 4.x 官方支持矩阵内                                                                  |                     |
| Doris            | **4.1.5**                                                        | 二进制（systemd）        | 建议升    | 4.0 线已收官，4.1 迭代到第 5 个补丁；新装无存量包袱                                                               |                     |
| Kafka            | **`apache/kafka:4.3.2`**                                         | Docker，`--net host` | 必须换    | Bitnami 版本化 tag 已下架；3.x 早已 EOL；4.3.2 是最新补丁版（4.4.0 无补丁，不追）                                     |                     |
| Flink            | **1.20.5（LTS）**                                                  | 二进制，Standalone HA   | 小幅升    | 1.x 唯一在支持的 LTS 线；CDC 3.6.0 有对应构建                                                              |                     |
| Flink CDC        | **3.6.0-1.20**                                                   | jar 放 `lib/`        | 明确锁定   | 仅提供 1.20 / 2.2 两套构建，2.3 无构建                                                                   |                     |
| ZooKeeper        | **`zookeeper:3.9.6`**（官方）                                        | Docker              | 必须换    | Bitnami 下架；Flink HA 只需要稳定 3.9 线                                                               |                     |
| etcd             | **`quay.io/coreos/etcd:v3.7.2`**                                 | Docker，`--net host` | 必须换    | 3.5.15 带高危 CVE；3.7 已移除 v2 API 与实验参数（我们未使用）                                                    |                     |
| 对象存储             | **`docker.io/pgsty/silo:RELEASE.2026-10-18T00-00-00Z`**          | Docker，6 drive EC   | 必须换    | 上游 `minio/minio` 镜像 2026-09-11 已删除                                                            |                     |
| Patroni          | **PGDG rpm `patroni` 4.1.5**                                     | systemd（非容器）        | 换交付形态  | `ghcr.io/zalando/patroni` 不可达；rpm 由 PG 社区维护、单元文件现成                                            |                     |
| PostgreSQL       | **17.11**（`postgresql17-server`）                                 | systemd（Patroni 托管） | 建议升    | Patroni 4.1.5 已适配，支持期到 2029                                                                   |                     |
| APISIX           | **`apache/apisix:3.19.0-debian`**                                | Docker              | 必须升    | 3.9 已 EOL，而它是对外唯一入口                                                                           |                     |
| Redis            | **`redis:7.4`**                                                  | Docker，`--net host` | 维持 7 线 | 部署命令的 tag 必须写 `redis:7.4`，不要写浮动 tag `redis:7`（v1.8 已同步）                                       | 7.4 仍在维护，不在本次冒进 8.x |
| DBAPI            | 现行版本（Java 21 运行）                                                 | systemd             | 既定     | 仅查 Doris ADS 层                                                                                |                     |
| DolphinScheduler | 3.x（独立集群，自带 MySQL）                                               | 独立 3 节点             | 既定     | 不并入平台拓扑                                                                                       |                     |
| JDK              | **Temurin `17.0.21+9`**（Doris / Flink）+ **系统 OpenJDK 21**（DBAPI） | 双版本并存，tarball       | 小幅升    | Flink 对 Java 21 仅实验性支持；`17.0.21+9` 于 2026-10-22 发布，满足 Doris 要求（jdk-17.0.10+）。下载与 SHA256 已实测核对 |                     |

> 所有镜像 tag **必须钉死不写 `latest`**；有条件时进一步钉 digest。

> **Doris 二进制下载（2026-10-24 修正）**：官方下载页 `https://doris.apache.org/zh-CN/download/`（版本/架构/校验和以此页为准）。4.1.5 二进制包**架构段命名是 `x64`，不是 `x86_64`**：avx2 = `apache-doris-4.1.5-bin-x64.tar.gz`、no-AVX2 = `apache-doris-4.1.5-bin-x64-noavx2.tar.gz`、ARM = `apache-doris-4.1.5-bin-arm64.tar.gz`。部署手册 §3.1 已按此修正，并附 avx2 包官方 SHA-512 校验（`ec7660a4…cb371b6`）。旧写法 `apache-doris-4.1.5-bin-x86_64.tar.gz` 经实测返回 **HTTP 404**，确认下不到。

---

## 1. 版本之间的硬性配对约束（不能单独动其中一个）

| 约束                     | 内容                                                                                                                                               | 违反后果                                              |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------- |
| **etcd ↔ Patroni**     | etcd ≥ 3.6.9 收紧认证（拓扑读取 / lease 续约不再允许匿名）。Patroni **4.1.1** 起才在 member-discovery 与 lease-keepalive 上做认证与重试 → 必须 **etcd 3.7.2 + Patroni 4.1.5 成对** | 只升 etcd：Patroni 续不上租约、反复选不出主；只升 Patroni：CVE 仍在    |
| **Flink ↔ CDC**        | CDC 3.6.0 只发 `1.20.x` / `2.2.x` 构建，无 2.3                                                                                                         | 拿错构建 → 作业提交即 `ClassNotFoundException` / 版本校验失败    |
| **Doris ↔ Kafka 客户端**  | Doris 4.1.5 Routine Load 内部 kafka-clients 需能连 Kafka 4.3.2（Kafka 4.x 删过老协议版本）                                                                     | Lag 不消费或连接被拒 → **必须实测**（见 §3）                     |
| **Doris ↔ file cache** | 4.1.5 把 `enable_file_cache` 默认值 false→true（FE/BE 双侧），会侵占 BE 数据盘                                                                                  | 磁盘账本失效、`/data/doris` 提前写满                         |
| **SILO ↔ 客户端**         | 与未改动的上游 `mc` 仅 best-effort 兼容；运维统一 `mcli`                                                                                                        | 混用可能出现 listing/删除语义差异；**禁止旧 MinIO 与 SILO 混部滚动升级** |
| **Patroni ↔ PG 二进制路径** | rpm 安装后 PG 二进制不在 PATH，`patroni.yml` 必须写 `postgresql.bin_dir: /usr/pgsql-17/bin`                                                                  | Patroni 找不到 `initdb`/`pg_ctl`，起不来                 |

---

## 2. 与旧方案的差异（为什么换）

| 组件         | 旧                                           | 新                                         | 触发变更的事实                                        |
| ---------- | ------------------------------------------- | ----------------------------------------- | ---------------------------------------------- |
| 对象存储       | `minio/minio`                               | `pgsty/silo`                              | MinIO 社区版停发二进制；Docker Hub 仓库归档、镜像被删除 → 原命令必然失败 |
| Kafka / ZK | `bitnami/kafka:3.7`、`bitnami/zookeeper:3.8` | 官方 `apache/kafka:4.3.2`、`zookeeper:3.9.6` | Bitnami 版本化 tag 下架；Kafka 3.x 已 EOL             |
| Patroni    | `ghcr.io/zalando/patroni:4.0.4`             | PGDG rpm 4.1.5                            | ghcr 不可达；且 4.0.4 无法配合新版 etcd                   |
| etcd       | `v3.5.15`                                   | `v3.7.2`                                  | 3.5.15 带高危 CVE；3.7 为当前线                        |
| Doris      | 4.0.8                                       | 4.1.5                                     | 4.0 线收官，官方建议升级                                 |
| APISIX     | 3.9                                         | 3.19.0                                    | 3.9 已 EOL，且为唯一对外入口                             |
| Flink      | 1.20.1                                      | 1.20.5                                    | 同 LTS 线补丁版，配置零改动                               |

**换镜像带来的两个隐形坑（已在手册修正）**：

1. `apache/kafka` 用 **`KAFKA_*`** 前缀，不是 Bitnami 的 `KAFKA_CFG_*`。照抄旧变量会被**静默忽略**，Kafka 用默认配置启动（KRaft quorum 等于没配）。
2. 必须显式给 **`CLUSTER_ID`**，且三台完全一致；用 `kafka-storage.sh random-uuid` 生成一次。

---

## 3. 上线前必须实测的四项（纸面无法确认）

| # | 项目                                      | 验证方法                                                                                       | 不通过时的退路                                                                                |
| - | --------------------------------------- | ------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| 1 | `quay.io/coreos/etcd:v3.7.2` 是否可拉、是否多架构 | `docker manifest inspect quay.io/coreos/etcd:v3.7.2`                                       | 退 3.6 最新补丁（Patroni 4.1.5 已适配 3.6.15）                                                   |
| 2 | SILO 6 drive 分布式是否正常起、Flink 能否读写 S3     | `docker exec silo silo healthcheck ready` + Console 看 6 drive online + 手工写一个 checkpoint 文件 | 退单节点 SILO + 独立备份（牺牲冗余，不牺牲可用）                                                           |
| 3 | Doris 4.1.5 Routine Load 消费 Kafka 4.3.2 | 建 topic → 灌 1000 行 → `SHOW ROUTINE LOAD\G` 看 Lag 归零、`ErrorRows=0`                          | 降 Kafka 至 4.2.2；仍不行再评估 Doris 侧升级                                                       |
| 4 | PGDG EL10 仓库是否已发布 `patroni` 包           | `dnf install -y postgresql17-server patroni patroni-etcd`                                  | `pip install "patroni[psycopg3,etcd3]"` 到独立 venv + 手写 systemd（`bin_dir` 同样指向 rpm 的 PG） |

---

## 4. 升级触发条件（何时才动）

| 组件             | 触发条件                                            | 动作                                                  |
| -------------- | ----------------------------------------------- | --------------------------------------------------- |
| Flink 2.x      | ① CDC 发布支持 2.3+ 的构建；② 测试环境完成 `config.yaml` 迁移跑通 | 两者同时满足再动；我们的 CDC 作业可从 MySQL 重做全量快照，无 savepoint 迁移包袱 |
| Kafka 4.4.x    | 出 4.4.1+ 补丁版                                    | 按补丁升级，不动大版本                                         |
| Doris 4.1.x    | 出 4.1.6+                                        | 滚动升级 FE→BE，先看 release notes 是否触及 Routine Load       |
| SILO           | 每次新版都按"下游升级"对待                                  | 读 release notes、锁 tag、留回滚路径；**全停全起，禁止混部滚动**         |
| etcd / Patroni | 任一升级                                            | **必须成对评估**（见 §1）                                    |

---

## 5. 版本核对记录（过程留痕）

| 日期         | 核对内容         | 结论                                                                                                           |
| ---------- | ------------ | ------------------------------------------------------------------------------------------------------------ |
| 2026-10-22 | 首轮全量版本核查     | 发现 3 个镜像已下架（`minio/minio`、`minio/mc`、`bitnami/*`）、4 个版本落后、1 处错误结论（"换 apache/kafka 后 `KAFKA_CFG_*` 同样支持"实为错误） |
| 2026-10-23 | 按用户确认的三条事实复核 | etcd → 3.7.2；对象存储 → SILO；Patroni → PGDG rpm。三者构成 v1.6 版本基线                                                   |
| 2026-10-23 | 命名与口径统一      | 文档更名去 OS 前缀；全篇对象存储统一 SILO；版本结论收敛到本文                                                                          |
| 2026-10-25 | v1.12 二轮复核：版本是否需变更 | **结论：不变更**。v1.12 两轮改动集中在 Flink 配置键迁移（1.20 新键）、`conf/masters`、SILO 入口 VIP `.207`、备份策略与文档口径，均不影响任何组件的锁定版本；文档版本升 v1.12 仅表示"与同批文档对齐"，**不代表组件升级** |

> 版本核查的历史过程与决策推演（`组件版本核查_v1.5`、`组件版本决策_v1.6`）已于 2026-10-24 随旧稿清理删除，结论已全部收敛到本文与各版修订记录，无需回溯。
