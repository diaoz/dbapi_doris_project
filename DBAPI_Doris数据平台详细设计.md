# DBAPI + Doris 数据平台详细设计文档

| 项目 | 内容 |
|---|---|
| 文档版本 | **v1.12**（与部署实施手册 v1.12 同步：Flink HA 补 `conf/masters`、固定 `high-availability.jobmanager.port`、checkpoint 配置键迁移到 Flink 1.20 新键、SILO 三桶配额与清理、RPO 表述加前提、容量水位统一、APISIX 版本清理、MinIO/SILO 命名统一；v1.11 应用层 2 台 → 3 台、v1.10 容量双水位与 ODS 入湖日期分区均沿用） |
| 日期 | 2026-10-25 |
| 范围 | Doris 数据标准底座数仓 + DBAPI 数据 API 发布平台 |
| 配套文档 | `DBAPI_Doris数据平台部署实施手册.md`（**v1.12**，部署命令与逐节点步骤）<br>`DBAPI_Doris硬件配置与参数调优.md`（**v1.12**，内存/磁盘账本与逐组件参数）<br>`DBAPI_Doris组件版本基线.md`（**版本锁定的唯一依据**，与本包同期）<br>`DBAPI_Doris架构图.svg` |
| 与前版关系 | v1.0（2026-10-13）已归档。v1.5 与手册统一 IP / 命名 / 规格 / Java 版本；v1.6 同步组件版本；v1.7 统一 SILO 口径并修正 PG 版本；v1.11 扩至 3 台；**v1.12 按外部评审清单关闭 P0/P1（仅 P1-07 未采纳）** |

> **版本口径（v1.12，唯一以 `DBAPI_Doris组件版本基线.md` 为准）**：Doris **4.1.5**、Kafka **4.3.2**、Flink **1.20.5 LTS**（CDC 3.6.0-1.20）、etcd **3.7.2**、PostgreSQL **17**（Patroni 4.1.5）、APISIX **3.19.0**、ZooKeeper **3.9.6**、SILO **RELEASE.2026-10-18**、Redis **7.4**、JDK **Temurin 17.0.21+9**（DBAPI 用系统 OpenJDK 21）。
>
> **命名口径（v1.12 统一）**：本文**组件名一律写 SILO**（`pgsty/silo`，Pigsty 维护的 S3 兼容对象存储）。仅在解释兼容性时保留三个历史痕迹——`.minio.sys`（存储格式名）、`MINIO_*`（兼容环境变量前缀）、`minio_*`（兼容指标名）。**不要再按"MinIO"找镜像或装软件**：上游 `minio/minio` 镜像已于 2026-09-11 删除，现场若照旧名部署必然拉取失败。

---

## 0. 版本变更摘要（v1.0 → v1.5）

| 项 | v1.0 | **v1.5** | 原因 |
|---|---|---|---|
| IP | 10.0.0.x 示例 | `192.168.110.204~203` | 按实际规划落定 |
| 入口 | 提到 Nginx 上游轮询 | **APISIX + keepalived VIP .199**，无 Nginx | 架构定版 |
| Doris FE | 1 Leader + 1 Follower + 1 **Observer** | **1 Leader + 2 Follower** | Observer 不参与选举，无法凑多数派 |
| 硬件 | FE/BE 8C16G / 16C64G，分 4 类节点 | **应用节点 8C32G×3（v1.11）、数据节点 16C64G×3（200G+500G+500G 三盘）** | 实机规格 |
| BE 内存 | `JAVA_OPTS="-Xmx32g"` | **`mem_limit=40%`** | BE 是 C++ 进程，无 JVM 堆 |
| 入湖 | Flink CDC → Kafka → Routine Load | 同，但**明确 DELETE 语义边界**（ODS 承接 I/U，删除走 DWD 对账） | upsert-kafka 的 tombstone 会让删除静默丢失 |
| 日志表 | `api_call_log` / 保留 180 天 | `api_call_log(called_at)` / 保留 **90 天** | 与手册 §0.2.1 统一 |
| Java | Spring Boot 3.2 + Java 17 | Spring Boot 3.2 + **Java 21**（Doris 仍用 Temurin 17） | AlmaLinux 10 无 Java 17 仓库包 |
| PG 高可用 | 手工主从 | **Patroni + etcd，synchronous_mode=true（同步复制正常时 RPO≈0，前提见 §6）** | 自动故障转移 |
| 建表 DDL | 3 处会直接报错 | 已修正（§2.4） | key 列顺序 / 分布列必须在 key 内 / 分区表主键须含分区列 |
| 缺失章节 | 无监控 / 备份 / TLS / 回滚 | 新增 §5 | 生产可用性 |

---

## 1. 总体设计

### 1.1 建设目标

1. **Doris 数仓**：以 Apache Doris 4.1.5 为统一存储与计算引擎，按 ODS → DWD → DWS → ADS 四层建设数据标准底座，沉淀统一的命名规范、字段规范、指标字典与质量规则。
2. **DBAPI 平台**：把 ADS 层数据发布为受控 RESTful API，供 BI、大屏及外部系统调用；认证、配额、限流、审计四要素默认开启。
3. **可用性目标**：查询链路（APISIX/DBAPI/Doris FE-BE/PG/Redis）无单点；入湖链路（Flink/Kafka/ZK/SILO）**数据层**无单点——但 **SILO 的客户端入口当前只指向 dw01 单个 endpoint**，属已知单点边界（P0-05，见 §10 风险清单），不得宣称"checkpoint 完全无单点"。PG 主库故障在**同步复制正常、同步 standby 可用且 Patroni 策略满足**的前提下 RPO≈0、RTO ≤ 60s（三台时切换后仍有从库接任同步，降级窗口只有"追平差量"的秒级，见 §6）；**同步节点不可用、网络分区等异常场景下的提交行为取决于同步复制配置，必须通过故障演练验证**，因此本文不再使用"绝对 RPO=0"的表述。

### 1.2 总体架构

```
                      外部系统（BI / 大屏 / 第三方）
                                  │ HTTP(S)
                                  ▼
                  ┌── keepalived VIP 192.168.110.199 ──┐
                  │     （三台同时竞选，priority 150/120/100）      │
                  │                                    │
        APISIX (gw01 .204)   APISIX (gw02 .205)   APISIX (gw03 .206)   ← 三台同读 etcd 同一 prefix
                  │                │                │
        DBAPI  (gw01)     DBAPI  (gw02)     DBAPI  (gw03)
                  │   ── Redis 缓存（1 主 2 从 + 3 哨兵，全在应用层）
                  │   ── PG 元数据库（Patroni 1 主 2 从，VIP .200）
                  │                │                │
                  ▼                ▼                ▼
          Doris FE Leader (dw01 .201) ── Follower (dw02 .202) / Follower (dw03 .203)
                  │
      BE ×3（dw01/02/03，副本 3）
                  ▲
                  │ Routine Load（消费 Kafka）
      Kafka ×3（dw01/02/03）  ←──  Flink CDC(HA)  ←──  业务源库 binlog
                                   │  ▲
                        ZK ×3（选主）│  │ checkpoint / HA 元数据
                                   ▼  │
                                SILO ×3
                  ▲
                  │ 四层加工（DS 批处理：DataX + Doris SQL）
            ODS → DWD → DWS → ADS
```

**两层职责**

| 层 | 节点 | 只做 | 关键约束 |
|---|---|---|---|
| 数据/入湖层 | doris-dw01/02/03 | 写 + 存（Doris、Kafka、Flink、SILO、ZK） | 不对外暴露任何端口，BE/Kafka/ZK/Flink 端口仅层内可达；**v1.11 起不再承载任何应用层组件** |
| 应用服务层 | dbapi-gw01/02/03 | 读 + 出口（APISIX、DBAPI、PG 1主2从、Redis、etcd 3 成员、哨兵 3、keepalived） | 只经 9030 查 Doris，不碰 ODS/DWD 明细、不直连业务库；三台同构，任一台故障不影响职能完整 |
| 独立调度集群 | DolphinScheduler-1/2/3（192.168.110.131~133） | 定时批处理（DataX 抽数、层间 SQL、质量校验） | 不在本平台拓扑内，仅网络对接 |

### 1.3 数据流与分层原则

- **单向依赖**：ODS → DWD → DWS → ADS，上层只依赖下一层，禁止跨层与反向依赖。
- **逐层收敛**：明细不越过 DWD，汇总不越过 DWS，对外只出 ADS。
- **消费隔离**：DBAPI 只读 ADS（经评审可开放部分 DWS），永远不暴露 ODS/DWD。
- **实时只落 ODS**：Flink/Kafka 链路只负责"把变更搬进 ODS"，四层加工全部由 DS 批处理完成——这样实时链路的延迟抖动不会传导到指标口径。

### 1.4 组件职责边界

| 组件 | 给谁用 | 存什么 | 明确不做什么 |
|---|---|---|---|
| **Doris** | DBAPI（读 ADS）、DS（加工） | 业务/BI 数据（四层） | 不存 API 元数据、不存导出文件 |
| **PostgreSQL** | DBAPI | API 定义、授权、调用日志、发布快照 | **不存业务数据** |
| **Redis** | DBAPI | 热查询结果集（TTL）、nonce 防重放 | 不做持久化主存储 |
| **SILO** | DBAPI（导出）+ Flink（checkpoint） | CSV 导出、Flink 状态 | 不做数据库 |
| **Kafka** | Flink CDC → Routine Load | CDC 变更流 | 不做长期存储（保留 48h） |
| **etcd** | APISIX + Patroni | APISIX 全部路由/插件配置；Patroni lease | 不存业务数据、不与 ZK 混用 |
| **ZooKeeper** | Flink HA | JM 选主、HA 元数据 | 不服务 APISIX |
| **DolphinScheduler** | 批处理链路 | 工作流定义与执行历史（自有 MySQL） | 不参与实时链路 |

> **etcd 与 ZooKeeper 为什么不能合并**：两者都是协调组件，但生态与协议不同（etcd=gRPC/Raft KV，ZK=ZAB/树形节点）。APISIX 原生只支持 etcd 作为配置中心，Flink 的 HA 后端以 ZK 最成熟。合并意味着自行改造其中一侧，收益远小于风险。

---

## 2. Doris 数仓详细设计

### 2.1 集群规划（按实机 16C/64G×3）

| 角色 | 节点 | 数量 | 说明 |
|---|---|---|---|
| FE Leader | doris-dw01 (.201) | 1 | 写元数据、执行查询计划 |
| FE Follower | doris-dw02 (.202)、doris-dw03 (.203) | 2 | **参与选举**，BDBJE 三副本 |
| BE | dw01/02/03 | 3 | 每节点 `mem_limit=40%`（25.6G），存储 480G（独占 `/dev/sdb`） |
| 副本数 | — | 3 | `replication_num=3`，与 BE 数一致 |

> **为什么不用 Observer**：Observer 只同步元数据做读扩展，**不参与 Leader 选举**。3 台 FE 里若有 1 台是 Observer，Leader 宕机后只剩 1 个 Follower，达不到多数派（2/3），集群无法选出新 Leader，表现为"Doris 整体不可用"。这是 v1.0 的致命配置错误。Observer 只应在 BE 数量多、读压力大的集群里额外加，不占用最小 HA 配额。

**端口**

| 端口 | 组件 | 用途 | 开放范围 |
|---|---|---|---|
| 8030 | FE | HTTP（Stream Load / Web UI） | 数据层内 + 应用层 + DS |
| 9030 | FE | MySQL 协议（SQL 查询） | 数据层内 + 应用层 + DS |
| 9010 | FE | edit_log（BDBJE） | 仅数据层内 |
| 9020 | FE | thrift RPC | 仅数据层内 |
| 8040 | BE | HTTP（健康检查 / Stream Load 重定向） | 数据层内 + DS |
| 9050 | BE | heartbeat | 仅数据层内 |
| 9060 | BE | thrift | 仅数据层内 |
| 8060 | BE | brpc（副本同步） | 仅数据层内 |

### 2.2 命名规范

| 对象 | 规则 | 示例 |
|---|---|---|
| 库 | `dw_db`（数仓）、`dbapi_db`（PG 元数据） | — |
| 表名 | `{层}_{主题}_{内容}_{周期}`，小写 snake_case | `dws_trade_shop_amt_1d` |
| 字段 | 小写 snake_case，禁拼音与歧义缩写 | `order_amt`、`user_id` |
| 层前缀 | `ods_` / `dwd_` / `dws_` / `ads_` / `dim_` | `dim_user` |
| 周期后缀 | `_1d` 日、`_1w` 周、`_1m` 月、`_nd` 无周期 | `ads_trade_shop_rank_d` |
| Kafka topic | 与目标 ODS 表同名 | `ods_trade_order` |
| Routine Load 作业 | `rl_{目标表名}` | `rl_ods_trade_order` |

> **命名统一（v1.0 遗留问题）**：v1.0 中出现 `ods_trade_order`（表）与 `ods_orders`（topic）两套名字，手册也混用。v1.5 定版：**topic 与表同名** `ods_trade_order`，避免消费端对不上。

### 2.3 字段规范

| 类别 | 规范 | 示例 |
|---|---|---|
| 主键 / ID | BIGINT，禁止 INT | `order_id BIGINT` |
| 金额 | DECIMAL(20,2)，禁 FLOAT/DOUBLE | `order_amt DECIMAL(20,2)` |
| 比率 | DECIMAL(10,4) | `mom_rate DECIMAL(10,4)` |
| 时间 | DATETIME（秒）/ DATE（日期） | `create_time DATETIME` |
| 布尔 | TINYINT，`is_` 前缀 | `is_current TINYINT` |
| 枚举 | TINYINT + 字典表，禁止裸数字入库 | `order_status TINYINT` |
| 字符串 | VARCHAR(n)，最大长度 + 20% 余量 | `shop_name VARCHAR(128)` |
| 必备字段 | `etl_time`（入湖时间）、`op_type`（ODS CDC 操作）、`stat_date/pay_date`（分区日期） | 见各层 DDL |

### 2.4 建表 DDL（**已修复 v1.0 的 3 处语法/语义错误**）

执行前置：`CREATE DATABASE IF NOT EXISTS dw_db;`

#### 2.4.1 ODS 贴源层（Duplicate 模型，按天分区，保留 30 天）

```sql
CREATE TABLE IF NOT EXISTS ods_trade_order (
    order_id      BIGINT         NOT NULL COMMENT '订单ID',
    order_no      VARCHAR(64)    NOT NULL COMMENT '订单编号',
    stat_date     DATE           NOT NULL COMMENT '分区日期(入湖日期；不是下单日期)',
    user_id       BIGINT         COMMENT '用户ID',
    shop_id       BIGINT         COMMENT '店铺ID',
    order_amt     DECIMAL(20,2)  COMMENT '订单总金额',
    order_status  TINYINT        COMMENT '订单状态: 1待付 2已付 3发货 4完成 5取消',
    create_time   DATETIME       COMMENT '下单时间',
    op_type       VARCHAR(8)     COMMENT 'CDC操作: I新增 U更新',
    etl_time      DATETIME       COMMENT '数据入湖时间'
) ENGINE=OLAP
DUPLICATE KEY(order_id, order_no, stat_date)
COMMENT 'ODS-交易订单贴源表'
PARTITION BY RANGE(stat_date) ()
DISTRIBUTED BY HASH(order_id) BUCKETS 10
PROPERTIES (
    "dynamic_partition.enable"    = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start"     = "-30",
    "dynamic_partition.end"       = "3",
    "dynamic_partition.prefix"    = "p",
    "dynamic_partition.buckets"   = "10",
    "replication_num"             = "3"
);
```

> **修复点 1（key 列顺序）**：Doris 要求 KEY 列必须是表的**最前面连续若干列**，且顺序与 `KEY(...)` 一致，同时分区列必须在 KEY 内。v1.0 把 `stat_date` 放在最后一列、却写进 `DUPLICATE KEY(order_id, order_no, stat_date)`，建表会直接报 `Key column should be the first column`。v1.5 把 `stat_date` 提到第 3 列。
>
> **`op_type` 的作用**：承接 Flink CDC 的操作类型，供 DWD 做"取最新版本"与删除对账（见 §2.7.1）。
>
> **⚠️ `stat_date` 必须是"入湖日期"而不是"下单日期"（v1.10 修正，v1.9 及以前是必现故障）**：
> - 旧写法 `stat_date = date_format(create_time,'%Y-%m-%d')` 让分区列取业务时间，而动态分区窗口是 `start=-30 ~ end=+3`。**Flink CDC 首次全量同步历史订单时，一年前的订单会被算进"30 天前"的分区——那个分区早已不存在也不会被创建**，Routine Load 逐条报 `Partition not found`，ErrorRows 迅速超过 `max_error_number=1000`，**作业直接转入 PAUSED，全量永远跑不完**。
> - 这不是概率问题：只要源库存在超过 30 天的历史数据（几乎所有生产库都有），首次接入 100% 触发。
> - 正确做法：**ODS 是贴源缓冲层，按"入湖时间"分区**，`stat_date = curdate()`，任何历史数据都落进今天的分区；业务时间保留在 `create_time` 列，由 DWD 加工时取用。
> - 连带影响：`stat_date` 的"保留 30 天"从此严格等于"最近 30 天入湖的数据"，语义更准确，重跑窗口也是 30 天（与 §2.5 一致）。

#### 2.4.2 DWD 明细层（Unique MoW 模型，保留 90 天）

```sql
CREATE TABLE IF NOT EXISTS dwd_trade_order_detail (
    order_id     BIGINT         NOT NULL COMMENT '订单ID',
    sku_id       BIGINT         NOT NULL COMMENT '商品SKU ID',
    pay_date     DATE           NOT NULL COMMENT '分区日期(支付日期)',
    user_id      BIGINT         COMMENT '用户ID',
    shop_id      BIGINT         COMMENT '店铺ID',
    category_id  BIGINT         COMMENT '末级类目ID',
    sku_amt      DECIMAL(20,2)  COMMENT 'SKU成交金额',
    sku_qty      INT            COMMENT '购买数量',
    pay_time     DATETIME       COMMENT '支付时间',
    is_deleted   TINYINT        COMMENT '删除标记: 1已删除 0正常',
    etl_time     DATETIME       COMMENT '入湖时间'
) ENGINE=OLAP
UNIQUE KEY(order_id, sku_id, pay_date)
COMMENT 'DWD-交易订单明细事实表'
PARTITION BY RANGE(pay_date) ()
DISTRIBUTED BY HASH(order_id) BUCKETS 20
PROPERTIES (
    "dynamic_partition.enable"    = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start"     = "-90",
    "dynamic_partition.end"       = "3",
    "dynamic_partition.prefix"    = "p",
    "dynamic_partition.buckets"   = "20",
    "replication_num"             = "3",
    "enable_unique_key_merge_on_write" = "true"
);
```

> **修复点 2（分布列必须在 key 内）**：v1.0 写 `UNIQUE KEY(order_id, sku_id, pay_date)` 却 `DISTRIBUTED BY HASH(user_id)`。Unique/Aggregate 模型的分布列**必须是 KEY 列的子集**，否则报 `Distribution column should be a key column`。v1.5 改为 `HASH(order_id)`。
>
> `enable_unique_key_merge_on_write=true`（Doris 2.0+ 默认）让 Unique 表在写入时即完成合并，点查性能与 Duplicate 表接近，这也是 DWD 选 MoW 而不是 Aggregate 的原因。

#### 2.4.3 DWS 汇总层（Aggregate 模型，保留 24 个月）

```sql
CREATE TABLE IF NOT EXISTS dws_trade_shop_amt_1d (
    stat_date    DATE           NOT NULL COMMENT '统计日期',
    shop_id      BIGINT         NOT NULL COMMENT '店铺ID',
    category_id  BIGINT         NOT NULL COMMENT '类目ID',
    order_cnt    BIGINT         SUM     COMMENT '订单数',
    sku_cnt      BIGINT         SUM     COMMENT 'SKU件数',
    order_amt    DECIMAL(20,2)  SUM     COMMENT '成交金额',
    user_id_set  BITMAP         BITMAP_UNION COMMENT '下单用户集合(用于去重计数)'
) ENGINE=OLAP
AGGREGATE KEY(stat_date, shop_id, category_id)
COMMENT 'DWS-店铺类目成交日汇总'
PARTITION BY RANGE(stat_date) ()
DISTRIBUTED BY HASH(shop_id) BUCKETS 6
PROPERTIES (
    "dynamic_partition.enable"    = "true",
    "dynamic_partition.time_unit" = "MONTH",   -- v1.10：日汇总表用日分区 → 730 分区 × 10 桶 = 7300 tablet，
    "dynamic_partition.start"     = "-36",     --   而本层单副本仅 ~15GB（≈2MB/tablet），远低于 100MB 建议值，
                                               --   元数据与 Compaction 白白受压。改月分区 + 6 桶（3 BE 上 2/2/2 均匀）
    "dynamic_partition.end"       = "3",
    "dynamic_partition.prefix"    = "p",
    "dynamic_partition.buckets"   = "6",
    "replication_num"             = "3"
);
```

> **修复点 3（去重用户数语义）**：v1.0 用 `user_cnt BIGINT SUM` 表示"下单用户数"，SUM 只能累加下单次数、无法去重，指标必然虚高。v1.5 改为 **BITMAP + BITMAP_UNION**，查询用 `bitmap_union_count(user_id_set)` 得到真实 UV。
>
> 写入方式：`INSERT INTO dws_trade_shop_amt_1d SELECT stat_date, shop_id, category_id, count(*), sum(sku_qty), sum(sku_amt), bitmap_union(to_bitmap(user_id)) FROM ... GROUP BY 1,2,3;`

#### 2.4.4 ADS 应用层（Duplicate 模型，面向 API/BI 的宽表）

```sql
CREATE TABLE IF NOT EXISTS ads_trade_shop_rank_d (
    stat_date    DATE           NOT NULL COMMENT '统计日期',
    shop_id      BIGINT         NOT NULL COMMENT '店铺ID',
    shop_name    VARCHAR(128)   COMMENT '店铺名称',
    order_cnt    BIGINT         COMMENT '订单数',
    order_amt    DECIMAL(20,2)  COMMENT '成交金额',
    user_cnt     BIGINT         COMMENT '下单用户数(来源 bitmap 去重结果)',
    rank_no      INT            COMMENT '当日排名',
    mom_rate     DECIMAL(10,4)  COMMENT '成交金额环比',
    update_time  DATETIME       COMMENT '更新时间'
) ENGINE=OLAP
DUPLICATE KEY(stat_date, shop_id)
COMMENT 'ADS-店铺成交排行日报(API取数)'
PARTITION BY RANGE(stat_date) ()
DISTRIBUTED BY HASH(shop_id) BUCKETS 6
PROPERTIES (
    "dynamic_partition.enable"    = "true",
    "dynamic_partition.time_unit" = "MONTH",   -- v1.10：日汇总表用日分区 → 730 分区 × 10 桶 = 7300 tablet，
    "dynamic_partition.start"     = "-36",     --   而本层单副本仅 ~15GB（≈2MB/tablet），远低于 100MB 建议值，
                                               --   元数据与 Compaction 白白受压。改月分区 + 6 桶（3 BE 上 2/2/2 均匀）
    "dynamic_partition.end"       = "3",
    "dynamic_partition.prefix"    = "p",
    "dynamic_partition.buckets"   = "6",
    "replication_num"             = "3"
);
```

> ADS 用 Duplicate 而非 Unique：DS 每天**整分区覆盖写**（`INSERT OVERWRITE`），天然幂等，Duplicate 写入更快且无合并开销。

#### 2.4.5 维表（Unique MoW，全量更新，不分区）

```sql
CREATE TABLE IF NOT EXISTS dim_user (
    user_id       BIGINT      NOT NULL COMMENT '用户ID',
    user_name     VARCHAR(64) COMMENT '用户名',
    phone         VARCHAR(32) COMMENT '手机号(脱敏存储)',
    city          VARCHAR(64) COMMENT '常驻城市',
    register_time DATETIME    COMMENT '注册时间',
    update_time   DATETIME    COMMENT '更新时间'
) ENGINE=OLAP
UNIQUE KEY(user_id)
COMMENT 'DIM-用户维表'
DISTRIBUTED BY HASH(user_id) BUCKETS 10
PROPERTIES ("replication_num" = "3", "enable_unique_key_merge_on_write" = "true");
```

#### 2.4.6 拉链维表（Unique MoW + SCD2）

```sql
CREATE TABLE IF NOT EXISTS dim_user_scd2 (
    user_id    BIGINT      NOT NULL COMMENT '用户ID',
    start_date DATE        NOT NULL COMMENT '版本生效日期',
    user_name  VARCHAR(64) COMMENT '用户名',
    city       VARCHAR(64) COMMENT '常驻城市',
    end_date   DATE        COMMENT '版本失效日期(NULL=当前)',
    is_current TINYINT     COMMENT '是否当前版本: 1是 0否'
) ENGINE=OLAP
UNIQUE KEY(user_id, start_date)
COMMENT 'DIM-用户拉链表(SCD2)'
DISTRIBUTED BY HASH(user_id) BUCKETS 10
PROPERTIES ("replication_num" = "3", "enable_unique_key_merge_on_write" = "true");
```

查询当前版本：`WHERE user_id = ? AND is_current = 1`；查询历史时点：`WHERE user_id = ? AND start_date <= '2026-01-01' AND (end_date IS NULL OR end_date > '2026-01-01')`。

### 2.5 分区与分桶策略

| 项 | 策略 |
|---|---|
| 分区 | RANGE(日期) + 动态分区，Doris 自动建/删 |
| ODS 分区键 | **入湖日期 `curdate()`**（不是业务日期，见 §2.4.1） |
| ODS 保留 | 30 天（可回溯重跑近 1 个月） |
| DWD 保留 | 90 天；加工时须过滤 `pay_date >= curdate()-90` |
| DWS / ADS | **月分区**（`time_unit=MONTH`, `start=-36`）+ 6 桶，保留 36 个月；日分区会产生 730 个分区 × 10 桶 = 7300 tablet 而本层仅 ~15GB，tablet 过小反压元数据 |
| 分桶列 | 高基数列：`order_id` / `shop_id` / `user_id`，且必须在 KEY 内 |
| 单桶目标 | 100MB ~ 1GB；按预估量 ÷ 目标桶大小，预留 30% 增长 |
| 分桶数参考 | 10~20 起步；单表 > 100GB 时按 1GB/桶上调 |
| 小维表 | 无合适分桶列时 `DISTRIBUTED BY RANDOM BUCKETS 10` |

> 当前 3 BE × 480G，单表 10~20 桶在 3 个 BE 上分布均匀（桶数不必是 BE 数的整数倍，Doris 会按桶调度）。桶数过大会推高元数据与 Compaction 压力，**在 64G 小内存机器上宁少勿多**。

### 2.6 数据质量规则

| 规则 | 对象 | 阈值 | 处置 |
|---|---|---|---|
| 主键唯一性 | DWD/DWS/ADS | 重复行 = 0 | 阻断下游任务 |
| 非空率 | 关键字段（id / 金额 / 时间） | ≥ 99.9% | 告警 + 人工确认 |
| 值域校验 | 枚举字段 | 100% 命中字典表 | 告警 |
| 行数波动 | 每层每任务 | 环比降幅 > 50% | 告警 + 阻断 |
| 指标波动 | DWS/ADS 核心指标 | 环比 > ±30% | 告警 + 人工确认 |
| 及时性 | 实时链路 | Routine Load Lag > 5 分钟 | 告警 |
| 一致性 | ODS vs 源库（按日抽查） | 主键数差异 < 0.1% | 告警并触发补数 |

规则由 DS 在每个任务节点后置校验卡点执行，失败即阻断并通知值班。

### 2.7 集成链路

#### 2.7.1 实时链路：Flink CDC → Kafka → Routine Load

**链路定义**

| 跳 | 组件 | 位置 | 关键配置 |
|---|---|---|---|
| ① | 业务源库（MySQL） | 外部 | CDC 只读账号，`binlog_format=ROW`、`binlog_row_image=FULL` |
| ② | Flink CDC（HA） | dw01/02/03，1 JM + 1 TM/台 | ZK 选主；checkpoint 30s，落 SILO |
| ③ | Kafka ×3 | dw01/02/03 :9092 | topic 3 分区、副本 3、`min.insync.replicas=2`、lz4 |
| ④ | Doris Routine Load | FE 作业 + BE 并行消费 | 并发 3，`max_batch_interval=10`（秒） |
| ⑤ | ODS `ods_trade_order` | dw01/02/03（副本 3） | Duplicate 模型，`op_type` 记录 I/U |

**Flink SQL（提交到 HA 集群）**

```sql
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
-- 提交后与客户端解绑，否则关闭 sql-client 终端会把流式作业一起取消
SET 'execution.attached' = 'false';

CREATE TABLE mysql_orders (
  order_id     BIGINT,
  order_no     STRING,
  user_id      BIGINT,
  shop_id      BIGINT,
  order_amt    DECIMAL(20,2),
  order_status INT,
  create_time  TIMESTAMP(3),
  op           STRING METADATA FROM 'op' VIRTUAL,   -- binlog 操作类型：+I/+U/-U/-D（写出前归一化为 I/U）
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector'='mysql-cdc','hostname'='<源库IP>','port'='3306',
  'username'='<cdc用户>','password'='<密码>',
  'database-name'='<库>','table-name'='orders',
  'server-time-zone'='Asia/Shanghai',
  'scan.incremental.snapshot.enabled'='true'
);

-- 列顺序必须与 ods_trade_order 前 8 列（除 stat_date/etl_time）及 Routine Load 的 jsonpaths 一致
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
  'properties.acks'='all',
  'properties.enable.idempotence'='true',
  'properties.transaction.timeout.ms'='900000',
  'properties.max.in.flight.requests.per.connection'='5'
);

INSERT INTO kafka_orders
SELECT order_id, order_no, user_id, shop_id, order_amt, order_status, create_time,
       CASE WHEN op = '+I' THEN 'I' ELSE 'U' END AS op   -- CDC 取值是 +I/+U/-U/-D，归一化为 I/U
FROM mysql_orders;
```

> **字段必须与 ODS 表对齐**：`stat_date` / `etl_time` 由 Routine Load 侧计算，不从 Kafka 取；其余 8 个字段与 `ods_trade_order` DDL 及 `jsonpaths` 顺序一一对应。早期草稿用 `id/cust_id/amount/ts`，会让 Routine Load 的 `jsonpaths` 全部落空（作业 RUNNING 但 ErrorRows 持续增长或数据全为 NULL）。
>
> **`op` 必须归一化**：Flink MySQL CDC 的 `op` metadata 取值是 **`+I` / `+U` / `-U` / `-D`**，直接写出会让 ODS 的 `op_type` 全是 `+I/+U`，DWD 取最新与 DS 对账按 `I` 过滤时全部落空。INSERT 里用 `CASE` 转成 `I/U`（`-D` 走 tombstone，不会到这里）。
>
> **`properties.acks=all` + 幂等**：Kafka sink 默认 `acks=1`，broker 抖动存在丢消息窗口；CDC 是唯一数据源，丢了就是永久缺口。幂等生产者要求 `acks=all` 且 `max.in.flight≤5`，三条一起写才生效。
>
> **DELETE 语义**：`upsert-kafka` 对 `-D` 写 tombstone（value=null），Routine Load 会跳过 → ODS 只承接 I/U，删除由 DWD 每日对账置 `is_deleted=1`（见 §2.7.2）。

**Routine Load（Doris 侧）**

```sql
CREATE ROUTINE LOAD dw_db.rl_ods_trade_order ON ods_trade_order
COLUMNS(
    order_id, order_no, user_id, shop_id, order_amt, order_status, create_time, op_type,
    stat_date = curdate(),
    etl_time  = now()
)
PROPERTIES (
    "desired_concurrent_number" = "3",
    "max_batch_interval"        = "10",        -- 单位秒，写 "10s" 会报错
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
```

监控：`SHOW ROUTINE LOAD FOR dw_db.rl_ods_trade_order\G`，关注 `State=RUNNING`、`Lag`、`ErrorRows`、`TotalRows`。

> **`stat_date = curdate()` 而不是 `date_format(create_time,...)`（v1.10）**：ODS 按入湖日期分区，历史订单才不会撞上"分区不存在"（原因见 §2.4.1）。`curdate()` 是 Doris 内置函数，Routine Load 的 `COLUMNS` 计算列支持。
>
> **`strict_mode=false` 的监控假象（必须知道）**：非严格模式下，类型转换失败**不会**计入 `ErrorRows`——它会静默把该字段写成 `NULL` 并继续导入。所以 `ErrorRows=0` **并不代表数据干净**，只代表 JSON 能解析、字段能对上。真正的脏数据兜底是 DWD 层的非空率/值域校验（§2.6），以及下面这条日巡检：
> ```sql
> -- 每日跑一次，任一字段为空率 > 0.1% 即告警（这才是 strict_mode=false 下的真实防线）
> SELECT count(*) AS total,
>        sum(order_id IS NULL)    AS null_id,
>        sum(order_amt IS NULL)   AS null_amt,
>        sum(create_time IS NULL) AS null_ctime
> FROM dw_db.ods_trade_order WHERE stat_date = curdate() - 1;
> ```

> ⚠️ **`OFFSET_BEGINNING` 只在首次初始化时使用**：ODS 是 Duplicate 模型，同一批消息消费两次就是两行。首次建作业时 Kafka 通常无历史（或需要回灌），用它是合理的；**重建/重提作业必须改成 `OFFSET_END`**，或用 `ALTER ROUTINE LOAD ... FROM KAFKA ("kafka_partitions"="0,1,2","kafka_offsets"="OFFSET_END")` 跳过存量，否则整段重灌、报表翻倍。自查：`SELECT order_id, count(*) FROM ods_trade_order GROUP BY order_id HAVING count(*)>1`。

**DWD 加工必须过滤超出保留期的业务日期（v1.10 新增约束）**

ODS 按入湖日期分区后，历史订单也进了今天的 ODS 分区。DS 往 DWD 写的时候，如果直接把 `create_time` 当作 `pay_date`，一年前的订单会落到 DWD `-90` 天以前的分区——同样报 `Partition not found`，Stream Load 任务失败。

所以 **DWD 的加工 SQL 一律加业务日期下界**：

```sql
INSERT INTO dw_db.dwd_trade_order_detail
SELECT order_id, sku_id, date(create_time) AS pay_date, user_id, shop_id, ... 
FROM dw_db.ods_trade_order
WHERE stat_date = curdate() - 1                    -- 只扫昨天的入湖分区
  AND date(create_time) >= curdate() - 90          -- ★ 业务日期收敛到 DWD 保留窗口（-90 天）
  AND op_type IN ('I','U');
```

> 语义边界要跟业务说明白：**DWD 只保留近 90 天明细**，更早的历史订单不入 DWD（ODS 也留不住）。若业务确需长周期明细，只能放宽 `dynamic_partition.start` 并同步重算磁盘账本（§2.8），不能靠"跳过报错"解决。

#### 2.7.2 **DELETE 语义的边界（v1.0 的隐藏缺陷，必须明确）**

`upsert-kafka` 对 `-D` 写入的是 **tombstone（value=null）**，Doris Routine Load 消费到空消息会**跳过**——结果是：源库删了一条订单，ODS 永远不知道，DWD 里这条数据一直存在。

v1.5 采用**分层解决**（不引入自定义代码，可靠性可控）：

| 层 | 做法 |
|---|---|
| ODS | 只承接 **I / U**（`op_type` 记录类型）。空消息被 Routine Load 跳过，属预期行为 |
| DWD | Unique MoW 模型 + `is_deleted` 标记。DS 每日跑"删除对账"任务：DataX 抽源库当日主键快照 → 与 DWD 当日分区比对 → 源库已不存在的主键 `UPDATE is_deleted=1` |
| DWS/ADS | 汇总时统一 `WHERE is_deleted = 0`，删除不影响历史分区（已发布的历史指标不回溯，符合财务口径） |

删除对账 SQL 骨架（DS 任务）：

```sql
-- 1) 源库快照落到临时表 tmp_src_order_pk（DataX 抽取）
-- 2) 标记删除
UPDATE dwd_trade_order_detail d
SET is_deleted = 1, etl_time = now()
WHERE d.pay_date = '${bizdate}'
  AND d.is_deleted = 0
  AND NOT EXISTS (SELECT 1 FROM tmp_src_order_pk s WHERE s.order_id = d.order_id);
```

> **为什么不在 Flink 侧保留删除**：要保留删除就必须绕开 Kafka 的 tombstone 语义，只能自定义 `KafkaSerializationSchema` 输出 `{"op":"D","id":...}` 的普通消息，再在 Routine Load 里 `op_type` 过滤 + DWD 落软删。方案可行但引入自研代码与版本维护成本，收益仅是"删除延迟从 T+1 降到秒级"。**当前业务口径（T+1 报表）用不上秒级删除**，故不采用。若后续有实时删除需求（如风控名单），再按此方案升级。

#### 2.7.3 离线链路：DataX → Stream Load

```json
{
  "job": {
    "setting": { "speed": { "channel": 3 }, "errorLimit": { "record": 0, "percentage": 0.02 } },
    "content": [{
      "reader": {
        "name": "mysqlreader",
        "parameter": {
          "username": "reader",
          "password": "******",
          "column": ["order_id","order_no","user_id","shop_id","order_amt","order_status","create_time"],
          "where": "create_time >= '${bizdate} 00:00:00' AND create_time < '${tomorrow} 00:00:00'",
          "connection": [{ "jdbcUrl": ["jdbc:mysql://<源库IP>:3306/biz_db"], "table": ["trade_order"] }]
        }
      },
      "writer": {
        "name": "doriswriter",
        "parameter": {
          "feCnns": ["192.168.110.201:8030"],
          "jdbcUrl": "jdbc:mysql://192.168.110.201:9030/dw_db",
          "database": "dw_db",
          "table": "ods_trade_order",
          "column": ["order_id","order_no","curdate()","user_id","shop_id","order_amt","order_status","create_time","'I'","now()"],
          "loadProps": { "format": "json", "strip_outer_array": true, "strict_mode": false }
        }
      }
    }]
  }
}
```

> **⚠️ writer 的 `column` 是"按表列顺序填值"，不是"按列名映射"（v1.10 修正，旧写法会把 10 列全部错位）**
>
> `doriswriter` 的 `column` 数组按**位置**对应目标表的列顺序。`ods_trade_order` 的列序是：
> ```
> 1 order_id      2 order_no      3 stat_date     4 user_id       5 shop_id
> 6 order_amt     7 order_status  8 create_time   9 op_type      10 etl_time
> ```
> 旧写法按"reader 读出的顺序 + 3 个函数"拼成 `order_id, order_no, user_id, shop_id, order_amt, order_status, create_time, 'I', now(), date_format(...)`，逐位比对就是：
>
> | 位置 | 旧写法给的值 | 实际落到哪一列 | 结果 |
> |---|---|---|---|
> | 3 | `user_id`（BIGINT） | **stat_date（DATE）** | 时间戳被当日期解析，失败写 NULL |
> | 4 | `shop_id` | user_id | 店铺 ID 进了用户 ID |
> | 5 | `order_amt` | shop_id | 金额进了店铺 ID |
> | 6 | `order_status` | order_amt | 状态码进了金额 |
> | 7 | `create_time` | order_status | 时间进了状态 |
> | 8 | `'I'` | create_time | 字符串进 DATETIME → NULL |
> | 9 | `now()` | op_type | 时间进操作类型 |
> | 10 | `date_format(...)` | etl_time | —— |
>
> 叠加 `strict_mode=false` 与 `errorLimit.record=0` 只兜 Flink 侧错误 → **离线补数写进去的全是错位数据，且不告警**。这类错误比"报错"危险一个数量级：报表能出数，只是全错。
>
> **修正后**：`curdate()` 放在第 3 位（对应 `stat_date`），其余按表列顺序排；同时把 `date_format(create_time,...)` 换成 `curdate()`，避免 doriswriter 引用源列做函数运算（不同版本支持不一致）。
>
> **自检方法**：补数跑完后立刻抽样 `SELECT * FROM ods_trade_order WHERE stat_date=curdate() AND op_type='I' LIMIT 5`，重点看 `shop_id` 是否等于源库值、`order_amt` 是否是金额量级。若 `order_status` 出现日期、`order_amt` 出现个位数，就是又错位了。

> 批处理补数通道与实时链路**写同一张 ODS 表**：实时槽位负责增量，DataX 负责 T+1 校验与历史补数。两者都写 Duplicate 表，重复数据由 DWD 的 Unique 模型 + 取最新 `etl_time` 收敛，不会污染结果。

#### 2.7.4 调度编排（DolphinScheduler）

```
[实时链路自检] → [ODS 补数/对账] → [DWD 清洗(去重取最新)] → [DWD 删除对账] →
[DWS 汇总] → [ADS 宽表] → [质量校验] → [成功/告警]
```

每个节点重试 3 次、超时按历史耗时 2 倍设置；失败通知到值班群。**质量校验失败必须阻断下游**（不能产出"半成品"ADS）。

### 2.8 容量规划（按 3 × 480G BE）

**公式**：集群裸盘需求 = Σ(原始量 × 压缩比 0.2~0.35) × 副本 3 × 冗余 1.2

以原始数据 500GB 为例：

| 层 | 原始量 | 压缩后单副本 | ×3 副本 | 保留 |
|---|---|---|---|---|
| ODS | 500 GB | ~150 GB | 450 GB | 30 天滚动 |
| DWD | 400 GB | ~120 GB | 360 GB | 90 天 |
| DWS | ≈1/10 | ~15 GB | 45 GB | 24 个月 |
| ADS | ≈1/20 | ~10 GB | 30 GB | 24 个月 |
| **合计** | — | — | **≈885 GB** | 每节点 ≈295 GB |

**实机能力与两个不同性质的水位（v1.10 修正：旧稿只算了稳态，漏了故障重建）**

BE 480G × 3 节点 = 1440G 裸盘 ÷ 3 副本 = **480G 单副本容量**。但"能装下"和"坏了能自愈"是两件事，必须分开看：

| 水位 | 计算 | 数值 | 含义 |
|---|---|---|---|
| 稳态上限 | 1440 ÷ 3 | 480G 单副本 | 三副本全健康时的理论容量 |
| **故障自愈上限** | 单节点 ≤ 480 × 0.8（留 20% 给 compaction/元数据）÷ 1.5 | **256G 单副本** | **单 BE 宕机后，剩余 2 节点要扛 3 份副本（每节点 1.5 倍），仍能完成副本补齐** |

**为什么 1.5 倍**：3 副本分布在 3 节点时每节点持有 1 份；任一节点故障后，Doris 会把缺失的副本按剩余 2 节点均摊补回，于是每节点变成 1.5 份。若此时磁盘已用超 80%，**补齐任务会因 `Disk usage exceeds` 持续失败**，集群长期停在 2 副本状态——再坏一台就直接丢数据，而且期间任何 compaction 都可能被磁盘压力拖死。

**结论（按上表 885GB 多副本 / 295G 单副本测算）**：

- 当前的 295G/节点（61%）**稳态可运行，但已越过 256G 的故障自愈线**，即"能跑，但坏一块盘补不回来"。
- 若要求"单盘故障可自愈"，对应原始数据量上限约 **430GB**（885G × 256/295 ≈ 768G 多副本）。
- 落在 256G ~ 336G 之间时，必须准备好**应急脚本**：故障发生立即执行 ① 临时把非关键表 `ALTER TABLE ... MODIFY PARTITION (*) SET ("replication_num"="2")`；② 缩短 ODS 保留到 7 天；③ 释放空间后再逐步恢复 3 副本。
- 超过 336G（70%）则稳态本身就危险，必须加盘或缩短保留期。

> **扩容判据请用两条，而不是一条**：① `SHOW DATA` 单副本总量 > **768G**（自愈线）；② 任一 BE 磁盘 > **53%**。达到任一条即进入扩容流程，触发动作见《硬件配置与参数调优》§9。

**统一运行规则表（v1.12 新增，P1-13：把 256G/295G/336G/768G/53% 五个数字收敛为一套口径）**

先说明换算关系，否则这些数字看起来像互相矛盾：**单副本总量 = 单节点用量 × 3**（3 副本均摊到 3 节点）。所以「单节点 256G」=「单副本 768G」=「磁盘 53%」（256/480），是**同一道自愈线的三种表述**；295G 是当前估算值（61%）、336G 是 70% 风险线，二者都是"已经越过自愈线之后"的位置。

| 单节点用量 | 等价单副本总量 | 磁盘占比 | 定义 | 动作 |
|---|---|---|---|---|
| ≤256G | ≤768G | ≤53% | **正常**：单盘故障后能完成副本补齐 | 正常运行，按季度复核 |
| 256G ~ 336G | 768G ~ 1008G | 53% ~ 70% | **风险区**：日常能跑，但**单盘故障补不回来** | 启动扩容规划；ODS 保留期缩到 7 天；备好应急脚本（见下） |
| >336G | >1008G | >70% | **高风险**：稳态本身就危险 | 立即扩容或缩容，不得继续上线新表 |

- **当前估算 295G/节点（61%）落在"风险区"**：稳态可运行，但按故障自愈口径已经越线，即"能跑，但坏一块盘补不回来"。
- **不要混用两类阈值**：`53%`/`70%` 是"磁盘占用"视角，`256G`/`768G` 是"故障自愈能力"视角。**日常巡检看百分比，容量规划与扩容决策看单副本总量**——只看 70% 会误以为 61% 很安全，实际自愈能力早已丧失。
- **风险区应急脚本**（故障发生立即执行）：① 非关键表临时降至 2 副本 `ALTER TABLE ... MODIFY PARTITION (*) SET ("replication_num"="2")`；② ODS 保留期临时缩到 7 天；③ 空间释放后再逐步恢复 3 副本。

> 压缩比按数据特征浮动（文本高、数字低），上线一周后用 `SHOW DATA` 实测校准分桶与保留策略。触发扩容的阈值见《硬件配置与参数调优》§9。

---

## 3. DBAPI 详细设计

### 3.1 技术栈

| 组件 | 选型 | 说明 |
|---|---|---|
| 网关 | Apache APISIX **3.19.0** | 动态路由、限流、key-auth、prometheus；配置存 etcd |
| 服务层 | Spring Boot 3.2 + **Java 21** + MyBatis-Plus | AlmaLinux 10 系统仓库只有 Java 21/25，Doris 单独用 Temurin 17 |
| 缓存 | Redis 7（主从 + **3 哨兵**） | 热查询缓存、nonce 防重放 |
| 元数据库 | PostgreSQL 17（**Patroni 4.1.5 托管**，PGDG rpm 宿主机安装） | API 定义、授权、调用日志；`synchronous_mode=true` |
| 对象存储 | SILO（3 节点 × 2 目录，S3 兼容） | 异步导出文件；同时承载 Flink checkpoint |
| 注册配置 | 不引入 Nacos | 两个实例 + 环境变量/profile 足够，减少一个需维护的组件 |

> v1.0 写了"Spring Boot 3.2 + Java 17"与本手册的系统 JDK 冲突；v1.5 统一为 **Java 21**（Spring Boot 3.2 官方支持 21）。Doris 的 Java 17 是**组件级隔离**，不影响系统默认版本。

### 3.2 接口规范

**Base URL**：`http://dbapi.<域名>/v1`（生产经 APISIX 挂 TLS 后为 `https://`）

**认证 Header**

| Header | 说明 |
|---|---|
| X-Api-Key | 应用 Key（`api_app.app_key`） |
| X-Timestamp | 毫秒时间戳，服务端容忍 ±5 分钟 |
| X-Nonce | 16 位随机串，防重放 |
| X-Sign | HMAC-SHA256 签名，十六进制小写 |

签名：

```
sign = HMAC_SHA256(app_secret, app_key + "|" + timestamp + "|" + nonce + "|" + md5(body)).to_hex_lower()
```

GET 请求 body 为空串。服务端校验顺序：时间窗 → nonce（Redis `SETNX`，TTL 5 分钟）→ 签名 → IP 白名单 → 配额。

**统一响应体**

```json
{
  "code": 0,
  "message": "success",
  "request_id": "req_01HQ8Z...",
  "data": { "total": 20, "rows": [ { "shop_id": 1001, "order_amt": "99.50" } ] }
}
```

**错误码**

| code | HTTP | 含义 |
|---|---|---|
| 0 | 200 | 成功 |
| 40001 | 400 | 参数缺失或类型错误 |
| 40101 | 401 | 签名错误 / 未授权 |
| 40102 | 401 | nonce 重放 |
| 40301 | 403 | IP 不在白名单 |
| 40401 | 404 | API 不存在或已下线 |
| 42901 | 429 | 超出 QPS 配额 |
| 42902 | 429 | 超出日调用配额 |
| 50001 | 500 | 服务内部错误 |
| 50301 | 503 | Doris 不可用（熔断中） |

### 3.3 接口示例

**店铺成交排行（GET，点查，走缓存）**

```
GET /v1/trade/shop-rank?stat_date=2026-10-12&limit=20
```

绑定 SQL 模板（**只允许 `#{}` 占位，禁止字符串拼接**）：

```sql
SELECT shop_id, shop_name, order_amt, order_cnt, rank_no
FROM ads_trade_shop_rank_d
WHERE stat_date = #{stat_date} AND rank_no <= #{limit}
ORDER BY rank_no
```

**复杂条件（POST）**

```
POST /v1/trade/shop-rank
Content-Type: application/json

{ "stat_date": "2026-10-12", "category_id": 88, "min_amt": 1000 }
```

发布时配置：入参定义（名称/类型/必填/默认值/上限）、出参定义（字段/类型/脱敏规则）、`cache_ttl`、`max_limit`。

### 3.4 核心表 DDL（PostgreSQL 17；Patroni 4.1.5 托管，兼容 16 语法）

```sql
-- 建库（v1.0 缺失，导致后续导入全部失败）
CREATE DATABASE dbapi_db WITH ENCODING 'UTF8' TEMPLATE template0;
\c dbapi_db;

-- 1. API 定义主表
CREATE TABLE api_def (
    id           BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    api_code     VARCHAR(64)  NOT NULL,
    name         VARCHAR(128) NOT NULL,
    object_type  VARCHAR(16)  NOT NULL,          -- DATASET / TABLE / SQL
    data_object  VARCHAR(256) NOT NULL,          -- Doris 表名 / BI 数据集编码
    sql_template TEXT         NOT NULL,
    http_method  VARCHAR(8)   NOT NULL DEFAULT 'GET',
    cache_ttl    INT          NOT NULL DEFAULT 300,
    max_limit    INT          NOT NULL DEFAULT 5000,
    status       VARCHAR(16)  NOT NULL DEFAULT 'DRAFT',  -- DRAFT/TESTING/ONLINE/OFFLINE
    version      INT          NOT NULL DEFAULT 1,
    created_by   VARCHAR(64),
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uk_api_def_code ON api_def (api_code, version);

-- 2. 入参定义
CREATE TABLE api_param (
    id            BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    api_id        BIGINT      NOT NULL,
    param_name    VARCHAR(64) NOT NULL,
    param_type    VARCHAR(16) NOT NULL,          -- string/int/date/decimal
    required      BOOLEAN     NOT NULL DEFAULT false,
    max_value     VARCHAR(64),                   -- 上限，防全表扫描
    default_value VARCHAR(256),
    description   VARCHAR(256)
);
CREATE INDEX idx_api_param_api ON api_param (api_id);

-- 3. 出参与脱敏
CREATE TABLE api_field (
    id           BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    api_id       BIGINT       NOT NULL,
    field_name   VARCHAR(64)  NOT NULL,
    source_field VARCHAR(128) NOT NULL,
    data_type    VARCHAR(16)  NOT NULL,
    mask_rule    VARCHAR(32),                    -- phone/id_card/email/name/空
    description  VARCHAR(256)
);
CREATE INDEX idx_api_field_api ON api_field (api_id);

-- 4. 调用方注册
CREATE TABLE api_app (
    id           BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    app_name     VARCHAR(128) NOT NULL,
    app_key      VARCHAR(64)  NOT NULL,
    app_secret   VARCHAR(128) NOT NULL,          -- AES-256-GCM 加密存储
    qps_quota    INT          NOT NULL DEFAULT 10,
    day_quota    BIGINT       NOT NULL DEFAULT 100000,
    ip_whitelist TEXT,
    status       VARCHAR(16)  NOT NULL DEFAULT 'ACTIVE',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uk_api_app_key ON api_app (app_key);

-- 5. 授权关系（字段级 / 行级）
CREATE TABLE api_auth (
    id          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    api_id      BIGINT      NOT NULL,
    app_id      BIGINT      NOT NULL,
    field_perm  JSONB       NOT NULL DEFAULT '[]',
    row_filter  VARCHAR(512),
    granted_by  VARCHAR(64),
    granted_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uk_api_auth ON api_auth (api_id, app_id);

-- 6. 调用日志（按月 RANGE 分区，保留 90 天）
-- 修复点：分区表的主键必须包含分区列，v1.0 只写 PRIMARY KEY(id) 会报
--         "unique constraint on partitioned table must include all partitioning columns"
CREATE TABLE api_call_log (
    id         BIGINT      GENERATED ALWAYS AS IDENTITY,
    called_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_id     BIGINT      NOT NULL,
    app_id     BIGINT      NOT NULL,
    request_id VARCHAR(64) NOT NULL,
    req_hash   VARCHAR(64),
    rows_cnt   INT,
    latency_ms INT,
    status     VARCHAR(16) NOT NULL,
    error_code VARCHAR(16),
    client_ip  VARCHAR(64),
    PRIMARY KEY (id, called_at)
) PARTITION BY RANGE (called_at);

CREATE INDEX idx_call_log_api_time  ON api_call_log (api_id, called_at);
CREATE INDEX idx_call_log_app_time  ON api_call_log (app_id, called_at);
CREATE INDEX idx_call_log_status    ON api_call_log (status, called_at);

-- 建分区（首次建两个月，之后由 DS 月度任务预建）
CREATE TABLE api_call_log_2026_10 PARTITION OF api_call_log
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE api_call_log_2026_11 PARTITION OF api_call_log
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');

-- 7. 版本快照
CREATE TABLE api_version (
    id           BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    api_id       BIGINT      NOT NULL,
    version      INT         NOT NULL,
    snapshot     JSONB       NOT NULL,
    published_by VARCHAR(64),
    published_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uk_api_version ON api_version (api_id, version);

-- 8. 操作审计（v1.0 只提"审计表"未给结构）
CREATE TABLE api_audit (
    id         BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    actor      VARCHAR(64) NOT NULL,
    action     VARCHAR(32) NOT NULL,             -- PUBLISH/GRANT/QUOTA/KEY_ROTATE
    target     VARCHAR(128),
    detail     JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_api_audit_time ON api_audit (created_at);
```

**日志分区月度维护（DS 工作流，每月 1 日）**

```sql
-- ① 预建下月分区（幂等）
DO $$
DECLARE
  m  date := date_trunc('month', now() + interval '1 month');
  nm text := 'api_call_log_' || to_char(m, 'YYYY_MM');
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = nm) THEN
    EXECUTE format('CREATE TABLE %I PARTITION OF api_call_log FOR VALUES FROM (%L) TO (%L)',
                   nm, m, m + interval '1 month');
  END IF;
END $$;

-- ② 删除 90 天前的分区（秒级，不锁主表，不产生 VACUUM 压力）
DROP TABLE IF EXISTS api_call_log_2026_07;
```

### 3.5 安全设计

1. **SQL 注入防护**：模板只允许 `#{param}` 占位，MyBatis 预编译；执行前强制校验语句以 `SELECT` 开头；自动追加 `LIMIT #{max_limit}`。
2. **字段级权限**：按 `api_auth.field_perm` 裁剪出参，未授权字段**不返回**（而非置空）。
3. **行级权限**：服务端把 `row_filter` 拼入 WHERE（如 `shop_id IN (SELECT shop_id FROM app_shop_scope WHERE app_id = #{ctx.app_id})`），调用方无法绕过。
4. **脱敏**：phone → `138****1234`；id_card → 保留前 4 后 2；email → 首字符 + `***` + 域名；name → 姓 + `*`。
5. **密钥**：`app_secret` 以 AES-256-GCM 加密存储，控制台只展示一次；支持一键轮换（旧密钥 24h 过渡）。
6. **日志**：调用日志保留 90 天（§3.4 分区策略）；发布/授权/配额变更落 `api_audit`。
7. **Admin API 保护**：APISIX Admin 9180 **只对 `127.0.0.1`、`192.168.110.204`、`192.168.110.205`、`192.168.110.206` 开放**（v1.9 收窄：原写 `192.168.110.0/24`，而 DS 集群与办公终端同在此网段，9180 可增删路由/上游，暴露面必须按主机收；v1.11 增补第三台），并启用强随机 `admin_key`。

### 3.6 性能设计

| 项 | 设计 |
|---|---|
| 缓存 key | `dbapi:{api_code}:{sha256(归一化参数)}`，TTL = `api_def.cache_ttl`（默认 300s） |
| 缓存失效 | 定时比对 ADS 表 `max(update_time)`，变化则按 `api_code` 前缀批量删除 |
| 连接池 | HikariCP：`maximum-pool-size 20`、`minimum-idle 5`、`connection-timeout 3000ms` |
| 总连接 | 3 实例 × 25 = 75（PG `max_connections=300`，余量给 Patroni 复制、DS 与运维） |
| 预热 | 高频 API（Top 20）上线时预生成缓存 |
| 降级 | Doris 异常时返回陈旧缓存并标记 `stale=true`（接口级开关） |
| 异步导出 | 预估行数 > 100,000 转异步：落 SILO → 返回 task_id → 完成后签名 URL（24h） |
| 大查询防护 | 必填日期条件 + `max_limit` + Doris `query_timeout=300` + APISIX `limit-req` 三层 |

### 3.7 异步导出流程

```
客户端 POST /v1/export/tasks（api_code + 参数）
   → 生成 task_id 返回 202
   → 线程池查询 Doris → 写 CSV → 上传 SILO（exports 桶）
   → 客户端 GET /v1/export/tasks/{task_id} 轮询
   → 完成返回预签名下载 URL
   → SILO 生命周期 7 天自动清理
```

---

## 4. 部署规格映射（按实机）

### 4.1 节点规格与组件共置

| 节点 | IP | 规格 | 组件 | 内存上限 |
|---|---|---|---|---|
| dbapi-gw01 | 192.168.110.204 | 8C / 32G / 500G | APISIX、DBAPI、**PG 主**（Patroni）、Redis 主、Sentinel(1)、etcd(1)、keepalived（priority 150） | 见调优文档 §1.2（≈18.7G） |
| dbapi-gw02 | 192.168.110.205 | 8C / 32G / 500G | APISIX、DBAPI、**PG 同步从**（Patroni）、Redis 从、Sentinel(2)、etcd(2)、keepalived（120） | 同上 |
| dbapi-gw03 | 192.168.110.206 | 8C / 32G / 500G | APISIX、DBAPI、**PG 第二从**（Patroni）、Redis 从、Sentinel(3)、etcd(3)、keepalived（100） | 同上 |
| doris-dw01 | 192.168.110.201 | 16C / 64G / 200G+500G+500G | Doris FE Leader + BE、Kafka broker1、Flink JM+TM、SILO、ZK | 见调优文档 §1.1（≈43.8G；**v1.11 etcd 迁出，省 0.5G**） |
| doris-dw02 | 192.168.110.202 | 16C / 64G / 200G+500G+500G | Doris FE Follower + BE、Kafka broker2、Flink JM+TM、SILO、ZK | 同上 |
| doris-dw03 | 192.168.110.203 | 16C / 64G / 200G+500G+500G | Doris FE Follower + BE、Kafka broker3、Flink JM+TM、SILO、ZK | 同上（**v1.11 哨兵迁出**） |

**VIP**

| VIP | 用途 | 跟随对象 |
|---|---|---|
| 192.168.110.199 | API 入口 | keepalived VI_1，探 APISIX :9080 |
| 192.168.110.200 | PG 主库入口 | keepalived VI_2，探 Patroni :8008/primary |

### 4.2 磁盘分配（数据节点：200G 系统盘 + 500G + 500G）

| 物理盘 | 挂载点 | 用途 | 配额 |
|---|---|---|---|
| `/dev/sda` 200G | `/` | OS、Docker 镜像、组件日志 | 200 GB |
| `/dev/sdb` 500G | `/data/doris` | FE meta + **BE storage（独占）** | BE 480G / FE meta ≤20G |
| `/dev/sdc` 500G | `/data/store` | Kafka 120G、SILO 300G（150×2）、Flink 50G、ZK 10G（**etcd 已于 v1.11 迁出**） | 480 GB |

- **BE 独占一块物理盘**，把最重的随机读 + Compaction 与 Kafka/SILO 的顺序写隔开，这是三盘规格带来的最大收益。
- 第二块盘仍有三者共享（Kafka/SILO/Flink），靠 Kafka 保留上限、SILO 桶配额、checkpoint 30s 间隔约束写入量。
- 扩容顺序：先扩 `/data/store`（Kafka/SILO 增长最快），再给 BE 加盘（`storage_root_path` 逗号追加，无需重建集群）。

> 第二块盘（`/data/store`）仍存在 Kafka/SILO/Flink 三者写竞争，是本方案剩余的主要物理约束。缓解措施见调优文档 §1.3 与 §9（`%util > 80%` 即需加盘）。

### 4.3 部署与参数

具体的逐节点命令见 `DBAPI_Doris数据平台部署实施手册.md`（v1.12）；内存/磁盘账本与逐组件参数（`fe.conf`、`be.conf`、Kafka、Flink、SILO、Patroni、Redis、APISIX、sysctl）见 `DBAPI_Doris硬件配置与参数调优.md`（v1.12）。本文档不重复命令，避免两处不一致。

---

## 5. 非功能设计（v1.0 缺失，本次补全）

### 5.1 监控与告警

| 层 | 采集 | 关键指标 | 告警阈值 |
|---|---|---|---|
| 主机 | node_exporter | CPU、内存、磁盘 %util、iowait | 内存 > 90%、`%util > 80%` 持续 5 分钟 |
| Doris | FE `:8030/metrics`、BE `:8040/metrics` | 查询 P99、`MemTracker`、Compaction score、副本不健康数 | 副本不健康 > 0、Compaction score > 100 |
| Kafka | kafka-exporter | 分区 ISR、消费 Lag | ISR < 副本数、Lag > 10 万 |
| Flink | `:8081/metrics`（Prometheus reporter） | checkpoint 失败数、重启次数、反压 | checkpoint 连续失败 3 次 |
| PG | postgres_exporter | 连接数、复制延迟、长事务 | 连接 > 240、复制延迟 > 10s |
| APISIX | `prometheus` 插件 | QPS、5xx、上游延迟 | 5xx > 1% |
| 业务 | DBAPI 自身 | 各 API 的 QPS/延迟/错误码 | 42901 突增、50301 > 0 |

部署建议：Prometheus + Grafana 单实例放**应用节点外**（如 DS 集群或独立监控机），避免再挤占 64G 数据节点内存。若必须内嵌，数据节点需预留额外 2G。

### 5.2 备份与恢复

| 对象 | 方式 | 频率 | 保留 | RTO |
|---|---|---|---|---|
| Doris 数据（ADS/DWS） | `BACKUP ... TO repo`（SILO 仓库） | 每日全量 | 14 天 | 分钟~小时级 |
| Doris 数据（ODS/DWD） | **不做物理备份**，靠"可重跑"：源库 + Kafka 48h + 分层保留（ODS 30 天 / DWD 90 天） | — | — | 重跑数小时~1 天 |
| Doris 表结构 | `SHOW CREATE TABLE` 导出为 SQL | 每次变更 | Git 版本化 | 分钟级 |
| PG | Patroni 流复制（实时）+ 每日 `pg_dump -Fc` | 每日 | 14 天 | 从库秒级；dump 恢复分钟级 |
| etcd | `etcdctl snapshot save` | 每日 | 14 天 | 分钟级 |
| ZK | `zkCli.sh save` / 数据目录快照 | 每日 | 7 天 | 分钟级 |
| SILO（checkpoint/导出） | 桶级别版本 + 关键 savepoint 手动归档 | 按需 | — | — |

> **PG 备份必须落在应用节点之外的存储**（如 SILO `backup` 桶），否则主从同故障域时两份一起丢。

**为什么 ODS/DWD 不做物理备份（v1.10 明确，旧稿"每日全量 14 天"在容量上不成立）**

SILO 6 drive 可用仅 ≈450G（§4.2），已分配 `flink` 80GiB + `exports` 160GiB，**剩余约 190GB**。而 Doris 单副本快照约 295G——**连一份全量都放不下，14 天保留更不可能**。旧写法会让备份任务在第 1 天就报仓库空间不足。

可行的分层策略：

| 层 | 单副本量 | 处置 |
|---|---|---|
| ADS / DWS | ~25GB | **每日 `BACKUP` 到 SILO `backup` 桶**，14 天滚动 ≈ 350GB → 与 `exports` 抢空间，需把 exports 配额降到 80GiB，或按周全量+日增量 |
| ODS / DWD | ~270GB | **不备份**，定位为"可重算的中间层"：源库还在、Kafka 留 48h、ODS 留 30 天，恢复路径 = 重跑 CDC 全量 + DS 加工 |

> 若业务要求 ODS/DWD 也能物理恢复（例如源库会清理历史），**必须外挂备份存储**（NAS 或独立对象存储 ≥1TB），SILO 的 450G 在承载 checkpoint + 导出之后不具备备份能力。这一项应在上线前与业务确认，不要默认"有 BACKUP 语句就等于有备份"。

### 5.3 TLS

| 链路 | 方案 |
|---|---|
| 外部 → APISIX | 9443 挂正式证书（或内网 CA），HTTP 9080 仅内网保留 |
| APISIX → DBAPI | 内网明文（同一二层），如需加密走 mTLS |
| DBAPI → PG | `sslmode=require`（PG 自签证书，Patroni 生成后分发） |
| DBAPI → Doris | 内网明文（Doris 9030 未开 TLS）；靠防火墙限制来源 |
| Flink/DBAPI → SILO | **生产必须启用 SILO HTTPS**（测试可用 HTTP）；未启用时至少限制 9000 仅数据层 + 应用层可达，并书面记录"checkpoint/导出走明文"的残余风险 |
| etcd | 生产建议开 peer/client TLS；当前内网可接受明文，但需限制 2379/2380 来源 |

> TLS 是**分阶段**项：一期先保证"外部到 APISIX"有证书、其余链路靠网络隔离；二期再补内部 mTLS。不要为了"全链路 TLS"阻塞上线。

### 5.4 升级与回滚

| 组件 | 升级方式 | 回滚 |
|---|---|---|
| Doris FE/BE | 逐台滚动（先 Observer/Follower 后 Leader），BE 用 `ALTER SYSTEM DECOMMISSION` 优雅下线 | 保留旧版本目录，停新起旧；元数据向前兼容但**不可跨大版本回退**，升级前必须备份 |
| DBAPI | 双实例滚动（摘一台 → 换 jar → 验证 → 换另一台） | jar 保留上一版本 + `systemctl` 回切 |
| APISIX | 路由配置在 etcd，镜像可回退 | 回退镜像 + reload |
| Kafka | 逐 broker 重启，`min.insync.replicas=2` 保证不中断 | 版本不可降级，升级前备份 `__consumer_offsets` 与 topic 配置 |
| PG | Patroni `patronictl switchover` 后升级从库 | 大版本升级需逻辑复制，**不在本次范围** |

---

## 6. 风险与注意事项

1. **FE 必须 3 个可投票成员**：Leader + 2 Follower。任何一台是 Observer 都会让多数派失效（§2.1）。
2. **Doris KEY 列规则**：KEY 列必须在表定义最前、顺序一致，且**分区列必须是 KEY 列**；Unique/Aggregate 的分布列必须在 KEY 内。三条任一条不满足都直接建表失败。
3. **BE 内存靠 `mem_limit` 不靠 JVM 参数**：BE 是 C++ 进程；64G 共置机取 40%，超过 50% 会挤压 Kafka/SILO 导致级联故障。
4. **第二块盘 I/O 争抢**：Kafka 追加 + SILO checkpoint + Flink RocksDB 共用 `/dev/sdc`，`%util` 长期 > 80% 时**优先加盘**而不是加节点（BE 已独占 `/dev/sdb`，风险比单盘方案低一档）。
5. **SILO 3 盘不满足 EC**：必须每节点划 2 个目录凑 6 drive，且三桶配额 + checkpoint 保留 3 份兜底。
6. **CDC 删除会丢**：`upsert-kafka` 的 tombstone 被 Routine Load 跳过，删除靠 DWD 每日对账闭环（§2.7.2），**不要**假设 ODS 能反映删除。
7. **Aggregate 模型 KEY 顺序即维度顺序**，上线后变更需重建表，建表前必须与业务确认维度组合。
8. **大查询防护四道闸**：必填日期参数 → `max_limit` → Doris `query_timeout` → APISIX 限流。少一道都可能打爆 25.6G 的 BE。
9. **压缩比需实测校准**：容量按 0.2~0.35 预估，上线一周后用 `SHOW DATA` 修正。
10. **动态分区时间窗**：`dynamic_partition.end=3` 预留未来 3 天，避免当天无分区可写；`start` 是删除边界，改小等于**立刻删历史分区**，务必先备份。
11. **密钥轮换**：`app_secret` 轮换期新旧并行 24h，提前通知调用方。
12. **etcd 双用耦合**：APISIX 路由写入风暴会抬高 Patroni 选主延迟；etcd 延迟 P99 > 100ms 时需排查（`etcdctl endpoint status`）。**v1.11 起三成员全在应用层**，意味着一次应用层双机同时维护就会丢掉 etcd 仲裁 → APISIX 读不到配置、Patroni 选不了主，**应用层维护必须逐台做**。
13. **Redis 必须 3 哨兵 + `--net host`**：2 哨兵永远无法完成切换；容器内网 IP 会导致切主后 DBAPI 连不上。v1.11 起三哨兵全在应用层（gw01/gw02/gw03），**数据节点上若残留 v1.10 的哨兵容器，会让 `num-other-sentinels` 变成 3 并覆盖新配置**，迁移时必删。
14. **DS 与本平台不共享元数据库**：DS 用自有 MySQL，避免调度库与 API 元数据库互相拖累。
15. **历史数据撞分区（v1.10）**：CDC 全量 + ODS 按业务日期分区 = 必现的 `Partition not found`，作业 PAUSED。已改为按入湖日期分区（§2.4.1），DWD 加工须加 `pay_date >= curdate()-90` 过滤（§2.7.1）。
16. **故障自愈水位（v1.10）**：单副本总量超过 768G（每节点 53%）后，单 BE 故障将无法完成副本补齐，集群长期停 2 副本。扩容判据必须同时看"自愈线"而不只是"70% 磁盘"（§2.8）。
17. **备份介质不足（v1.10）**：SILO 剩 ~190G，放不下 Doris 单副本全量。ODS/DWD 采用"可重跑"策略，ADS/DWS 才做物理备份；要全量物理备份必须外挂存储（§5.2）。
18. **`strict_mode=false` 不等于数据干净**：类型转换失败会静默写 NULL 且**不计入 ErrorRows**，`ErrorRows=0` 是假象。真防线是 §2.6 的非空率/值域校验 + 每日空值巡检（§2.7.1）。
19. **PG 只 1 台从库时 RPO 会失守（v1.11）**：2 台应用节点时主库宕机 → 唯一从库升主 → 集群再无从库可确认同步复制 → Patroni 退化为异步，**从切换那一刻到旧主修回，所有提交都不再是 RPO=0**，新主再坏一次即全丢。改 1 主 2 从后降级窗口只有"追平差量"的秒级。**判断是否真生效**：`patronictl list` 必须 3 行且其中一台 Sync，主库 `pg_stat_replication` 计数为 2。
20. **`synchronous_node_count` 不要改成 2（v1.11）**：看似"更安全"，实际是任一从库重启/抖动都会阻塞主库写入（`synchronous_commit` 等不到两个确认），把可靠性参数变成可用性事故。保持默认 1，第二台从库的价值在故障转移时接任同步角色。
21. **SILO 客户端入口仍是单点（v1.12 新增，P0-05）**：数据层三节点 EC 冗余 **≠** 入口无单点。Flink 的 `s3.endpoint` 与 DBAPI 导出地址当前都只指向 dw01（`.201:9000`）；该机整机故障时**数据在、但访问不到**，Flink checkpoint 会连续失败、HA 元数据读不到。两条出路：① 前置 L4 VIP（keepalived + HAProxy）或用 SILO 自身多 endpoint；② 若暂不做，必须在验收文档明确写下"Flink 对象存储访问入口为单点、`.201` 故障期间 checkpoint 中断"的残余风险与 RTO 边界。**不能既不做入口 HA 又宣称"checkpoint 无单点"。**
22. **Flink 链路语义是"至少一次"，不是"不丢且不重"（v1.12 新增，P0-06）**：在 checkpoint 成功、源端 binlog 保留充分、Kafka/Doris 下游具备恢复条件的前提下，Flink 可从最近成功 checkpoint 恢复；故障恢复期间**可能重复处理**。ODS 是 Duplicate 模型，同一批消息消费两次就是两行。最终"不重复"由 checkpoint + Kafka sink 事务 + ODS/DWD 幂等去重共同保证，**不要对外承诺端到端 exactly-once**。

---

## 7. 修订记录

> **[HISTORY — DO NOT COPY]** 本节为历史版本存档，含已失效的 IP、VIP、组件名与配置口径。不要复制其中命令或配置，现行口径见 §0~§6。

### v1.12（2026-10-25，按外部整改清单关闭 P0/P1）

- **RPO 表述（P1-10）**：删除"RPO=0"的绝对表述，改为"同步复制正常、同步 standby 可用时 RPO≈0"，并明确异常场景须经演练验证（§1.1 目标、§6 第 19/20 条）。
- **APISIX 版本（P1-11）**：§3.1 组件表 `Apache APISIX 3.9` → **3.19.0**，与版本基线一致。
- **配套文档版本（P1-12）**：头部配套手册与调优文档由 v1.7 改为 **v1.12**；版本口径标注由 v1.8 改为 **v1.12**。
- **容量水位（P1-13）**：§2.8 新增统一运行规则表，把 256G / 295G / 336G / 768G / 53% 收敛为三档（正常 / 风险区 / 高风险），并说明"单副本总量 = 单节点用量 × 3"的换算关系，区分"磁盘占用视角"与"故障自愈视角"。
- **SILO 入口单点（P0-05）**：§1.1 可用性目标不再宣称"入湖链路均无单点"；§6 新增第 21 条，明确数据层 EC ≠ 入口 HA 及两条出路。
- **链路语义（P0-06）**：§6 新增第 22 条，明确"至少一次 / 可能重复"，不承诺端到端 exactly-once。
- **命名统一（P1-19）**：全文组件名统一为 SILO，`.minio.sys` / `MINIO_*` / `minio_*` 仅作兼容性说明保留；头部增加命名口径说明，防止现场误装 MinIO。
- **TLS 口径（P1-06）**：§5.3 由"生产建议启用"改为"生产必须启用 HTTPS"，未启用须书面记录残余风险。
- **未采纳**：P1-07（S3 Secret 环境变量化），按用户要求保留占位符写法。

### v1.11（2026-10-23，应用服务层 2 台 → 3 台）

- **拓扑**：应用服务层改 3 台同构 `dbapi-gw01/02/03 = .204/.205/.206`（原 `.199/.200` 作废）；数据/入湖层 3 台**不再承载任何应用层组件**（etcd 与 Redis 哨兵均已迁出）。
- **PG 1 主 2 从**：§1.1 可用性目标与 §6 风险 19/20 明确"2 台时切换后 RPO 会失守、3 台把降级窗口压到秒级"，并给出三条验证口径（`patronictl list` 3 行 / `pg_stat_replication`=2 / 一台 Sync）。
- **etcd / 哨兵归位**：§4.1 节点表与 §1.2 拓扑图同步；§5.1 依赖关系不变（仍是同一集群），但成员全在应用层。
- **连接数**：§3.6 由 2×25 改 3×25 = 75（`max_connections=300` 仍有充足余量）。
- **Admin API**：§3.5 白名单增 `.206`。
- **口径清理**：架构图与 §3.6/§3.7、§4.2、§6 中的"MinIO"统一为 SILO（正文交互一律用 SILO，仅 §0 保留"MinIO 兼容分支"的来源说明）。

### v1.10（2026-10-23，容量与数据正确性复核）

第三轮评审的落点：前两轮解决"装得上"和"切得动"，本轮解决"**跑得完、装得下、坏了能自愈**"。

| # | 问题 | 后果 | 修正 |
|---|---|---|---|
| 1 | ODS `stat_date` 取业务时间 `create_time`，而动态分区窗口只有 -30 天 | **CDC 全量同步历史订单必现 `Partition not found`**，ErrorRows 超阈值后作业 PAUSED，全量永远跑不完 | 改为入湖日期 `curdate()`（§2.4.1 / §2.7.1）；DWD 加工加 `pay_date >= curdate()-90` 过滤 |
| 2 | DWS/ADS 用日分区 × 10 桶，保留 730 天 | 730 分区 × 10 桶 = **7300 tablet**，而该层仅 ~15GB → 约 2MB/tablet，远低于 100MB 建议，元数据与 compaction 白受压 | 改月分区（`start=-36`）+ **6 桶**（3 BE 上 2/2/2 均匀）→ tablet 降至 234 |
| 3 | 容量只算稳态 480G 单副本，未算故障重建 | 单 BE 宕机后每节点需扛 1.5 份，295G×1.5=442G > 384G 可用 → **副本补齐失败，集群长期停 2 副本** | 新增"故障自愈上限 256G 单副本 / 53%"，扩容判据由一条变两条（§2.8） |
| 4 | 备份写"每日全量 14 天"，介质是 SILO | SILO 剩 ~190G < 单副本 295G，**第一份备份就失败** | 分层：ADS/DWS 物理备份；ODS/DWD 走"可重跑"；要全量必须外挂存储（§5.2） |
| 5 | `strict_mode=false` 的边界未说明 | 类型转换失败静默写 NULL 且不计 ErrorRows，`ErrorRows=0` 是假象 | 补说明 + 每日空值巡检 SQL（§2.7.1） |
| 6 | DataX `doriswriter.column` 按"reader 顺序 + 函数"拼，与表列顺序不符 | **10 列全部错位**（user_id 进 stat_date、shop_id 进 user_id、金额进 shop_id…），`strict_mode=false` 下静默写错，离线补数出来的全是脏数据 | 按表列顺序重排，`date_format(create_time,...)` → `curdate()`；补逐位比对表与自检方法（§2.7.3） |

### v1.9（2026-10-23，网络边界定稿）

DS 集群确认与平台同处 `192.168.110.0/24`，APISIX `allow_admin` 由 `/24` 收窄为 `127.0.0.1` + `.199/.200`（§3.5，**此处 `.199/.200` 是当时的应用节点 IP**；v1.11 起节点改为 `.204/.205/.206`，这两个地址改作 VIP）；VIP `.199/.200` 确认无冲突。DS 节点 IP = `192.168.110.131/.132/.133`。

### v1.8（2026-10-23，HA 生效性复核）

同步部署手册 v1.8 的 7 项 HA 修正结论（keepalived `weight`、Patroni 配置路径、Redis 哨兵持久化、SILO EC 可用容量 1/2、PG `wal_log_hints`、Kafka 事务参数、Flink S3 插件改 hadoop）；本文档的 DDL 与链路设计不受影响。

### v1.7（2026-10-23，口径统一 + CDC 语义修正）

1. **PG 版本修正**：§3.4 标题由 `PostgreSQL 16` 改为 **17**（Patroni 4.1.5 托管，DDL 兼容 16 语法）。
2. **对象存储口径**：全篇统一称 SILO（保留"MinIO 分支/兼容"的来源说明），与部署手册 `/data/store/silo` 一致。
3. **`op` 取值修正**：Flink CDC 的 `op` metadata 实为 `+I/+U/-U/-D`，新增 `CASE` 归一化为 `I/U`；否则 ODS 的 `op_type` 与 DWD 取最新、DS 对账的过滤条件全部对不上。
4. **Kafka sink 可靠性**：补 `properties.acks=all` + `enable.idempotence=true`（默认 `acks=1` 存在丢消息窗口）。
5. **Routine Load 边界**：`OFFSET_BEGINNING` 仅限首次初始化，重建须改 `OFFSET_END`。
6. **配套文档**：手册更名 `DBAPI_Doris数据平台部署实施手册.md`；版本结论收敛到 `DBAPI_Doris组件版本基线.md`。

### v1.5（2026-10-22，按实机规格重写）

1. **架构口径统一**：IP 全量改为 `192.168.110.201~203`（数据层）+ `192.168.110.204/.205`（应用层，v1.11 起增 `.206`）；入口固定 APISIX + 双 VIP，删除 Nginx；DBAPI 连接串改经 VIP `.200`。
2. **FE 角色修正**：Observer → **Follower**（1 Leader + 2 Follower），修复"Leader 宕机后无法选举"的致命问题。
3. **DDL 全面修复**：ODS/DWD 的 key 列顺序、DWD 分布列（`user_id`→`order_id`）、PG 分区表主键含分区列（`id, called_at`）、DWS 去重用户数改 BITMAP、ADS 补 `user_cnt`。
4. **补齐缺失 DDL**：`CREATE DATABASE dbapi_db`、`api_audit` 表、日志分区月度维护脚本。
5. **实时链路修正**：Routine Load `max_batch_interval` 改整数秒、`jsonpaths` 对齐 8 列、topic 与表同名 `ods_trade_order`、补 `property.group.id` 与错误容忍参数；新增 **§2.7.2 DELETE 语义边界**与 DWD 删除对账方案。
6. **硬件与参数对齐**：新增 §4 部署规格映射（8C32G×2 / 16C64G×3 / 200G+500G+500G 三盘），参数细节移交 `DBAPI_Doris硬件配置与参数调优.md`；删除 v1.0 §4 的重复部署步骤（避免与手册冲突）。
7. **Java 统一**：DBAPI 用系统 Java 21；Doris 用组件级 Temurin 17。
8. **新增 §5 非功能设计**：监控告警、备份恢复、TLS、升级回滚。
9. **容量重算**：按 3×480G BE（BE 独占 500G 数据盘）给出 480G 单副本能力，标注约 1.6 倍余量与 70% 扩容阈值。

### v1.0（2026-10-13）

初版：Doris 四层建模 + DBAPI 接口/安全/性能设计 + 部署规格与验证清单。已归档。
