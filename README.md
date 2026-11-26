# kafka-console-rs

Rust 实现的 Kafka 控制台工具，单二进制 Web 服务。基于 rdkafka（librdkafka）+ axum，无 JVM 依赖。

## 功能

- **首页概览**：broker 拓扑与 leader 分区分布、topic/分区/副本/消息量统计、ISR 不足与无 leader 分区、消费组状态分布、消息量 Top 10，可查看 broker 关键服务端配置（`auto.create.topics.enable`、retention、`num.partitions` 等）
- 集群/topic 列表（含分区数、副本数、消息量估算）
- topic 详情：逐分区 leader / 副本 / ISR / 起止 offset，以及全部配置项（标出非默认值）
- topic 按需运维：增加分区、删除 topic（删除需输入 topic 名二次确认）
- 按 topic 看消费详情：哪些消费组在消费、各自逐分区积压
- 消费组列表与逐分区积压（committed offset vs high watermark）
- 消息浏览：SSE 流式返回，支持 offset / 时间戳 / beginning / end 定位
- **在线发送消息**：指定 key / 分区 / headers，返回实际落盘分区与 offset
- 消息页 topic 支持键入搜索（模糊过滤 + ↑↓ 选择 + 回车确认），分区下拉标注各分区可读消息数，可一键扫描全部有消息的分区
- **集群连接在页面端管理**：右上角「集群管理」可增删改集群、一键测试连通性，配置持久化到 `config/clusters.json`，重启不丢，**不再依赖配置文件**
- 二进制消息自动转 hex 预览，避免乱码

**读消息零干扰**：每个浏览请求创建独立 consumer，用 `assign()` + `seek()` 手动定位，不 subscribe、不提交位点、不加入消费组，线上消费者不会触发 rebalance。

## 下载

从 [Releases](../../releases) 下载对应平台的开包，解压即可运行，不需要装 JVM / JRE：

| 平台 | 包名 |
|---|---|
| Windows x64 | `kafka-console-rs-windows-x64.zip` |
| Linux x64 | `kafka-console-rs-linux-x64.tar.gz` |
| Linux arm64 | `kafka-console-rs-linux-arm64.tar.gz` |

包内结构：可执行文件 + `static/`（前端页面，**必须和可执行文件放同一目录**）+ `config/clusters.example.toml`。OpenSSL 与 librdkafka 均已静态链进去，目标机不需要额外装任何库。

Linux 版是 **musl 静态链接**，不依赖目标机的 glibc 版本：CentOS 7（glibc 2.17）、麒麟 / 统信之类老系统都能直接跑，不会出现 `GLIBC_2.xx not found`。

## 运行

```bash
# Windows
kafka-console-rs.exe

# Linux / macOS
tar -xzf kafka-console-rs-linux-x64.tar.gz
./kafka-console-rs-linux-x64/kafka-console-rs

# 浏览器访问
http://127.0.0.1:7755
```

服务会把当前工作目录当作根目录，`static/` 是从 **cwd** 找的，所以请在解压出来的目录里启动。

集群连接不需要改任何文件：启动后在页面右上角「集群管理」里填地址、协议、认证信息，保存即生效。

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `KAFKA_CONSOLE_LISTEN` | `0.0.0.0:7755` | 监听地址 |
| `KAFKA_CONSOLE_STORE` | `config/clusters.json` | 集群配置存储位置 |
| `KAFKA_CONSOLE_CONFIG` | `config/clusters.toml` | 可选的配置文件，只用于设置 `[server]` 和首次导入集群 |

配置文件是可选的：

- `config/clusters.json`（存储）不存在时，才会读 `config/clusters.toml` 里的 `[[clusters]]` 做一次导入；
- 两个文件都不存在也能启动，页面上添加集群即可；
- 存储文件里有明文密码，已在 `.gitignore` 里排除，接口返回时会脱敏（只告诉前端「已设置」）；
- 仓库里的 `config/clusters.example.toml` 是模板，复制成 `config/clusters.toml` 再改（该文件同样被 gitignore）；发行包里已经带了这个模板。

## API

| 端点 | 说明 |
|---|---|
| `GET /api/health` | 健康检查 |
| `GET /api/clusters` | 集群列表（密码脱敏） |
| `POST /api/clusters` | 新增/更新集群，body 同 `ClusterBody`；密码留空则沿用原值 |
| `DELETE /api/clusters/{name}` | 删除集群连接配置（不影响服务端） |
| `POST /api/clusters/test` | 测试连通性，返回 broker/topic 数与耗时 |
| `POST /api/clusters/{name}/disconnect` | 断开并释放该集群的客户端连接（librdkafka 线程 + TCP），配置保留，下次访问自动重连 |
| `GET /api/overview?cluster=local` | 集群概览（broker 拓扑、健康度、Top topic） |
| `GET /api/brokers/{id}/configs?cluster=local` | broker 关键配置 |
| `POST /api/messages` | 发送消息，body `{"cluster","topic","partition?","key?","value","headers?"}` |
| `GET /api/topics?cluster=local` | topic 列表 |
| `GET /api/topics/{topic}?cluster=local` | topic 详情：分区（leader/副本/ISR/水位）+ 配置项 |
| `DELETE /api/topics/{topic}?cluster=local` | 删除 topic（不可逆，拒绝 `__` 开头内部 topic） |
| `POST /api/topics/{topic}/partitions?cluster=local` | 增加分区，body `{"partition_count": 目标总数}` |
| `GET /api/topics/{topic}/groups?cluster=local` | 消费详情：消费该 topic 的组及逐分区积压 |
| `GET /api/partitions?cluster=local&topic=xxx` | 分区水位 |
| `GET /api/groups?cluster=local` | 消费组列表 |
| `GET /api/groups/{group}/lag?cluster=local` | 消费组逐分区积压 |
| `POST /api/groups/{group}/offsets?cluster=local` | 重置位点，body `{"topic","offsets":[{"partition","offset"}]}` 或 `{"topic","reset_to":"earliest\|latest"}`；拒绝活跃消费组 |
| `GET /api/messages?cluster=local&topic=xxx&partition=0&start=beginning&limit=100` | SSE 消息流 |

`start` 支持：`beginning`、`end`、数字 offset、毫秒时间戳（`>= 10^12` 自动识别，页面上用「指定时间」选择器换算）。

发送消息走 `FutureProducer`（cluster 上惰性创建并复用）。发送前会先校验 topic 是否存在——broker 若开了 `auto.create.topics.enable`，往写错的 topic 发消息会静默建出一个新 topic，控制台里直接拒掉并提示。

写操作（删除 topic、增加分区）走 AdminClient。删除在服务端额外拒绝 `__consumer_offsets` 这类内部 topic，页面上还需手动输入 topic 名称确认。增加分区的 `partition_count` 是 Kafka 语义的**操作后总数**，不是增量。

## 编译

三个平台由 GitHub Actions 自动构建（`.github/workflows/release.yml`）：推送 `v*` 标签即触发，产物直接挂到 Release 上。

| runner | target | 说明 |
|---|---|---|
| `windows-latest` | `x86_64-pc-windows-msvc` | 需 MSVC 工具链（`ilammy/msvc-dev-cmd` 负责进环境） |
| `ubuntu-latest` | `x86_64-unknown-linux-musl` | 静态链接，不绑定 glibc |
| `ubuntu-24.04-arm` | `aarch64-unknown-linux-musl` | 原生 arm64 runner + musl，不做交叉编译 |

Linux 侧依赖：`build-essential`、`cmake`、`perl`、`musl-tools`。用 gnu target 编出来的二进制会绑定构建机的 glibc（Ubuntu 24.04 = glibc 2.39），在老系统上直接报 `GLIBC_2.xx not found`，所以这里一律走 musl。

本地构建：

```bash
cargo build --release
```

### SSL / SASL 支持

`Cargo.toml` 已启用 `ssl-vendored`（内置编译 OpenSSL 3.6），因此 **SSL、SASL_SSL、SASL_PLAINTEXT 都能用**，页面端填完即连：

| security.protocol | 说明 |
|---|---|
| PLAINTEXT | 明文，默认 |
| SSL | TLS 双向/单向，填 `ssl.ca.location` 等 |
| SASL_PLAINTEXT | SASL over 明文（不安全，仅内网） |
| SASL_SSL | SASL over TLS，生产环境最常用 |

SASL 机制由 librdkafka 内置实现：**SCRAM-SHA-256 / SCRAM-SHA-512 / OAUTHBEARER 可用**。`PLAIN` 需要额外的 Cyrus SASL 库，Windows 下未启用。

**Windows 构建要求**（OpenSSL 从源码编译，比较挑环境）：

1. 完整的 Perl —— Git 自带的 Perl 是精简版（缺 `ExtUtils::MakeMaker`、`Locale::Maketext::Simple`），不能用。装一个：`scoop install perl`
2. MSVC 工具链 + Windows SDK（VS 安装时勾选「使用 C++ 的桌面开发」）
3. 用 `build-with-ssl.bat` 编译，它自动探测 Perl / MSVC / SDK 版本，把 `nmake`、`INCLUDE`、`LIB` 都准备好：

```bat
build-with-ssl.bat
build-with-ssl.bat --profile release   :: 额外参数透传给 cargo build
```

**Linux 构建要求**：`build-essential`、`cmake`、`perl`。OpenSSL 与 librdkafka 都是就地源码编译，直接 `cargo build --release` 即可，不需要系统装 `libssl-dev`。

首次编译 OpenSSL 约 25 分钟，之后增量编译正常。**Windows 上不要**直接 `cargo build`——环境不全时 OpenSSL 的 build script 会失败，并清掉已编译产物，得从头再来。

> 踩坑记录：`openssl-src` 会读取 `PERL` 环境变量当作 perl 可执行文件路径，所以脚本里不能用 `PERL=<目录>` 当变量名；改用 `OPENSSL_SRC_PERL` 指定完整路径。

## 实测结果（内网测试集群，2026-11-25）

- 18 个 topic，4 个消费组
- `mq2pspace-consumer`：7 个分区有提交位点，总积压 3133 条
- SSE 消息流正常，二进制消息 hex 预览正常
- 所有端点 < 1s 响应（topic 列表并发拉取 watermark）
