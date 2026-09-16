# sftp-blob-sync

把任意数量 SFTP 服务器上的文件并发同步到 Azure Blob Storage。支持增量跳过、递归目录、连接池复用，全部通过环境变量配置。

---

## 先读这一段：你可能不需要本项目

这个需求早有成熟方案，**如果你只是要把文件从 SFTP 同步到 Blob，请优先用 [rclone](https://rclone.org/azureblob/)**：

```bash
rclone sync sftp:/outbound azureblob:container/prefix --transfers 16
```

`sftp` 与 `azureblob` 都是 rclone 的官方后端，增量比对（大小/修改时间/校验和）、并发控制、重试、限速、`--dry-run`、过滤规则全部内置，功能是本项目的超集，且经过远比本项目广泛的实战检验。

其他现成选择：

| 方案 | 适用场景 |
|---|---|
| [rclone](https://rclone.org/azureblob/) | 绝大多数情况下的首选 |
| [Azure Data Factory](https://learn.microsoft.com/en-us/azure/data-factory/connector-sftp) | 企业环境，要托管服务与可视化编排；支持[按 LastModifiedDate 增量](https://azure.microsoft.com/en-us/blog/incrementally-copy-new-files-by-lastmodifieddate-with-azure-data-factory/) |
| [Airflow `SFTPToWasbOperator`](https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure/stable/operators/sftp_to_wasb.html) | 已经在跑 Airflow |

**那什么时候用本项目？**

- 需要在传输过程中嵌入自定义逻辑——改写路径、按节点映射不同前缀、条件化删除源文件等。本项目约 700 行可读 Python，改起来比给 rclone 套 shell 胶水直接。
- 受限环境里不方便部署 Go 二进制，但已有 Python 运行时。
- 多个 SFTP 节点希望用一份声明式 `.env` 管理，而不是编排 N 条命令。

如果以上都不符合，用 rclone。

---

## 核心特性

- **多线程连接池并发传输**：通过 `MAX_CONCURRENT_FILES` 配置每个节点的工作线程数，连接池在启动时并行预热（单次 SFTP 建连往往需数秒，按需建连会让 worker 排队干等）。
- **节点级并行**：多个 SFTP 节点同时同步，由 `MAX_CONCURRENT_SOURCES` 控制。注意实际总并发 = 节点并行度 × 单节点线程数。
- **缓冲式传输**：下载到内存缓冲区后再上传，超过 32MB 自动落盘为临时文件，任务结束即销毁。单文件耗时为“下载 + 上传”之和。
- **任意数量 SFTP 节点**：按 `SFTP<N>_*` 命名即可，N 为任意正整数且允许不连续（如只配 `SFTP1_` 与 `SFTP3_`）。每个节点可独立配置主机、端口、密码/私钥认证、源目录与 Blob 前缀。
- **增量检测**：`OVERWRITE_EXISTING=false` 时，启动阶段一次性列出目标前缀下所有 Blob 的大小并缓存到内存，之后逐文件比对不再产生网络往返；大小一致即跳过。
- **递归同步**：自动递归枚举子文件夹，并在 Azure Blob Storage 中保留相同的相对目录层级结构。
- **安全性**：支持密码认证和 SSH 私钥认证（支持私钥口令 Passphrase），敏感信息统一通过 `.env` 隔离管理。

---

## 目录结构

```text
.
├── .env.example       # 环境变量配置模板
├── .env               # 实际运行环境变量（不提交到代码仓库）
├── .gitignore         # Git 忽略配置
├── requirements.txt   # Python 依赖包清单
├── config.py          # 配置解析与校验模块
├── sftp_client.py     # SFTP 客户端封装（递归枚举与流式读取）
├── blob_uploader.py   # Azure Blob Storage 管理器
├── main.py            # 主程序入口
├── LICENSE            # Apache-2.0 许可证
└── README.md          # 说明文档
```

---

## 快速上手

### 1. 安装依赖

推荐使用 Python 3.8 及以上版本。在虚拟环境或系统环境中安装依赖：

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

复制 `.env.example` 为 `.env`（项目中已包含默认初始文件），并填写实际的服务器信息与密钥：

```bash
cp .env.example .env
```

打开 `.env` 文件并根据注释修改参数：

```ini
# ==============================================================================
# Azure Blob Storage 配置
# ==============================================================================
# 连接字符串可在 Azure Portal -> 存储账户 (Storage account) -> 访问密钥 (Access keys) 获取
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=your_account;AccountKey=your_key;EndpointSuffix=core.windows.net
AZURE_CONTAINER_NAME=my-target-container

# ==============================================================================
# SFTP 服务器 1 配置
# ==============================================================================
SFTP1_ENABLED=true
SFTP1_HOST=sftp1.example.com
SFTP1_PORT=22
SFTP1_USER=sftp_user_1
SFTP1_PASSWORD=your_password_1
# 如果使用私钥认证，可填写私钥路径（如 /Users/xxx/.ssh/id_rsa）并留空密码
SFTP1_KEY_FILE=
SFTP1_KEY_PASSPHRASE=
# SFTP 远程源目录
SFTP1_REMOTE_DIR=/data/incoming/
# 上传到 Azure 容器中的目录前缀（如 sftp1/）
SFTP1_BLOB_PREFIX=sftp1_data/

# ==============================================================================
# SFTP 服务器 2 配置（继续按 SFTP3_、SFTP4_ ... 追加即可，数量不限）
# ==============================================================================
SFTP2_ENABLED=true
SFTP2_HOST=sftp2.example.com
SFTP2_PORT=22
SFTP2_USER=sftp_user_2
SFTP2_PASSWORD=your_password_2
SFTP2_KEY_FILE=
SFTP2_KEY_PASSPHRASE=
SFTP2_REMOTE_DIR=/exports/
SFTP2_BLOB_PREFIX=sftp2_data/

# ==============================================================================
# 传输控制策略
# ==============================================================================
# 是否覆盖已存在且大小相同的文件 (false: 增量跳过; true: 强行覆盖)
OVERWRITE_EXISTING=false

# 复制到 Blob 成功后，是否删除 SFTP 上的原文件 (true: 删除; false: 保留)
DELETE_AFTER_COPY=false

# 日志级别 (INFO / DEBUG / WARNING / ERROR)
LOG_LEVEL=INFO

# ==============================================================================
# 并发与传输调优（可选，留空即用默认值）
# ==============================================================================
# 单个节点内并发传输的文件数
MAX_CONCURRENT_FILES=16
# 同时同步的 SFTP 节点数
MAX_CONCURRENT_SOURCES=2
# SSH channel 窗口大小 (MB)。paramiko 默认仅 2MB
SFTP_WINDOW_SIZE_MB=64
# 小于该阈值的文件一次性 PUT 上传，不走分块 (Azure SDK 默认 64)
AZURE_MAX_SINGLE_PUT_MB=16
# 分块上传的块大小 (MB)，Azure SDK 默认 4
AZURE_MAX_BLOCK_MB=8
# Azure HTTP 连接池上限，应不小于 总并发文件数 × 4
AZURE_CONNECTION_POOL_SIZE=64
```

> 调优参数的取值高度依赖网络环境。**动手调参之前请先读[性能说明](#性能说明)**——如果链路跨洲，这里任何一个参数都救不了你。

### 3. 执行同步

运行主程序即可自动同步所有已启用的 SFTP 节点（节点之间并行）：

```bash
python main.py
```

终端将实时输出扫描进度、传输速率及最终统计摘要：

```text
2026-09-15 21:00:00 [INFO] [Main] 正在启动 SFTP 到 Azure Blob Storage 同步服务 ...
2026-09-15 21:00:00 [INFO] [blob_uploader] 正在初始化 Azure Blob Storage 客户端 (容器: my-target-container) ...
2026-09-15 21:00:01 [INFO] [Main] 有效节点 2 个，节点级并行度: 2
2026-09-15 21:00:01 [INFO] [sftp_client] [SFTP-1] 正在并行预热 16 条 SFTP 连接 ...
2026-09-15 21:00:05 [INFO] [sftp_client] [SFTP-1] 连接池就绪，可用连接 16 条
2026-09-15 21:00:05 [INFO] [SFTP-1] 正在扫描远程目录: /data/incoming ...
2026-09-15 21:00:07 [INFO] [SFTP-1] 扫描完成，共发现 42 个文件待处理，并发工作线程数: 16
2026-09-15 21:00:07 [INFO] [blob_uploader] [Azure Blob] 已缓存前缀 'sftp1_data/' 下 32 个 blob 的大小 (耗时 0.31s)
2026-09-15 21:00:08 [INFO] [SFTP-1] [1/42] 同步成功: sftp1_data/test.csv (总用时: 1.07s | SFTP下载: 0.85s | Azure上传: 0.22s)
...
================ 全局同步统计 ================
总耗时: 15.30 秒
总扫描文件: 42
成功上传: 10
跳过未变: 32
失败数量: 0
==============================================
```

---

## 性能说明

以下结论来自一次真实排查（两个 SFTP 节点、约 1.8 万个文件），记录下来是因为它们与直觉相反。

### 先量地理位置和 RTT，再谈调参

这是最重要的一条。传输慢时，绝大多数人的第一反应是调缓冲区、调并发、调块大小。但如果运行机与两端不在同一大洲，这些参数的收益全部约等于零。

判断方法：分别测到 SFTP 服务器和 Azure 存储账号的 TCP 握手 RTT。若明显超过 100ms 且抖动剧烈，链路就是瓶颈，**唯一有效的手段是把作业迁到靠近两端的区域运行**（例如两端都在 AWS/Azure 美东时，就在美东起一台机器跑）。

原因是跨洲链路存在丢包。单条 TCP 流的吞吐上限近似 Mathis 公式：

```
吞吐 ≈ MSS / (RTT × √丢包率)
```

代入 MSS 1460 字节、RTT 272ms、丢包 0.1–0.5%，得到 0.07–0.16 MB/s——与实测的单连接 0.05–0.2 MB/s 吻合。此时限制吞吐的是**丢包导致的拥塞窗口反复塌缩**，不是接收窗口不足，所以放大 SSH channel window 没有意义。

> `吞吐 ≤ window / RTT` 这个 BDP 公式只适用于无丢包的"长肥管道"。跨洋链路是"长而漏"，套错模型会让你在无效的方向上花掉大量时间。

### 小文件与大文件是两种完全不同的瓶颈

同一批数据里两类文件的优化手段相反，而**中位数会掩盖这种差异**——务必按大小分档统计：

| | 小文件（< 1MB） | 大文件（≥ 1MB） |
|---|---|---|
| 瓶颈 | 每文件固定往返延迟（约 1.5s） | 链路吞吐 |
| 有效手段 | 提高并发，用并行掩盖延迟 | 跨洲时无解；同区域才有意义 |

实测中某节点 98% 的 worker 时间花在小文件上（延迟主导，提高并发后 14 分钟跑完），另一节点 85% 的时间花在仅占 27% 的大文件上（吞吐主导，提高并发收益次线性）。两个节点的中位文件大小完全相同，但平均值差了 7 倍。

### 对同一个文件分段并行下载无效

把一个文件切成 N 段、用 N 条连接并行拉取，实测没有任何加速（2/4/8 条连接的结果全部落在单连接自身的测量噪声带内）。但**不同文件之间**的并发是有效的——聚合吞吐随并发数上升，只是次线性。

### 测量时务必控制混淆因素

单连接速率的天然离散度极大（同样配置、不同文件之间实测相差 5.8 倍）。这意味着：

- 单次测量毫无意义，必须多样本取均值并观察离散度
- 对同一个文件反复测会受服务端缓存影响，每个变体应使用**不同的新鲜文件**
- 否则很容易把噪声当成"某项优化带来了 4.5 倍提升"

---

## 定时自动化运行（Cron 任务）

如需定期（如每天凌晨 2 点）自动执行同步任务，可以在 Linux/macOS 的 `crontab -e` 中添加：

```bash
0 2 * * * cd /path/to/sftp-blob-sync && /usr/bin/python3 main.py >> /var/log/sftp_sync.log 2>&1
```

---

## 许可证

[Apache License 2.0](LICENSE)
