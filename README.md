# sftp-blob-sync

把任意数量 SFTP 服务器上的文件并发同步到 Azure Blob Storage。支持增量跳过、递归目录、连接池复用，全部通过环境变量配置。

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

> 调优参数的取值高度依赖你的网络环境。**在动手调参之前，先确认 SFTP 服务器、Azure 存储账号与运行机三者的地理位置和 RTT**——如果链路跨洲且存在丢包，瓶颈是拥塞窗口塌缩而非这里任何一个参数，把作业迁到靠近两端的区域运行才是有效手段。

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

## 定时自动化运行（Cron 任务）

如需定期（如每天凌晨 2 点）自动执行同步任务，可以在 Linux/macOS 的 `crontab -e` 中添加：

```bash
0 2 * * * cd /path/to/sftp-blob-sync && /usr/bin/python3 main.py >> /var/log/sftp_sync.log 2>&1
```

---

## 许可证

[Apache License 2.0](LICENSE)
