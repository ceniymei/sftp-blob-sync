"""
配置加载与校验模块
"""
import os
import re
from dataclasses import dataclass, field
from typing import List, Optional
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # 兼容环境未安装 python-dotenv 的情况，使用轻量原生解析
    def _fallback_load_dotenv(env_path=".env"):
        if not os.path.exists(env_path):
            return
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip("'\"")
                if key not in os.environ:
                    os.environ[key] = val

    _fallback_load_dotenv()


SFTP_NODE_PATTERN = re.compile(r"^SFTP(\d+)_")


def discover_sftp_indices() -> List[int]:
    """从环境变量中发现所有已配置的 SFTP 节点编号

    扫描形如 SFTP1_HOST / SFTP2_USER 的变量名并取出编号，节点数量不限，
    编号也允许不连续（例如只配置 SFTP1 与 SFTP3）。
    """
    indices = set()
    for key in os.environ:
        m = SFTP_NODE_PATTERN.match(key)
        if m:
            indices.add(int(m.group(1)))
    return sorted(indices)


def env_int(name: str, default: int, minimum: int = 1) -> int:
    """读取正整数环境变量，非法值回退到默认值"""
    raw = os.getenv(name, "").strip()
    if not raw.isdigit():
        return default
    value = int(raw)
    return value if value >= minimum else default


def str_to_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in ("true", "1", "yes", "y", "on")


@dataclass
class SFTPConfig:
    name: str
    enabled: bool
    host: str
    port: int
    user: str
    password: Optional[str] = None
    key_file: Optional[str] = None
    key_passphrase: Optional[str] = None
    remote_dir: str = "/"
    blob_prefix: str = ""
    # SSH channel 窗口大小（字节）。paramiko 默认仅 2MiB，单连接吞吐上限约为
    # window / RTT，跨洋链路（RTT 150-250ms）会被锁死在 10MB/s 左右。
    window_size: int = 64 * 1024 * 1024

    def validate(self):
        if not self.enabled:
            return
        if not self.host:
            raise ValueError(f"[{self.name}] SFTP_HOST 不能为空")
        if not self.user:
            raise ValueError(f"[{self.name}] SFTP_USER 不能为空")
        if not self.password and not self.key_file:
            raise ValueError(f"[{self.name}] 必须提供 SFTP_PASSWORD 或 SFTP_KEY_FILE 之一进行认证")
        if self.key_file and not os.path.isfile(self.key_file):
            raise ValueError(f"[{self.name}] 找不到指定的私钥文件: {self.key_file}")


@dataclass
class AzureBlobConfig:
    connection_string: str
    container_name: str
    # 小于该阈值的文件一次 PUT 传完，不走分块（SDK 默认 64MB）
    max_single_put_size: int = 16 * 1024 * 1024
    # 分块上传的块大小。SDK 默认 4MB，高延迟链路下偏小
    max_block_size: int = 8 * 1024 * 1024
    # HTTP 连接池上限，需 >= 并发文件数 × upload 并发数
    connection_pool_maxsize: int = 64

    def validate(self):
        if not self.connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING 不能为空，请在 .env 中设置")
        if not self.container_name:
            raise ValueError("AZURE_CONTAINER_NAME 不能为空，请在 .env 中设置")


@dataclass
class AppConfig:
    azure: AzureBlobConfig
    sftp_sources: List[SFTPConfig] = field(default_factory=list)
    overwrite_existing: bool = False
    delete_after_copy: bool = False
    max_concurrent_files: int = 5
    max_concurrent_sources: int = 2
    log_level: str = "INFO"


def load_config() -> AppConfig:
    """从环境变量加载所有配置"""
    azure_conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "").strip()
    azure_container = os.getenv("AZURE_CONTAINER_NAME", "").strip()

    azure_config = AzureBlobConfig(
        connection_string=azure_conn_str,
        container_name=azure_container,
        max_single_put_size=env_int("AZURE_MAX_SINGLE_PUT_MB", 16) * 1024 * 1024,
        max_block_size=env_int("AZURE_MAX_BLOCK_MB", 8) * 1024 * 1024,
        connection_pool_maxsize=env_int("AZURE_CONNECTION_POOL_SIZE", 64),
    )

    # SFTP 传输窗口，所有节点共用同一调优值
    sftp_window_size = env_int("SFTP_WINDOW_SIZE_MB", 64) * 1024 * 1024

    sftp_sources = []
    for idx in discover_sftp_indices():
        prefix = f"SFTP{idx}_"
        enabled = str_to_bool(os.getenv(f"{prefix}ENABLED"), default=True)
        host = os.getenv(f"{prefix}HOST", "").strip()
        port_val = os.getenv(f"{prefix}PORT", "22").strip()
        port = int(port_val) if port_val.isdigit() else 22
        user = os.getenv(f"{prefix}USER", "").strip()
        password = os.getenv(f"{prefix}PASSWORD", "").strip() or None
        key_file = os.getenv(f"{prefix}KEY_FILE", "").strip() or None
        key_passphrase = os.getenv(f"{prefix}KEY_PASSPHRASE", "").strip() or None
        remote_dir = os.getenv(f"{prefix}REMOTE_DIR", "/").strip() or "/"
        blob_prefix = os.getenv(f"{prefix}BLOB_PREFIX", f"sftp{idx}/").strip()

        # 确保 blob_prefix 如果不为空则规范化
        if blob_prefix and not blob_prefix.endswith("/"):
            blob_prefix += "/"
        if blob_prefix.startswith("/"):
            blob_prefix = blob_prefix.lstrip("/")

        sftp_cfg = SFTPConfig(
            name=f"SFTP-{idx}",
            enabled=enabled,
            host=host,
            port=port,
            user=user,
            password=password,
            key_file=key_file,
            key_passphrase=key_passphrase,
            remote_dir=remote_dir,
            blob_prefix=blob_prefix,
            window_size=sftp_window_size,
        )
        sftp_sources.append(sftp_cfg)

    overwrite_existing = str_to_bool(os.getenv("OVERWRITE_EXISTING"), default=False)
    delete_after_copy = str_to_bool(os.getenv("DELETE_AFTER_COPY"), default=False)
    max_concurrent_files = env_int("MAX_CONCURRENT_FILES", 5)
    max_concurrent_sources = env_int("MAX_CONCURRENT_SOURCES", 2)
    log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()

    return AppConfig(
        azure=azure_config,
        sftp_sources=sftp_sources,
        overwrite_existing=overwrite_existing,
        delete_after_copy=delete_after_copy,
        max_concurrent_files=max_concurrent_files,
        max_concurrent_sources=max_concurrent_sources,
        log_level=log_level,
    )
