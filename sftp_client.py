"""
SFTP 客户端封装模块
"""
import concurrent.futures
import logging
import os
import posixpath
import queue
import stat
import threading
from typing import Generator, NamedTuple, Optional
import paramiko
from config import SFTPConfig

logger = logging.getLogger(__name__)

# 从 prefetch 缓冲区读取的块大小。加大可减少 Python 层循环开销，
# 与 SFTP 协议层的 32KB 请求粒度无关。
READ_CHUNK_SIZE = 1024 * 1024


class RemoteFileInfo(NamedTuple):
    """远程文件信息"""
    full_path: str       # SFTP上的绝对/完整路径，例如 /upload/2024/data.csv
    relative_path: str   # 相对于 remote_dir 的相对路径，例如 2024/data.csv
    size: int            # 文件大小（字节）
    mtime: int           # 最后修改时间戳


class SFTPClientWrapper:
    """SFTP 客户端封装，支持自动连接、递归列举与流式读取"""

    def __init__(self, config: SFTPConfig):
        self.config = config
        self.ssh_client: Optional[paramiko.SSHClient] = None
        self.sftp: Optional[paramiko.SFTPClient] = None

    def connect(self):
        """建立 SSH 与 SFTP 会话"""
        logger.info(f"[{self.config.name}] 正在连接 SFTP 服务器 {self.config.host}:{self.config.port} ...")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs = {
            "hostname": self.config.host,
            "port": self.config.port,
            "username": self.config.user,
            "timeout": 30,
        }

        # 身份验证：优先私钥，其次密码
        if self.config.key_file:
            logger.info(f"[{self.config.name}] 使用私钥认证: {self.config.key_file}")
            connect_kwargs["key_filename"] = self.config.key_file
            if self.config.key_passphrase:
                connect_kwargs["passphrase"] = self.config.key_passphrase
        elif self.config.password:
            connect_kwargs["password"] = self.config.password
            # 只配了密码时，禁止 paramiko 去翻 ~/.ssh 和 ssh-agent。
            # 否则每次建连都要先白跑一轮注定失败的 publickey 认证（约 1-2 秒）。
            connect_kwargs["look_for_keys"] = False
            connect_kwargs["allow_agent"] = False

        ssh.connect(**connect_kwargs)
        self.ssh_client = ssh

        # 单连接吞吐上限 ≈ window / RTT。paramiko 默认窗口仅 2MiB，
        # 跨洋链路会把速率锁死在 10MB/s 左右，必须在开 sftp 会话前放大。
        transport = ssh.get_transport()
        transport.default_window_size = self.config.window_size
        # 默认每传输 512MB 就重新协商密钥，大文件传输时会造成可观的停顿
        transport.packetizer.REKEY_BYTES = 2 ** 40

        self.sftp = ssh.open_sftp()
        logger.info(
            f"[{self.config.name}] SFTP 连接成功 "
            f"(channel window: {self.config.window_size // (1024 * 1024)} MB)"
        )

    def close(self):
        """关闭连接释放资源"""
        if self.sftp:
            try:
                self.sftp.close()
            except Exception as e:
                logger.debug(f"[{self.config.name}] 关闭 sftp 异常: {e}")
            self.sftp = None
        if self.ssh_client:
            try:
                self.ssh_client.close()
            except Exception as e:
                logger.debug(f"[{self.config.name}] 关闭 ssh 异常: {e}")
            self.ssh_client = None
        logger.info(f"[{self.config.name}] 连接已关闭")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def list_files_recursive(self, base_dir: Optional[str] = None) -> Generator[RemoteFileInfo, None, None]:
        """递归枚举目录下所有文件"""
        if not self.sftp:
            raise RuntimeError("SFTP 未连接，无法列举文件")

        target_base = base_dir or self.config.remote_dir
        # 规范化路径
        target_base = target_base.rstrip("/")
        if not target_base:
            target_base = "/"

        self._scan_dirs = 0
        self._scan_files = 0
        yield from self._walk_dir(target_base, root_dir=target_base)
        logger.info(
            f"[{self.config.name}] 目录遍历结束: 共 {self._scan_dirs} 个目录, {self._scan_files} 个文件"
        )

    def _walk_dir(self, current_dir: str, root_dir: str) -> Generator[RemoteFileInfo, None, None]:
        """内部递归遍历函数"""
        try:
            entries = self.sftp.listdir_attr(current_dir)
        except IOError as e:
            logger.error(f"[{self.config.name}] 读取目录失败: {current_dir}, 错误: {e}")
            return

        # 递归遍历是串行的、每层一次往返；打进度便于定位在哪一层卡住
        self._scan_dirs = getattr(self, "_scan_dirs", 0) + 1
        logger.debug(f"[{self.config.name}] 已进入目录 {current_dir} ({len(entries)} 项)")
        if self._scan_dirs % 100 == 0:
            logger.info(
                f"[{self.config.name}] 扫描中... 已遍历 {self._scan_dirs} 个目录, "
                f"累计 {getattr(self, '_scan_files', 0)} 个文件 (当前: {current_dir})"
            )

        for entry in entries:
            filename = entry.filename
            if filename in (".", ".."):
                continue

            full_path = posixpath.join(current_dir, filename)

            if stat.S_ISDIR(entry.st_mode):
                # 递归遍历子目录
                yield from self._walk_dir(full_path, root_dir=root_dir)
            elif stat.S_ISREG(entry.st_mode):
                # 普通文件
                # 计算相对于 root_dir 的相对路径
                if root_dir == "/":
                    rel_path = full_path.lstrip("/")
                else:
                    rel_path = posixpath.relpath(full_path, root_dir)

                self._scan_files = getattr(self, "_scan_files", 0) + 1
                yield RemoteFileInfo(
                    full_path=full_path,
                    relative_path=rel_path,
                    size=entry.st_size,
                    mtime=entry.st_mtime,
                )

    def open_file_stream(self, remote_path: str):
        """以二进制流模式打开远程文件"""
        if not self.sftp:
            raise RuntimeError("SFTP 未连接")
        return self.sftp.open(remote_path, mode="rb")

    def download_to_stream(self, remote_path: str, target_stream, file_size: Optional[int] = None):
        """通过并发预取将远程文件下载到目标流中

        相比 sftp.getfo()，这里复用 listdir_attr 阶段已经拿到的 file_size，
        省掉每个文件一次 stat() 往返 —— 高延迟链路上文件数量多时收益明显。
        """
        if not self.sftp:
            raise RuntimeError("SFTP 未连接")

        with self.sftp.open(remote_path, "rb") as remote_file:
            if file_size is None:
                file_size = remote_file.stat().st_size
            if file_size > 0:
                # 后台线程流水线预取，消弭 SFTP 协议的逐请求往返延迟
                remote_file.prefetch(file_size)
            while True:
                chunk = remote_file.read(READ_CHUNK_SIZE)
                if not chunk:
                    break
                target_stream.write(chunk)

    def delete_file(self, remote_path: str):
        """删除远程文件"""
        if not self.sftp:
            raise RuntimeError("SFTP 未连接")
        self.sftp.remove(remote_path)
        logger.info(f"[{self.config.name}] 远程文件已删除: {remote_path}")


class SFTPConnectionPool:
    """SFTP 连接池，用于多线程并发文件下载"""

    def __init__(self, config: SFTPConfig, max_size: int = 5):
        self.config = config
        self.max_size = max(1, max_size)
        self._pool = queue.Queue(maxsize=self.max_size)
        self._all_clients = []
        self._reserved = 0
        self._lock = threading.Lock()

    def _create_client(self) -> SFTPClientWrapper:
        client = SFTPClientWrapper(self.config)
        client.connect()
        return client

    def prewarm(self):
        """并行预建连接池中的所有连接

        单次建连约需 4 秒（TCP + 密钥交换 + 认证）。若等到传输时按需建连，
        worker 线程会一个个排队干等；并行预热把这段开销压到一次建连的时间。
        """
        need = self.max_size - len(self._all_clients)
        if need <= 0:
            return

        logger.info(f"[{self.config.name}] 正在并行预热 {need} 条 SFTP 连接 ...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=need) as executor:
            futures = [executor.submit(self._create_client) for _ in range(need)]
            for future in concurrent.futures.as_completed(futures):
                try:
                    client = future.result()
                except Exception as e:
                    logger.warning(f"[{self.config.name}] 预热连接失败（将减少一条可用连接）: {e}")
                    continue
                with self._lock:
                    self._all_clients.append(client)
                    self._reserved += 1
                self._pool.put(client)

        if not self._all_clients:
            raise RuntimeError(f"[{self.config.name}] 所有 SFTP 连接均建立失败")
        logger.info(f"[{self.config.name}] 连接池就绪，可用连接 {len(self._all_clients)} 条")

    def acquire(self):
        """获取一个可用连接的上下文管理器"""
        client = None
        # 尝试非阻塞从池中拿现有连接
        try:
            client = self._pool.get_nowait()
        except queue.Empty:
            # 仅在锁内占位，建连（约 4 秒）放到锁外执行，
            # 否则其他线程会全部堵在锁上等这一次建连完成。
            reserved = False
            with self._lock:
                if self._reserved < self.max_size:
                    self._reserved += 1
                    reserved = True

            if reserved:
                try:
                    client = self._create_client()
                except Exception:
                    with self._lock:
                        self._reserved -= 1
                    raise
                with self._lock:
                    self._all_clients.append(client)

        # 若达到连接上限，阻塞等待空闲连接
        if client is None:
            client = self._pool.get()

        return _PooledSFTPContext(self, client)

    def _release(self, client: SFTPClientWrapper):
        self._pool.put(client)

    def close_all(self):
        with self._lock:
            for client in self._all_clients:
                try:
                    client.close()
                except Exception:
                    pass
            self._all_clients.clear()
            self._reserved = 0
            while not self._pool.empty():
                try:
                    self._pool.get_nowait()
                except queue.Empty:
                    break


class _PooledSFTPContext:
    def __init__(self, pool: SFTPConnectionPool, client: SFTPClientWrapper):
        self.pool = pool
        self.client = client

    def __enter__(self) -> SFTPClientWrapper:
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool._release(self.client)
