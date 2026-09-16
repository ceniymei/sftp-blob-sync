"""
Azure Blob Storage 上传管理模块
"""
import logging
import time
from typing import Dict, Optional
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContainerClient
from config import AzureBlobConfig

logger = logging.getLogger(__name__)


class AzureBlobManager:
    """Azure Blob Storage 管理器，支持容器自动创建、元数据检查与流式上传"""

    def __init__(self, config: AzureBlobConfig):
        self.config = config
        self.blob_service_client: Optional[BlobServiceClient] = None
        self.container_client: Optional[ContainerClient] = None

    def connect(self):
        """初始化 Azure Blob 客户端并确保容器存在"""
        logger.info(f"正在初始化 Azure Blob Storage 客户端 (容器: {self.config.container_name}) ...")
        # SDK 默认 max_block_size=4MB / 单次 PUT 阈值 64MB，高延迟链路下块偏小；
        # 连接池默认值也不足以支撑 并发文件数 × 分块并发数 的 HTTP 连接量。
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.config.connection_string,
            max_single_put_size=self.config.max_single_put_size,
            max_block_size=self.config.max_block_size,
            connection_pool_maxsize=self.config.connection_pool_maxsize,
        )
        self.container_client = self.blob_service_client.get_container_client(self.config.container_name)

        # 尝试创建容器（如果不存在）
        try:
            self.container_client.create_container()
            logger.info(f"容器 '{self.config.container_name}' 不存在，已自动创建")
        except ResourceExistsError:
            logger.debug(f"容器 '{self.config.container_name}' 已存在")
        except Exception as e:
            logger.warning(f"检查/创建容器时发生提示: {e}，将尝试直接使用该容器")

    def list_blob_sizes(self, prefix: str = "") -> Dict[str, int]:
        """一次性列出指定前缀下所有 blob 的 name -> size 映射

        增量同步若逐个文件调 get_blob_properties()，N 个文件就是 N 次 HTTP 往返
        （本例 3402 个文件、单次往返约 0.24s，光检查就要十几分钟）。这里改成一次
        分页 list，之后的比对全在内存完成。
        """
        if not self.container_client:
            raise RuntimeError("Azure Blob 客户端未就绪")

        start_time = time.time()
        sizes: Dict[str, int] = {}
        for blob in self.container_client.list_blobs(name_starts_with=prefix):
            sizes[blob.name] = blob.size

        logger.info(
            f"[Azure Blob] 已缓存前缀 '{prefix}' 下 {len(sizes)} 个 blob 的大小 "
            f"(耗时 {time.time() - start_time:.2f}s)"
        )
        return sizes

    def get_blob_size(self, blob_name: str) -> Optional[int]:
        """获取已存在 Blob 的大小（字节）。若不存在返回 None"""
        if not self.container_client:
            raise RuntimeError("Azure Blob 客户端未就绪")
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            props = blob_client.get_blob_properties()
            return props.size
        except ResourceNotFoundError:
            return None
        except Exception as e:
            logger.debug(f"检查 Blob 属性异常 ({blob_name}): {e}")
            return None

    def upload_stream(self, blob_name: str, stream, file_size: int, overwrite: bool = False) -> bool:
        """从二进制文件流上传至 Blob Storage (零磁盘落地)"""
        if not self.container_client:
            raise RuntimeError("Azure Blob 客户端未就绪")

        blob_client = self.container_client.get_blob_client(blob_name)
        start_time = time.time()

        try:
            # 流式直传，设定分块并发
            blob_client.upload_blob(
                data=stream,
                overwrite=overwrite,
                max_concurrency=4,
            )
            duration = time.time() - start_time
            speed_mb = (file_size / 1024 / 1024) / duration if duration > 0 else 0
            logger.info(
                f"[Azure Blob] 上传完成: {blob_name} ({file_size} 字节, 上传耗时 {duration:.2f}s, 速率 ~{speed_mb:.2f} MB/s)"
            )
            return duration
        except Exception as e:
            logger.error(f"[Azure Blob] 上传失败: {blob_name}, 错误: {e}")
            raise
