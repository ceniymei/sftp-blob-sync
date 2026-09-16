"""
主程序入口：从两台 SFTP 服务器流式同步文件到 Azure Blob Storage
"""
import concurrent.futures
import logging
import sys
import tempfile
import threading
import time
from config import AppConfig, SFTPConfig, load_config
from sftp_client import SFTPConnectionPool
from blob_uploader import AzureBlobManager

# 配置日志
def setup_logging(level_name: str):
    log_level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Azure SDK 在 INFO 级别会打印每个请求/响应的完整 HTTP header，
    # 数千个文件会产生十几万行日志，既拖慢 I/O 又让真正的信息无法阅读。
    # 只在显式开 DEBUG 时才放行这些第三方日志。
    if log_level > logging.DEBUG:
        for noisy in (
            "azure.core.pipeline.policies.http_logging_policy",
            "azure.storage.blob",
            "azure.identity",
            "paramiko.transport",
        ):
            logging.getLogger(noisy).setLevel(logging.WARNING)


def sync_single_sftp(sftp_cfg: SFTPConfig, blob_manager: AzureBlobManager, app_config: AppConfig):
    """通过多线程连接池并发同步单个 SFTP 服务器上的文件到 Blob Storage"""
    logger = logging.getLogger(sftp_cfg.name)
    logger.info(f"========== 开始同步节点: {sftp_cfg.name} ({sftp_cfg.host}:{sftp_cfg.port}) ==========")

    # 1. 先建好连接池并并行预热：单次建连约 4 秒，等到传输时按需建连会让 worker 排队干等
    pool = SFTPConnectionPool(sftp_cfg, max_size=app_config.max_concurrent_files)
    failed_stats = {"total_scanned": 0, "uploaded": 0, "skipped": 0, "failed": 1, "deleted": 0}
    try:
        pool.prewarm()
    except Exception as e:
        logger.error(f"节点 {sftp_cfg.name} 建立 SFTP 连接失败: {e}", exc_info=True)
        pool.close_all()
        return failed_stats

    # 2. 复用池中的一条连接扫描目录，避免为扫描单独建连、用完又丢弃
    all_files = []
    try:
        with pool.acquire() as scan_client:
            logger.info(f"正在扫描远程目录: {sftp_cfg.remote_dir} ...")
            all_files = list(scan_client.list_files_recursive())
    except Exception as e:
        logger.error(f"节点 {sftp_cfg.name} 列取文件列表失败: {e}", exc_info=True)
        pool.close_all()
        return failed_stats

    total_count = len(all_files)
    logger.info(f"扫描完成，共发现 {total_count} 个文件待处理，并发工作线程数: {app_config.max_concurrent_files}")

    stats = {
        "total_scanned": total_count,
        "uploaded": 0,
        "skipped": 0,
        "failed": 0,
        "deleted": 0,
    }

    if total_count == 0:
        pool.close_all()
        return stats

    # 3. 增量同步：一次性拉取目标前缀下所有 blob 的大小。
    #    逐个文件调 get_blob_properties() 的话，N 个文件就是 N 次 HTTP 往返。
    existing_sizes = None
    if not app_config.overwrite_existing:
        try:
            existing_sizes = blob_manager.list_blob_sizes(sftp_cfg.blob_prefix)
        except Exception as e:
            logger.warning(f"列举已有 blob 失败，本轮退化为全量上传: {e}")

    stats_lock = threading.Lock()
    processed_count = 0

    def process_file_task(file_info):
        nonlocal processed_count
        rel_path = file_info.relative_path.lstrip("/")
        # 若路径包含 outbound/ 前缀则自动移除
        if rel_path.startswith("outbound/"):
            rel_path = rel_path[len("outbound/"):]

        blob_name = f"{sftp_cfg.blob_prefix}{rel_path}".replace("//", "/")

        # 检查是否可跳过已存在且大小相同的文件（查内存缓存，不发网络请求）
        if existing_sizes is not None:
            existing_size = existing_sizes.get(blob_name)
            if existing_size is not None and existing_size == file_info.size:
                logger.info(f"跳过未变更文件: {file_info.full_path} (大小一致: {file_info.size} 字节)")
                with stats_lock:
                    stats["skipped"] += 1
                    processed_count += 1
                return

        # 从连接池借用连接流式上传
        try:
            task_start = time.time()

            # 使用内存缓冲区（小文件在内存，超过32MB自动临时落盘，任务结束立即安全销毁）
            with tempfile.SpooledTemporaryFile(max_size=32 * 1024 * 1024) as tmp_buf:
                # 步骤 1: 高速流水线预取下载 (消弭 SFTP 协议网络延迟)
                sftp_start = time.time()
                with pool.acquire() as client:
                    client.download_to_stream(file_info.full_path, tmp_buf, file_size=file_info.size)
                    if app_config.delete_after_copy:
                        try:
                            client.delete_file(file_info.full_path)
                            with stats_lock:
                                stats["deleted"] += 1
                        except Exception as del_err:
                            logger.error(f"删除远程文件失败: {file_info.full_path}: {del_err}")

                sftp_duration = time.time() - sftp_start
                sftp_speed_mb = (file_info.size / 1024 / 1024) / sftp_duration if sftp_duration > 0 else 0
                logger.info(
                    f"[SFTP下载完成] {file_info.full_path} (耗时 {sftp_duration:.2f}s, 速率 ~{sftp_speed_mb:.2f} MB/s)"
                )

                # 步骤 2: 上传至 Azure Blob Storage
                tmp_buf.seek(0)
                blob_duration = blob_manager.upload_stream(
                    blob_name=blob_name,
                    stream=tmp_buf,
                    file_size=file_info.size,
                    overwrite=True,
                )

            total_file_time = time.time() - task_start
            logger.info(
                f"[{processed_count + 1}/{total_count}] 同步成功: {blob_name} "
                f"(总用时: {total_file_time:.2f}s | SFTP下载: {sftp_duration:.2f}s | Azure上传: {blob_duration:.2f}s)"
            )

            with stats_lock:
                stats["uploaded"] += 1
                processed_count += 1
        except Exception as err:
            logger.error(f"传输文件失败: {file_info.full_path}, 错误: {err}")
            with stats_lock:
                stats["failed"] += 1
                processed_count += 1

    # 4. 使用线程池并发传输
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=app_config.max_concurrent_files) as executor:
            futures = [executor.submit(process_file_task, f_info) for f_info in all_files]
            concurrent.futures.wait(futures)
    finally:
        pool.close_all()

    logger.info(
        f"========== 节点完成: {sftp_cfg.name} | "
        f"扫描: {stats['total_scanned']} | 上传: {stats['uploaded']} | "
        f"跳过: {stats['skipped']} | 失败: {stats['failed']} | 删除原文件: {stats['deleted']} =========="
    )
    return stats


def main():
    start_time = time.time()
    config = load_config()
    setup_logging(config.log_level)
    logger = logging.getLogger("Main")

    logger.info("正在启动 SFTP 到 Azure Blob Storage 同步服务 ...")

    # 校验 Azure 配置
    try:
        config.azure.validate()
    except ValueError as e:
        logger.error(f"配置错误: {e}")
        sys.exit(1)

    # 初始化 Azure 客户端
    blob_manager = AzureBlobManager(config.azure)
    try:
        blob_manager.connect()
    except Exception as e:
        logger.error(f"连接 Azure Blob Storage 失败: {e}")
        sys.exit(1)

    overall_stats = {
        "uploaded": 0,
        "skipped": 0,
        "failed": 0,
        "total_scanned": 0,
    }

    # 遍历每个 SFTP 源
    active_sources = [s for s in config.sftp_sources if s.enabled]
    if not active_sources:
        logger.warning(
            "未启用任何 SFTP 数据源。请在 .env 中至少配置一组 SFTP<N>_HOST / SFTP<N>_USER "
            "（N 为任意正整数，如 SFTP1_、SFTP2_），并确认对应的 SFTP<N>_ENABLED 未设为 false"
        )
        return

    valid_sources = []
    for sftp_cfg in active_sources:
        try:
            sftp_cfg.validate()
            valid_sources.append(sftp_cfg)
        except ValueError as val_err:
            logger.error(f"跳过配置无效的节点 [{sftp_cfg.name}]: {val_err}")
            overall_stats["failed"] += 1

    # 各节点之间互不相干，并行同步；每个节点内部仍按 MAX_CONCURRENT_FILES 并发
    if valid_sources:
        source_workers = min(len(valid_sources), max(1, config.max_concurrent_sources))
        logger.info(f"有效节点 {len(valid_sources)} 个，节点级并行度: {source_workers}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=source_workers) as executor:
            future_map = {
                executor.submit(sync_single_sftp, cfg, blob_manager, config): cfg
                for cfg in valid_sources
            }
            for future in concurrent.futures.as_completed(future_map):
                cfg = future_map[future]
                try:
                    stats = future.result()
                except Exception as node_err:
                    logger.error(f"节点 {cfg.name} 同步异常终止: {node_err}", exc_info=True)
                    overall_stats["failed"] += 1
                    continue
                for key in ("uploaded", "skipped", "failed", "total_scanned"):
                    overall_stats[key] += stats[key]

    total_duration = time.time() - start_time
    logger.info("================ 全局同步统计 ================")
    logger.info(f"总耗时: {total_duration:.2f} 秒")
    logger.info(f"总扫描文件: {overall_stats['total_scanned']}")
    logger.info(f"成功上传: {overall_stats['uploaded']}")
    logger.info(f"跳过未变: {overall_stats['skipped']}")
    logger.info(f"失败数量: {overall_stats['failed']}")
    logger.info("==============================================")

    if overall_stats["failed"] > 0:
        sys.exit(2)


if __name__ == "__main__":
    main()
