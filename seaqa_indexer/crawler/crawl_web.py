import logging
import json
import datetime

from seaqa_indexer.utils.constants import CrawlStatus
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.crawler.web_crawl import WebCrawl


logger = logging.getLogger(__name__)


def craw_web(connection_id, db_session_class, mq, sync_logger):
    def failed_crawl_site(error_msg, params):
        site_id, site_url, connection_status, sync_logger = params
        connection_status['last_sync_status'] = CrawlStatus.FAILED
        seaqa_db.update_connection_status(site_id, json.dumps(connection_status))
        sync_logger._log_warning(f'crawl site failed, id: {site_id}, url: {site_url}, error: {error_msg}')
        # stop capture and flush to persist logs
        try:
            sync_logger.stop_capture()
        finally:
            sync_logger._flush_log_lines()

    def success_call_back(msg, params):
        site_id = params[0]
        msg_content = {
            'connection_id': site_id,
        }
        mq.lpush('index_task', json.dumps(msg_content))
        # mark finished and persist captured logs
        sync_logger._log_info(f"Crawl finished for connection: {site_id}")
        try:
            sync_logger.stop_capture()
        finally:
            sync_logger._flush_log_lines()
    sync_logger.init(connection_id)
    # start capture external libs (trafilatura, scrapy, etc.)
    sync_logger._log_info(f"Start crawl connection: {connection_id}")
    seaqa_db = SeaqaDB(db_session_class)
    project_connection = seaqa_db.get_project_connection_by_id(connection_id)
    if not project_connection:
        sync_logger._log_warning(f'connection: {connection_id} not found')
        try:
            sync_logger.stop_capture()
        finally:
            sync_logger._flush_log_lines()
        return

    connection_is_active = project_connection.is_active
    if not connection_is_active:
        sync_logger._log_warning(f'connection: {connection_id} is inactive')
        try:
            sync_logger.stop_capture()
        finally:
            sync_logger._flush_log_lines()
        return

    connection_status = json.loads(project_connection.status)
    last_sync_status = connection_status.get('last_sync_status')
    if last_sync_status == CrawlStatus.CRAWLING:
        sync_logger._log_warning(f'connection: {connection_id} is crawling')
        try:
            sync_logger.stop_capture()
        finally:
            sync_logger._flush_log_lines()
        return

    connection_status['last_sync_status'] = CrawlStatus.CRAWLING
    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
    connection_config = json.loads(project_connection.config)
    site_url = connection_config.get('url')
    sitemap_url = connection_config.get('sitemap_url')
    project_uuid = str(project_connection.project_uuid)

    web_crawl = WebCrawl(db_session_class, sync_logger)

    if not sitemap_url:
        sitemap_url = web_crawl.get_sitemap_url(site_url)
        if sitemap_url:
            connection_config['sitemap_url'] = sitemap_url
            seaqa_db.update_connection_config(connection_id, json.dumps(connection_config))

    try:
        web_crawl.crawl(connection_id, site_url, sitemap_url, project_uuid, connection_status, success_call_back, failed_crawl_site)
    except Exception as e:
        sync_logger._log_error(f'failed to crawl site: {connection_id}, error: {e}')
        connection_status['last_sync_status'] = CrawlStatus.FAILED
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
        try:
            sync_logger.stop_capture()
        finally:
            sync_logger._flush_log_lines()
