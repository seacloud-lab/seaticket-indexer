import logging
import json

from seaqa_indexer.utils.constants import CrawlStatus
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.crawler.web_crawl import WebCrawl


logger = logging.getLogger(__name__)


def craw_web(connection_id, db_session_class, mq):
    def failed_crawl_site(error_msg, params):
        site_id, site_url, connection_status = params
        connection_status['last_sync_status'] = CrawlStatus.FAILED
        seaqa_db.update_connection_status(site_id, json.dumps(connection_status))
        logger.warning('crawl site failed, id: %s, url: %s, error: %s', site_id, site_url, error_msg)

    def success_call_back(msg, params):
        site_id = params[0]
        msg_content = {
            'connection_id': site_id,
        }
        mq.lpush('index_task', json.dumps(msg_content))

    logger.info(f"Start crawl connection: {connection_id}")
    seaqa_db = SeaqaDB(db_session_class)
    project_connection = seaqa_db.get_project_connection_by_id(connection_id)
    if not project_connection:
        logger.warning('connection: %s not found', connection_id)
        return

    connection_is_active = project_connection.is_active
    if not connection_is_active:
        logger.warning('connection: %s is inactive', connection_id)
        return

    connection_status = json.loads(project_connection.status)
    last_sync_status = connection_status.get('last_sync_status')
    if last_sync_status == CrawlStatus.CRAWLING:
        logger.warning('connection: %s is crawling', connection_id)
        return

    connection_status['last_sync_status'] = CrawlStatus.CRAWLING
    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

    connection_config = json.loads(project_connection.config)
    site_url = connection_config.get('url')
    sitemap_url = connection_config.get('sitemap_url')
    project_uuid = str(project_connection.project_uuid)

    web_crawl = WebCrawl(db_session_class)

    if not sitemap_url:
        sitemap_url = web_crawl.get_sitemap_url(site_url)
        if sitemap_url:
            connection_config['sitemap_url'] = sitemap_url
            seaqa_db.update_connection_config(connection_id, json.dumps(connection_config))

    try:
        web_crawl.crawl(connection_id, site_url, sitemap_url, project_uuid, connection_status, success_call_back, failed_crawl_site)
    except Exception as e:
        logger.exception('failed to crawl site: %s, error: %s', connection_id, e)
        connection_status['last_sync_status'] = CrawlStatus.FAILED
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))


# def failed_crawl_site(seaqa_db, error_msg, params):
#     site_id, site_url, connection_status = params
#     connection_status['last_sync_status'] = CrawlStatus.FAILED
#     seaqa_db.update_connection_status(site_id, json.dumps(connection_status))
#     logger.warning('crawl site failed, id: %s, url: %s, error: %s', site_id, site_url, error_msg)
#
#
# def success_call_back(msg, mq, params):
#     site_id = params[0]
#     mq.add_site_index_task(site_id)
