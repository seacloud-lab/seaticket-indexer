import os
import logging
import json
import datetime
from sqlalchemy.sql import text

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.constants import CrawlStatus, ConnectionType
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.seasearch.index_store.site_index import SiteIndex
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.seasearch.utils import gen_site_index_name, get_all_index_by_site
from seaqa_indexer.crawler.utils import get_site_dir
from seaqa_indexer.mq import get_mq


logger = logging.getLogger(__name__)


class IndexManager:
    def __init__(self):
        self.db_session = init_db_session_class()
        self.seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        self.site_index = SiteIndex(self.seasearch_api)
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def get_project_connection_by_id(self, connection_id):
        with self.db_session() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid, pc.type, pc.updated_at, pc.indexed_at
             FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.id=:connection_id"""
            connection = session.execute(text(sql), {'connection_id': connection_id}).fetchone()
        return connection

    def get_pending_index_project_connections(self, start, limit):
        with self.db_session() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid, pc.type
             FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.status=:status 
            AND (pc.updated_at > pc.indexed_at OR pc.indexed_at IS NULL) LIMIT :start, :limit"""
            sites = session.execute(text(sql),
                                    {'start': start, 'limit': limit, 'status': CrawlStatus.COMPLETED}
                                    ).fetchall()
            return sites

    def finish_index(self, connection_id):
        indexed_at = datetime.datetime.now(datetime.UTC)
        with self.db_session() as session:
            cmd = """UPDATE project_connection SET indexed_at=:indexed_at WHERE id = :connection_id"""
            session.execute(text(cmd), {'connection_id': connection_id, 'indexed_at': indexed_at})
            session.commit()

    def update_site_index(self, project_connection):
        site_id = project_connection.id
        site_config = json.loads(project_connection.config)
        site_url = site_config.get('url')
        project_uuid = project_connection.project_uuid

        site_dir = get_site_dir(site_url, project_uuid)
        crawled_site_files = [f for f in os.listdir(site_dir)]
        site_index_name = gen_site_index_name(project_uuid)
        self.site_index.create_index_if_missing(site_index_name)

        index_site_url_to_site_info = get_all_index_by_site(self.site_index, site_index_name, site_id)
        index_site_urls_set = set(index_site_url_to_site_info.keys())
        crawled_site_urls_set = set()

        indexed_site_info = index_site_url_to_site_info.get(site_url)

        for filename in crawled_site_files:
            filepath = os.path.join(site_dir, filename)

            with open(filepath, 'r', encoding='utf-8') as f:
                crawled_data = json.load(f)

            url = crawled_data.get('url')
            etag = crawled_data.get('etag')
            last_modified = crawled_data.get('last_modified')
            content = crawled_data.get('content')
            title = crawled_data.get('title')

            crawled_site_urls_set.add(url)

            logger.debug('start to update url: %s', url)

            if not indexed_site_info:
                self.site_index.add_document(site_index_name, site_id, content, url, etag, title, last_modified)
            else:
                if (etag and indexed_site_info.get('etag') == etag) or \
                        (last_modified and indexed_site_info.get('last_modified') == last_modified):
                    continue
                self.site_index.update_document(site_index_name, site_id, content, url, etag, title, last_modified)

        # clear deleted site crawled url
        need_cleared_site_url_index_set = index_site_urls_set - crawled_site_urls_set
        self.site_index.delete_indexes_by_urls(site_index_name, need_cleared_site_url_index_set)

    def update_indexes(self):
        start, limit = 0, 1000
        while True:
            try:
                project_connections = self.get_pending_index_project_connections(start, limit)
            except Exception as e:
                logger.error("Error: %s" % e)
                return
            start += 1000
            if len(project_connections) == 0:
                break
            for project_connection in project_connections:
                connection_id = project_connection.id
                project_uuid = project_connection.project_uuid
                connection_type = project_connection.type
                try:
                    if connection_type == ConnectionType.SITE.value:
                        self.add_connection_index_task(connection_id)
                except Exception as e:
                    logger.error('update index failed, project_uuid: %, connection_id: %s failed, error: %s', project_uuid, connection_id, e)
                    continue

    def update_index(self, connection_id):
        logger.info('start index project connection: %s', connection_id)
        connection = self.get_project_connection_by_id(connection_id)
        if not connection:
            logger.warning('project connection: %s , not exist')
            return

        update_at = connection.updated_at
        indexed_at = connection.indexed_at
        connection_type = connection.type
        status = connection.status

        if status != CrawlStatus.COMPLETED:
            logger.warning('project connection: %s does not update completed.')
            return

        if not indexed_at or indexed_at < update_at:
            if connection_type == ConnectionType.SITE.value:
                self.update_site_index(connection)

            self.finish_index(connection_id)
            logger.info('finish index project connection: %s, connection_type: %s', connection_id, connection_type)

    def add_connection_index_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('index_task', json.dumps(msg_content))

    def search(self, project_uuid, keyword, start=0, size=20):
        site_index_name = gen_site_index_name(project_uuid)
        return self.site_index.search(site_index_name, keyword, start, size)
