import os
import logging
import json
import datetime
from sqlalchemy.sql import text

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.constants import CrawlStatus, ConnectionType, REPO_FILE_SIZE_LIMIT, REPO_NAME_CACHE_TIME_OUT
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.seasearch.index_store.site_index import SiteIndex
from seaqa_indexer.seasearch.index_store.seafile_index import SeafileIndex
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.seasearch.utils import gen_index_name_by_type, get_all_id_to_site_info_by_site_id, \
    get_path_to_index_info_by_connection_id, get_content_by_download_link
from seaqa_indexer.crawler.utils import get_site_dir
from seaqa_indexer.mq import get_mq
from seaqa_indexer.utils.seahub_api import SeahubAPI, FileNotFoundException
from seaqa_indexer.utils import md5, time_str_to_utc_time, decrypt_config, encrypt_config
from seaqa_indexer.seasearch.extract import ExtractorFactory
from seaqa_indexer.app.event_redis import redis_cache


logger = logging.getLogger(__name__)


class IndexManager:
    def __init__(self):
        self.db_session = init_db_session_class()
        self.seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        self.site_index = SiteIndex(self.seasearch_api)
        self.seafile_index = SeafileIndex(self.seasearch_api)
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def get_project_connection_by_id(self, connection_id):
        with self.db_session() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid, pc.type, pc.updated_at, pc.indexed_at
             FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.id=:connection_id AND pc.deleted=false"""
            connection = session.execute(text(sql), {'connection_id': connection_id}).fetchone()
        return connection

    def get_pending_index_project_connections(self, start, limit):
        with self.db_session() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid, pc.type, pc.updated_at, pc.indexed_at
             FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.deleted=false LIMIT :start, :limit"""
            connections = session.execute(text(sql), {'start': start, 'limit': limit}).fetchall()
            return connections

    def finish_index(self, connection_id, indexed_at, updated_at=None, config=None):
        with self.db_session() as session:
            cmd = """UPDATE project_connection SET indexed_at=:indexed_at"""
            params = {'connection_id': connection_id, 'indexed_at': indexed_at}
            if updated_at:
                cmd += ", updated_at=:updated_at"
                params['updated_at'] = updated_at
            if config:
                cmd += ", config=:config"
                params['config'] = encrypt_config(config)
            cmd += " WHERE id = :connection_id"
            session.execute(text(cmd), params)
            session.commit()

    def update_site_index(self, project_connection):
        update_at = project_connection.updated_at
        indexed_at = project_connection.indexed_at
        status = project_connection.status
        if status != CrawlStatus.COMPLETED:
            logger.warning('project connection: %s does not update completed.')
            return

        if indexed_at and indexed_at > update_at:
            return

        connection_id = project_connection.id
        site_config = json.loads(project_connection.config)
        site_url = site_config.get('url')
        project_uuid = project_connection.project_uuid
        new_indexed_at = datetime.datetime.now(datetime.UTC)

        site_dir = get_site_dir(connection_id, project_uuid)
        crawled_site_files = [f for f in os.listdir(site_dir)]
        site_index_name = gen_index_name_by_type(project_uuid, project_connection.type)
        self.site_index.create_index_if_missing(site_index_name)

        index_id_to_site_info = get_all_id_to_site_info_by_site_id(self.site_index, site_index_name, connection_id)
        old_index_ids_set = set(index_id_to_site_info.keys())
        new_index_ids_set = set()

        for filename in crawled_site_files:
            filepath = os.path.join(site_dir, filename)

            with open(filepath, 'r', encoding='utf-8') as f:
                crawled_data = json.load(f)

            url = crawled_data.get('url')
            etag = crawled_data.get('etag')
            last_modified = crawled_data.get('last_modified')
            content = crawled_data.get('content')
            title = crawled_data.get('title')

            key = md5(str(connection_id) + url)
            new_index_ids_set.add(key)

            logger.debug('start to update url: %s', url)
            indexed_site_info = index_id_to_site_info.get(key)

            if not indexed_site_info:
                self.site_index.add_document(site_index_name, connection_id, content, url, etag, title, last_modified)
            else:
                if (etag and indexed_site_info.get('etag') == etag) or \
                        (last_modified and indexed_site_info.get('last_modified') == last_modified):
                    continue
                self.site_index.update_document(site_index_name, connection_id, content, url, etag, title, last_modified)

        # clear deleted site crawled url
        need_cleared_site_id_set = old_index_ids_set - new_index_ids_set
        self.site_index.delete_indexes_by_ids(site_index_name, need_cleared_site_id_set)

        self.finish_index(connection_id, new_indexed_at)

    def update_seafile_index(self, project_connection):
        connection_id = project_connection.id
        config = decrypt_config(json.loads(project_connection.config))
        server_url = config.get('server_url')
        api_token = config.get('api_token')
        old_repo_id = config.get('repo_id')
        old_last_modified = project_connection.updated_at
        project_uuid = project_connection.project_uuid

        seahub_api = SeahubAPI(server_url, api_token)
        repo_info = seahub_api.get_repo_info()
        new_repo_id = repo_info.get('repo_id')
        new_last_modified = repo_info.get('last_modified')
        new_repo_name = repo_info.get('repo_name')

        redis_cache.set(new_repo_id, json.dumps({server_url: new_repo_name}), REPO_NAME_CACHE_TIME_OUT)

        new_last_modified = time_str_to_utc_time(new_last_modified)
        if new_repo_id == old_repo_id and str(old_last_modified) == str(new_last_modified).split('+')[0]:
            logger.info('connection: %s, repo: %s does not change, skip this update', connection_id, new_repo_id)
            return

        seafile_index_name = gen_index_name_by_type(project_uuid, project_connection.type)
        self.seafile_index.create_index_if_missing(seafile_index_name)
        new_indexed_at = datetime.datetime.now(datetime.UTC)

        index_id_to_index_info = get_path_to_index_info_by_connection_id(self.seafile_index, seafile_index_name, connection_id)
        old_index_ids_set = set(index_id_to_index_info.keys())
        new_index_ids_set = set()

        repo_items = seahub_api.list_items_in_dir('/', 'f')

        for dirent in repo_items.get('dirent_list'):
            parent_dir = dirent.get('parent_dir')
            dirent_name = dirent.get('name')
            mtime = dirent.get('mtime')
            size = dirent.get('size')

            key = md5(str(connection_id) + new_repo_id + parent_dir + dirent_name)
            new_index_ids_set.add(key)
            index_info = index_id_to_index_info.get(key, {})

            logger.debug('start to connection: %s repo_id: %s, parent_dir: %s, name: %s', connection_id, new_repo_id, parent_dir, dirent_name)

            old_mtime = index_info.get('mtime')
            timestamp_mtime = datetime.datetime.fromisoformat(mtime).timestamp()
            if old_mtime:
                old_mtime = old_mtime / 1000

            if timestamp_mtime == old_mtime:
                logger.debug('connection: %s, repo_id: %s, parent_dir: %s, name: %s is not change.',
                            connection_id, new_repo_id, parent_dir, dirent_name)
                continue

            content = None
            if size > REPO_FILE_SIZE_LIMIT:
                self.seafile_index.update_document(seafile_index_name, connection_id, content, new_repo_id, parent_dir, dirent_name, timestamp_mtime)
                continue

            file_path = os.path.join(parent_dir, dirent_name)
            extractor = ExtractorFactory.get_extractor(dirent_name)
            if extractor:
                try:
                    download_link = seahub_api.get_download_link(os.path.join(parent_dir, dirent_name))
                except FileNotFoundException:
                    logger.warning('connection: %s, repo_id: %s, parent_dir: %s, name: %s file not found',
                                   connection_id, new_repo_id, parent_dir, dirent_name)
                    continue
                content = get_content_by_download_link(download_link)
                content = extractor.extract(connection_id, new_repo_id, content, file_path)
            self.seafile_index.update_document(seafile_index_name, connection_id, content, new_repo_id, parent_dir, dirent_name, timestamp_mtime)

        # clear deleted site crawled url
        need_cleared_seafile_index_set = old_index_ids_set - new_index_ids_set
        self.seafile_index.delete_indexes_by_ids(seafile_index_name, need_cleared_seafile_index_set)

        config['repo_id'] = new_repo_id
        config['repo_name'] = new_repo_name
        self.finish_index(connection_id, new_indexed_at, updated_at=new_last_modified, config=config)

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
                status = project_connection.status
                updated_at = project_connection.updated_at
                indexed_at = project_connection.indexed_at
                if connection_type == ConnectionType.SITE.value:
                    if status != CrawlStatus.COMPLETED:
                        continue
                    elif indexed_at and updated_at and indexed_at > updated_at:
                        continue
                try:
                    self.add_connection_index_task(connection_id)
                except Exception as e:
                    logger.error('timer update index failed, project_uuid: %, connection_id: %s failed, error: %s', project_uuid, connection_id, e)
                    continue

    def update_index(self, connection_id):
        logger.info('start index project connection: %s', connection_id)
        connection = self.get_project_connection_by_id(connection_id)
        if not connection:
            logger.warning('project connection: %s , not exist')
            return

        connection_type = connection.type

        if connection_type == ConnectionType.SITE.value:
            self.update_site_index(connection)
        elif connection_type == ConnectionType.SEAFILE.value:
            self.update_seafile_index(connection)

        logger.info('finish index project connection: %s, connection_type: %s', connection_id, connection_type)

    def delete_index_by_project(self, project_uuid):
        # delete seafile index
        seafile_index_name = gen_index_name_by_type(project_uuid, ConnectionType.SEAFILE.value)
        self.seafile_index.delete_index_by_index_name(seafile_index_name)

        # delete site index
        site_index_name = gen_index_name_by_type(project_uuid, ConnectionType.SITE.value)
        self.site_index.delete_index_by_index_name(site_index_name)

    def delete_index_by_connection(self, project_uuid, connection_id, connection_type):
        index_name = gen_index_name_by_type(project_uuid, connection_type)
        index = None
        if connection_type == ConnectionType.SITE.value:
            index = self.site_index
        elif connection_type == ConnectionType.SEAFILE.value:
            index = self.seafile_index
        start = 0
        per_size = 100
        while True:
            hits, total = index.query_data_by_connection_id(index_name, connection_id, start, per_size)
            if not hits:
                return
            ids = []
            for hit in hits:
                _id = hit['_id']
                ids.append(_id)

            self.seafile_index.delete_indexes_by_ids(index_name, ids)

    def add_connection_index_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('index_task', json.dumps(msg_content))

    def search(self, project_uuid, connection_types, keyword, start=0, size=20):
        result_docs = []
        bulk_search_params = []
        connection_type_list = connection_types.split(',') if connection_types \
            else [connection_type.value for connection_type in ConnectionType]

        for connection_type in connection_type_list:
            if connection_type == ConnectionType.SEAFILE.value:
                seafile_index_name = gen_index_name_by_type(project_uuid, ConnectionType.SEAFILE.value)
                seafile_search_param = self.seafile_index.get_search_param(seafile_index_name, keyword, start, size)
                bulk_search_params.append(seafile_search_param)
            elif connection_type == ConnectionType.SITE.value:
                site_index_name = gen_index_name_by_type(project_uuid, ConnectionType.SITE.value)
                site_search_param = self.site_index.get_search_param(site_index_name, keyword, start, size)
                bulk_search_params.append(site_search_param)
            elif connection_type == ConnectionType.EMAIL.value:
                pass
            elif connection_type == ConnectionType.GITHUB_ISSUE.value:
                pass
            elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
                pass

        query_body = json.dumps({
            'index_queries': bulk_search_params
        })
        results = self.seasearch_api.unified_search(query_body)

        hits = results.get('hits', []).get('hits', [])
        total = results.get('hits', {}).get('total', {}).get('value', 0)

        if not hits:
            return result_docs
        for hit in hits:
            index_name = hit.get('_index')
            source = hit.get('_source')
            score = hit.get('_score')
            _id = hit.get('_id')
            if index_name.startswith(ConnectionType.SEAFILE.value):
                repo_id = source['repo_id']
                connection_id = source['connection_id']
                repo_cache_info = redis_cache.get(repo_id)
                if not repo_cache_info:
                    project_connection = self.get_project_connection_by_id(connection_id)
                    if not project_connection:
                        continue
                    repo_name = json.loads(project_connection.config).get('repo_name')
                    server_url = json.loads(project_connection.config).get('server_url')
                    repo_cache_info = json.dumps({server_url: repo_name})
                    redis_cache.set(repo_id, repo_cache_info, REPO_NAME_CACHE_TIME_OUT)

                repo_cache_info = json.loads(repo_cache_info)
                server_url = list(repo_cache_info.keys())[0]
                repo_name = list(repo_cache_info.values())[0]

                r = {
                    'filename': source['filename'],
                    'path': source['path'],
                    'repo_id': source['repo_id'],
                    'connection_id': source['connection_id'],
                    'content': source['content'],
                    'score': score,
                    'repo_name': repo_name,
                    'server_url': server_url,
                    '_id': _id,
                    'type': 'seafile',
                }
                result_docs.append(r)
            elif index_name.startswith(ConnectionType.SITE.value):
                r = {
                    'title': source['title'],
                    'url': source['url'],
                    'connection_id': source['connection_id'],
                    'content': source['content'],
                    'score': score,
                    '_id': _id,
                    'type': 'site',
                }
                result_docs.append(r)
            # TODO other index type
            elif index_name.startswith(ConnectionType.EMAIL.value):
                pass
            elif index_name.startswith(ConnectionType.GITHUB_ISSUE.value):
                pass
            elif index_name.startswith(ConnectionType.DISCOURSE_FORUM.value):
                pass

        logger.debug('all type index search keyword: %s, in project: %s , \nsearch result: %s', keyword,
                        project_uuid, result_docs)

        return result_docs
