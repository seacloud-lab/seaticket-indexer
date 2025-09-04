import logging
import json
from sqlalchemy.sql import text

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.constants import ConnectionType, REPO_NAME_CACHE_TIME_OUT
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.seasearch.index_store.site_index import SiteIndex
from seaqa_indexer.seasearch.index_store.seafile_index import SeafileIndex
from seaqa_indexer.seasearch.index_store.github_issues_index import GitHubIssuesIndex
from seaqa_indexer.seasearch.index_store.discourse_index import DiscourseIndex
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.seasearch.utils import gen_index_name_by_type
from seaqa_indexer.mq import get_mq
from seaqa_indexer.app.event_redis import redis_cache


logger = logging.getLogger(__name__)


class IndexManager:
    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        self.site_index = SiteIndex(self.seasearch_api)
        self.seafile_index = SeafileIndex(self.seasearch_api)
        self.github_issues_index = GitHubIssuesIndex(self.seasearch_api, self.db_session_class)
        self.discourse_index = DiscourseIndex(self.seasearch_api, self.db_session_class)
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def get_project_connection_by_id(self, connection_id):
        with self.db_session_class() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid, pc.type, pc.updated_at, pc.indexed_at, pc.is_active
             FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.id=:connection_id AND pc.deleted=false"""
            connection = session.execute(text(sql), {'connection_id': connection_id}).fetchone()
        return connection

    def delete_index_by_project(self, project_uuid):
        # delete seafile index
        seafile_index_name = gen_index_name_by_type(project_uuid, ConnectionType.SEAFILE.value)
        self.seafile_index.delete_index_by_index_name(seafile_index_name)

        # delete site index
        site_index_name = gen_index_name_by_type(project_uuid, ConnectionType.SITE.value)
        self.site_index.delete_index_by_index_name(site_index_name)

        #delete issues index
        issues_index_name = gen_index_name_by_type(project_uuid,ConnectionType.GITHUB_ISSUE.value)
        self.github_issues_index.delete_index_by_index_name(issues_index_name)

        # delete discourse index
        discourse_index_name = gen_index_name_by_type(project_uuid, ConnectionType.DISCOURSE_FORUM.value)
        self.discourse_index.delete_index_by_index_name(discourse_index_name)

    def delete_index_by_connection(self, project_uuid, connection_id, connection_type):
        index_name = gen_index_name_by_type(project_uuid, connection_type)
        index = None
        if connection_type == ConnectionType.SITE.value:
            index = self.site_index
        elif connection_type == ConnectionType.SEAFILE.value:
            index = self.seafile_index
        elif connection_type == ConnectionType.GITHUB_ISSUE.value:
            index = self.github_issues_index
        elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
            index = self.discourse_index
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

            index.delete_indexes_by_ids(index_name, ids)

    def add_connection_index_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('index_task', json.dumps(msg_content))

    def search(self, project_uuid, connection_types, connection_ids, keyword, start=0, size=20):
        result_docs = []
        bulk_search_params = []
        connection_type_list = connection_types.split(',') if connection_types \
            else [connection_type.value for connection_type in ConnectionType]

        connection_id_list = connection_ids.split(',') if connection_ids else None

        for connection_type in connection_type_list:
            if connection_type == ConnectionType.SEAFILE.value:
                seafile_index_name = gen_index_name_by_type(project_uuid, ConnectionType.SEAFILE.value)
                seafile_search_param = self.seafile_index.get_search_param(seafile_index_name, keyword, start, size, connection_id_list)
                bulk_search_params.append(seafile_search_param)
            elif connection_type == ConnectionType.SITE.value:
                site_index_name = gen_index_name_by_type(project_uuid, ConnectionType.SITE.value)
                site_search_param = self.site_index.get_search_param(site_index_name, keyword, start, size, connection_id_list)
                bulk_search_params.append(site_search_param)
            elif connection_type == ConnectionType.EMAIL.value:
                pass
            elif connection_type == ConnectionType.GITHUB_ISSUE.value:
                issues_index_name = gen_index_name_by_type(project_uuid, ConnectionType.GITHUB_ISSUE.value)
                issues_search_param = self.github_issues_index.get_search_param(issues_index_name, keyword, start, size, connection_id_list)
                bulk_search_params.append(issues_search_param)
            elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
                discourse_index_name = gen_index_name_by_type(project_uuid, ConnectionType.DISCOURSE_FORUM.value)
                discourse_search_param = self.discourse_index.get_search_param(discourse_index_name, keyword, start, size, connection_id_list)
                bulk_search_params.append(discourse_search_param)

        if not bulk_search_params:
            return []

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
                r = {
                    'title': source['title'],
                    'repo_owner': source['repo_owner'],
                    'repo_name': source['repo_name'],
                    'issue_number': source['issue_number'],
                    'connection_id': source['connection_id'],
                    'content': source['body'],
                    'state': source['state'],
                    'labels': source['labels'],
                    'score': score,
                    '_id': _id,
                    'type': 'github_issue',
                }
                result_docs.append(r)
            elif index_name.startswith(ConnectionType.DISCOURSE_FORUM.value):
                r = {
                    'title': source['title'],
                    'topic_id': source['topic_id'],
                    'connection_id': source['connection_id'],
                    'content': source['body'],
                    'slug': source['slug'],
                    'score': score,
                    'bumped_at': source['bumped_at'],
                    '_id': _id,
                    'type': 'discourse_forum',
                }
                result_docs.append(r)

        logger.debug('all type index search keyword: %s, in project: %s , \nsearch result: %s', keyword,
                        project_uuid, result_docs)

        return result_docs
