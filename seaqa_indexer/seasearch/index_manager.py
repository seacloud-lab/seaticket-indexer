import logging
import json
from urllib.parse import urlparse

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
from seaqa_indexer.utils.seaqa_db import SeaqaDB


logger = logging.getLogger(__name__)


class IndexManager:
    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        self.site_index = SiteIndex(self.seasearch_api)
        self.seafile_index = SeafileIndex(self.seasearch_api)
        self.github_issues_index = GitHubIssuesIndex(self.seasearch_api)
        self.discourse_index = DiscourseIndex(self.seasearch_api)
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
        self.seaqa_db = SeaqaDB(self.db_session_class)

    def delete_index_by_project(self, project_uuid):
        project_connections = self.seaqa_db.get_connections_by_project_uuid(project_uuid)
        for project_connection in project_connections:
            connection_id = project_connection.id
            connection_type = project_connection.type
            self.delete_index_by_connection(connection_id, connection_type)

    def delete_index_by_connection(self, connection_id, connection_type):
        index_name = gen_index_name_by_type(connection_id, connection_type)
        if connection_type == ConnectionType.SITE.value:
            self.site_index.delete_index_by_index_name(index_name)
        elif connection_type == ConnectionType.SEAFILE.value:
            self.seafile_index.delete_index_by_index_name(index_name)
        elif connection_type == ConnectionType.GITHUB_ISSUE.value:
            self.github_issues_index.delete_index_by_index_name(index_name)
        elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
            self.discourse_index.delete_index_by_index_name(index_name)

    def add_connection_index_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('index_task', json.dumps(msg_content))

    def search(self, project_uuid, connection_ids, time_from, time_to, keyword, start=0, size=20):
        result_docs = []
        bulk_search_params = []

        connection_id_list = connection_ids.split(',') if connection_ids else None
        project_connections = self.seaqa_db.get_connection_by_ids(project_uuid, connection_id_list)

        connection_id_to_config = {}
        for project_connection in project_connections:
            connection_type = project_connection.type
            connection_id = project_connection.id
            index_name = gen_index_name_by_type(connection_id, connection_type)
            connection_id_to_config[connection_id] = json.loads(project_connection.config)

            if connection_type == ConnectionType.SEAFILE.value:
                seafile_search_param = self.seafile_index.get_search_param(index_name, keyword, start, size, time_from, time_to)
                bulk_search_params.append(seafile_search_param)
            elif connection_type == ConnectionType.SITE.value:
                site_search_param = self.site_index.get_search_param(index_name, keyword, start, size, time_from, time_to)
                bulk_search_params.append(site_search_param)
            elif connection_type == ConnectionType.EMAIL.value:
                pass
            elif connection_type == ConnectionType.GITHUB_ISSUE.value:
                issues_search_param = self.github_issues_index.get_search_param(index_name, keyword, start, size, time_from, time_to)
                bulk_search_params.append(issues_search_param)
            elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
                discourse_search_param = self.discourse_index.get_search_param(index_name, keyword, start, size, time_from, time_to)
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
                    config = connection_id_to_config.get(connection_id)
                    repo_name = config.get('repo_name')
                    server_url = config.get('server_url')
                    repo_cache_info = json.dumps({server_url: repo_name})
                    redis_cache.set(repo_id, repo_cache_info, REPO_NAME_CACHE_TIME_OUT)

                repo_cache_info = json.loads(repo_cache_info)
                server_url = list(repo_cache_info.keys())[0]
                repo_name = list(repo_cache_info.values())[0]

                url = server_url.rstrip('/') + '/' + 'lib' + '/' + repo_id + '/file/' + source['path'].strip('/') + '/' + source['filename']
                r = {
                    'filename': source['filename'],
                    'path': source['path'],
                    'repo_id': source['repo_id'],
                    'connection_id': source['connection_id'],
                    'content': source['content'],
                    'score': score,
                    'repo_name': repo_name,
                    'server_url': server_url,
                    'url': url,
                    'mtime': source['mtime'],
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
                config = connection_id_to_config.get(source['connection_id'])
                server_url = config.get('repository')
                url_parsed = urlparse(server_url)
                base_url = url_parsed.scheme + "://" + url_parsed.netloc
                parts = url_parsed.path.strip("/").split("/")
                repo_owner, repo_name = parts[0], parts[1]
                url = f'{base_url}/{repo_owner}/{repo_name}/issues/' + str(source['issue_number'])
                r = {
                    'title': source['title'],
                    'repo_owner': source['repo_owner'],
                    'repo_name': source['repo_name'],
                    'issue_number': source['issue_number'],
                    'connection_id': source['connection_id'],
                    'content': source['body'],
                    'comments_body': source['comments_body'],
                    'state': source['state'],
                    'labels': source['labels'],
                    'score': score,
                    'url': url,
                    '_id': _id,
                    'type': 'github_issue',
                }

                result_docs.append(r)
            elif index_name.startswith(ConnectionType.DISCOURSE_FORUM.value):
                config = connection_id_to_config.get(source['connection_id'])
                discourse_forum_url = config.get('url')
                slug = source['slug']
                topic_id = source['topic_id']
                topic_url = discourse_forum_url.rstrip('/') + '/t/' + slug + '/' + str(topic_id)
                r = {
                    'title': source['title'],
                    'topic_id': topic_id,
                    'connection_id': source['connection_id'],
                    'content': source['body'],
                    'slug': slug,
                    'score': score,
                    'bumped_at': source['bumped_at'],
                    'created_at': source['created_at'],
                    'url': topic_url,
                    '_id': _id,
                    'type': 'discourse_forum',
                }
                result_docs.append(r)

        logger.debug('all type index search keyword: %s, in project: %s , \nsearch result: %s', keyword,
                        project_uuid, result_docs)

        return result_docs
