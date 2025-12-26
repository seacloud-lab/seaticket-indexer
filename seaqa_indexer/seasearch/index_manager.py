import logging
import json
import datetime
from urllib.parse import urlparse

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.constants import SUPPORT_VECTOR_RETRIEVAL_TYPES, ConnectionType, REPO_NAME_CACHE_TIME_OUT
from seaqa_indexer.seasearch.index_store.knowledge_base_index import KnowledgeBaseIndex
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.seadb_models.models import WebCrawlTable, DiscourseTopicsTable, GithubIssuesTable, SeafileTable, \
    ThreadTable, KnowledgeBaseTable, TicketsTable
from seaqa_indexer.seasearch.index_store.site_index import SiteIndex
from seaqa_indexer.seasearch.index_store.seafile_index import SeafileIndex
from seaqa_indexer.seasearch.index_store.github_issues_index import GitHubIssuesIndex
from seaqa_indexer.seasearch.index_store.discourse_index import DiscourseIndex
from seaqa_indexer.seasearch.index_store.email_index import EmailIndex

from seaqa_indexer.seasearch.index_store.summary_vector_index import SummaryVectorIndex
from seaqa_indexer.seasearch.index_store.content_vector_index import ContentVectorIndex
from seaqa_indexer.seasearch.index_store.knowledge_base_summary_vector_index import KnowledgeBaseSummaryVectorIndex
from seaqa_indexer.seasearch.index_store.ticket_summary_vector_index import TicketSummaryVectorIndex

from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.seasearch.utils import gen_index_name_by_type, gen_conn_summary_index_name, gen_content_vector_index_name, \
    gen_kb_index_name, gen_kb_summary_index_name, gen_ticket_summary_index_name
from seaqa_indexer.mq import get_mq
from seaqa_indexer.app.event_redis import redis_cache
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.ai_processor.utils import generate_embedding


logger = logging.getLogger(__name__)


class IndexManager:
    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        self.site_index = SiteIndex(self.seasearch_api)
        self.seafile_index = SeafileIndex(self.seasearch_api)
        self.github_issues_index = GitHubIssuesIndex(self.seasearch_api)
        self.discourse_index = DiscourseIndex(self.seasearch_api)
        self.email_index = EmailIndex(self.seasearch_api)
        self.knowledge_base_index = KnowledgeBaseIndex(self.seasearch_api)

        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.seadb_api = SeaDBAPI()

        self.summary_vector_index = SummaryVectorIndex(self.seasearch_api, self.seadb_api)
        self.content_vector_index = ContentVectorIndex(self.seasearch_api, self.seadb_api, storage=None)
        self.kb_summary_vector_index = KnowledgeBaseSummaryVectorIndex(self.seasearch_api, self.seadb_api)
        self.ticket_summary_vector_index = TicketSummaryVectorIndex(self.seasearch_api, self.seadb_api)

    def _build_connection_search_params(self, project_connections, keyword, start, size, time_from, time_to):
        bulk_search_params = []
        connection_id_to_config = {}
        for project_connection in project_connections:
            connection_type = project_connection.type
            connection_id = project_connection.id
            index_name = gen_index_name_by_type(connection_id, connection_type)
            connection_id_to_config[connection_id] = json.loads(project_connection.config)
            if connection_type == ConnectionType.SEAFILE.value:
                bulk_search_params.append(self.seafile_index.get_search_param(index_name, keyword, start, size, time_from, time_to))
            elif connection_type == ConnectionType.SITE.value:
                bulk_search_params.append(self.site_index.get_search_param(index_name, keyword, start, size, time_from, time_to))
            elif connection_type == ConnectionType.GITHUB_ISSUE.value:
                bulk_search_params.append(self.github_issues_index.get_search_param(index_name, keyword, start, size, time_from, time_to))
            elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
                bulk_search_params.append(self.discourse_index.get_search_param(index_name, keyword, start, size, time_from, time_to))
            elif connection_type == ConnectionType.EMAIL.value:
                bulk_search_params.append(self.email_index.get_search_param(index_name, keyword, start, size, time_from, time_to))
        return connection_id_to_config, bulk_search_params

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
        elif connection_type == ConnectionType.EMAIL.value:
            self.email_index.delete_index_by_index_name(index_name)

    def add_connection_index_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
            'type': 'connection',
        }
        self.mq.lpush('index_task', json.dumps(msg_content))

    def search(self, project_uuid, connection_ids, extra_sources, time_from, time_to, keyword, start=0, size=20):
        result_docs = []
        bulk_search_params = []

        if connection_ids:
            connection_id_list = connection_ids.split(',')
            project_connections = self.seaqa_db.get_connection_by_ids(project_uuid, connection_id_list)
            connection_id_to_config, connection_params = self._build_connection_search_params(
                project_connections, keyword, start, size, time_from, time_to
            )
            bulk_search_params.extend(connection_params)

        if extra_sources and "knowledge_base" in extra_sources:
            kb_index_name = gen_kb_index_name(project_uuid)
            knowledge_base_search_param = self.knowledge_base_index.get_search_param(kb_index_name, keyword, start, size, time_from, time_to)
            bulk_search_params.append(knowledge_base_search_param)

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
            try:
                _id = int(hit.get('_id'))
            except:
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

                url = server_url.rstrip('/') + '/' + 'lib' + '/' + repo_id + '/file/'
                if source['path'].strip('/'):
                    url += source['path'].strip('/') + '/'
                url += source['title']
                r = {
                    'title': source['title'],
                    'path': source['path'],
                    'repo_id': source['repo_id'],
                    'connection_id': source['connection_id'],
                    'content': source['content'],
                    'score': score,
                    'repo_name': repo_name,
                    'server_url': server_url,
                    'url': url,
                    'modified_time': source['modified_time'],
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
                r = {
                    'title': source['title'],
                    'connection_id': source['connection_id'],
                    'content': source['content'],
                    'score': score,
                    '_id': _id,
                    'type': 'email',
                }
                result_docs.append(r)
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
                    'content': source['content'],
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
                    'content': source['content'],
                    'slug': slug,
                    'score': score,
                    'modified_time': source['modified_time'],
                    'url': topic_url,
                    '_id': _id,
                    'type': 'discourse_forum',
                }
                result_docs.append(r)
            elif index_name.startswith("knowledge_base_"):
                r = {
                    'title': source.get('title'),
                    'content': source.get('content'),
                    'modified_time': source.get('modified_time'),
                    'score': score,
                    '_id': _id,
                    'type': 'knowledge_base',
                }
                result_docs.append(r)

        logger.debug('all type index search keyword: %s, in project: %s , \nsearch result: %s', keyword,
                        project_uuid, result_docs)

        return result_docs

    def semantic_search(self, project_uuid, connection_ids, extra_sources, query_text, username, time_from, time_to, top_k=10):
        project_connections = []
        if connection_ids:
            connection_id_list = connection_ids.split(',')
            project_connections = self.seaqa_db.get_connection_by_ids(project_uuid, connection_id_list) or []

        all_results = []
        per_connection_limit = max(top_k, 20)
        query_vector = generate_embedding(query_text, project_uuid, username=username)
        if not query_vector:
            logger.warning('Failed to generate query vector for: %s', query_text)
            return []

        summary_index_names = []
        content_index_names = []

        for project_connection in project_connections:
            connection_type = project_connection.type
            connection_id = project_connection.id

            summary_index_name = gen_conn_summary_index_name(connection_id, connection_type)
            summary_index_names.append(summary_index_name)

            if connection_type in SUPPORT_VECTOR_RETRIEVAL_TYPES:
                content_index_name = gen_content_vector_index_name(connection_id, connection_type)
                if self.content_vector_index.check_index(content_index_name):
                    content_index_names.append(content_index_name)

        # Batch search summary indexes
        if summary_index_names:
            try:
                summary_results = self.summary_vector_index.semantic_search(
                    summary_index_names, query_vector, per_connection_limit,
                    time_from=time_from, time_to=time_to
                )
                for result in summary_results:
                    result['source_type'] = 'summary'
                all_results.extend(summary_results)
            except Exception as e:
                logger.warning('Summary semantic search failed: %s', e)

        # Batch search content indexes
        if content_index_names:
            try:
                chunk_results = self.content_vector_index.semantic_search(
                    content_index_names, query_vector, per_connection_limit,
                    time_from=time_from, time_to=time_to
                )
                for result in chunk_results:
                    result['source_type'] = 'chunk'
                all_results.extend(chunk_results)
            except Exception as e:
                logger.exception(e)
                logger.warning('Chunk semantic search failed: %s', e)

        if extra_sources and 'knowledge_base' in extra_sources:

            kb_summary_index_name = gen_kb_summary_index_name(project_uuid)
            if self.kb_summary_vector_index.check_index(kb_summary_index_name):
                try:
                    kb_summary_results = self.kb_summary_vector_index.semantic_search(
                        kb_summary_index_name, query_vector, per_connection_limit,
                        time_from=time_from, time_to=time_to
                    )
                    for result in kb_summary_results:
                        result['source_type'] = 'knowledge_base_summary'
                    all_results.extend(kb_summary_results)
                except Exception as e:
                    logger.warning('Knowledge base summary semantic search failed: %s', e)
            else:
                logger.warning('Knowledge base summary index not found')

        if extra_sources and 'ticket' in extra_sources:
            ticket_summary_index_name = gen_ticket_summary_index_name(project_uuid)
            if self.ticket_summary_vector_index.check_index(ticket_summary_index_name):
                try:
                    ticket_summary_results = self.ticket_summary_vector_index.semantic_search(
                        ticket_summary_index_name, query_vector, per_connection_limit,
                        time_from=time_from, time_to=time_to
                    )
                    for result in ticket_summary_results:
                        result['source_type'] = 'ticket_summary'
                    all_results.extend(ticket_summary_results)
                except Exception as e:
                    logger.warning('Ticket summary semantic search failed: %s', e)
            else:
                logger.warning('Ticket summary index not found')

        all_results.sort(key=lambda x: x.get('score', 0), reverse=True)

        # encapsulate data
        connection_pks_map = {}  # {connection_id: {'pks': [pk1, pk2], 'results': [result1, result2]}}
        connection_objects = {}
        kb_pks = set()
        ticket_pks = set()

        for result in all_results:
            if result.get('source_type') == 'knowledge_base_summary':
                pk = result.get('pk')
                if pk is not None:
                    kb_pks.add(pk)
                continue

            if result.get('source_type') == 'ticket_summary':
                pk = result.get('pk')
                if pk is not None:
                    ticket_pks.add(pk)
                continue

            connection_id = int(result.get('connection_id'))
            pk = result.get('pk')

            if connection_id not in connection_pks_map:
                connection_pks_map[connection_id] = {'pks': [], 'results': []}

            connection_pks_map[connection_id]['pks'].append(pk)
            connection_pks_map[connection_id]['results'].append(result)

        for connection in project_connections:
            connection_objects[connection.id] = connection

        records_map = {}  # {connection_id: {pk: record}}
        for connection_id, data in connection_pks_map.items():
            if connection_id not in connection_objects:
                logger.warning(f'Connection {connection_id} not found or deleted')
                continue

            connection = connection_objects[connection_id]
            connection_type = connection.type
            table_name = None
            if connection_type == ConnectionType.SITE.value:
                table_name = WebCrawlTable.gen_table_name(connection_id)
            elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
                table_name = DiscourseTopicsTable.gen_table_name(connection_id)
            elif connection_type == ConnectionType.GITHUB_ISSUE.value:
                table_name = GithubIssuesTable.gen_table_name(connection_id)
            elif connection_type == ConnectionType.SEAFILE.value:
                table_name = SeafileTable.gen_table_name(connection_id)
            elif connection_type == ConnectionType.EMAIL.value:
                table_name = ThreadTable.gen_table_name(connection_id)

            if not table_name:
                logger.warning(f'Table name not found for connection {connection_id}')
                continue

            pks = data['pks']
            if not pks:
                continue

            try:
                pks_str = ','.join(map(str, pks))
                sql = f"SELECT * FROM `{table_name}` WHERE _pk IN ({pks_str})"
                res = self.seadb_api.query_rows(project_uuid, sql)
                records = res.get('results', [])

                # pk -> record
                records_map[connection_id] = {}
                for record in records:
                    record_pk = record.get('_pk')
                    records_map[connection_id][record_pk] = record
            except Exception as e:
                logger.error(f'Error batch querying records for connection {connection_id}: {e}')
                records_map[connection_id] = {}

        kb_records = {}
        if kb_pks:
            try:
                kb_table_name = KnowledgeBaseTable.gen_table_name()
                kb_pks_str = ','.join(map(str, kb_pks))
                sql = f"SELECT * FROM `{kb_table_name}` WHERE _pk IN ({kb_pks_str})"
                res = self.seadb_api.query_rows(project_uuid, sql)
                records = res.get('results', [])
                for record in records:
                    record_pk = record.get('_pk')
                    kb_records[record_pk] = record
                    kb_records[str(record_pk)] = record
            except Exception as e:
                logger.error(f'Error querying knowledge base records: {e}')
                kb_records = {}

        ticket_records = {}
        if ticket_pks:
            try:
                ticket_table_name = TicketsTable.gen_table_name()
                ticket_pks_str = ','.join(map(str, ticket_pks))
                sql = f"SELECT * FROM `{ticket_table_name}` WHERE _pk IN ({ticket_pks_str})"
                res = self.seadb_api.query_rows(project_uuid, sql)
                records = res.get('results', [])
                for record in records:
                    record_pk = record.get('_pk')
                    ticket_records[record_pk] = record
                    ticket_records[str(record_pk)] = record
            except Exception as e:
                logger.error(f'Error querying ticket records: {e}')
                ticket_records = {}

        processed_results = []
        for result in all_results:
            pk = result.get('pk')
            source_type = result.get('source_type')
            score = result.get('score')
            ai_summary = result.get('ai_summary', '')

            if source_type == 'knowledge_base_summary':
                record = kb_records.get(pk)
                if not record:
                    logger.warning(f'Knowledge base record not found for pk {pk}')
                    continue

                processed_result = {
                    '_id': pk,
                    'score': score,
                    'type': 'knowledge_base',
                    'content': ai_summary,
                    'title': record.get('title', ''),
                    'modified_time': record.get('modified_time', ''),
                    'source_type': source_type
                }
                processed_results.append(processed_result)
                continue

            if source_type == 'ticket_summary':
                record = ticket_records.get(pk)
                if not record:
                    logger.warning(f'Ticket record not found for pk {pk}')
                    continue

                processed_result = {
                    '_id': pk,
                    'score': score,
                    'type': 'ticket',
                    'content': ai_summary,
                    'title': record.get('title', ''),
                    'modified_time': record.get('modified_time', ''),
                    'source_type': source_type
                }
                processed_results.append(processed_result)
                continue

            connection_id = result.get('connection_id')
            if connection_id is None or int(connection_id) not in connection_objects:
                continue
            connection_id = int(connection_id)

            connection = connection_objects[connection_id]
            connection_type = connection.type

            record = records_map.get(connection_id, {}).get(pk)
            if not record:
                logger.warning(f'Record not found for connection {connection_id}, pk {pk}')
                continue

            processed_result = {
                '_id': pk,
                'connection_id': connection_id,
                'score': score,
                'type': connection_type,
                'content': ai_summary,
            }

            if connection_type == ConnectionType.SITE.value:
                url = record.get('url', '')
                if source_type == 'chunk':
                    processed_result['source_type'] = 'chunk'
                    processed_result['content'] = result.get('content', '')
                processed_result['title'] = record.get('title', '')
                processed_result['url'] = url

            elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
                connection_config = json.loads(connection.config)
                discourse_forum_url = connection_config.get('url')
                slug = record.get('slug')
                topic_id = record.get('topic_id')
                topic_url = discourse_forum_url.rstrip('/') + '/t/' + slug + '/' + str(topic_id)
                processed_result['title'] = record.get('title', '')
                processed_result['slug'] = record.get('slug', '')
                processed_result['topic_id'] = record.get('topic_id', '')
                processed_result['modified_time'] = record.get('modified_time', '')
                processed_result['url'] = topic_url

            elif connection_type == ConnectionType.SEAFILE.value:
                connection_config = json.loads(connection.config)
                repo_name = connection_config.get('repo_name')
                repo_id = connection_config.get('repo_id')
                server_url = connection_config.get('server_url')
                url = server_url.rstrip('/') + '/' + 'lib' + '/' + repo_id + '/file/' + record.get('path', '').strip('/') + '/' + record.get('title', '')
                processed_result['title'] = record.get('title', '')
                processed_result['path'] = record.get('path', '')
                processed_result['repo_id'] = repo_id
                processed_result['repo_name'] = repo_name
                processed_result['server_url'] = server_url
                processed_result['modified_time'] = record.get('modified_time', '')
                processed_result['url'] = url

            elif connection_type == ConnectionType.GITHUB_ISSUE.value:
                connection_config = json.loads(connection.config)
                server_url = connection_config.get('repository')
                url_parsed = urlparse(server_url)
                base_url = url_parsed.scheme + "://" + url_parsed.netloc
                parts = url_parsed.path.strip("/").split("/")
                repo_owner, repo_name = parts[0], parts[1]
                url = f'{base_url}/{repo_owner}/{repo_name}/issues/' + str(record.get('issue_number', ''))
                processed_result['title'] = record.get('title', '')
                processed_result['issue_number'] = record.get('issue_number', '')
                processed_result['state'] = record.get('state', '')
                processed_result['labels'] = record.get('labels')
                processed_result['repo_name'] = repo_name
                processed_result['repo_owner'] = repo_owner
                processed_result['url'] = url
            elif connection_type == ConnectionType.EMAIL.value:
                processed_result['title'] = record.get('title', '')
                processed_result['modified_time'] = record.get('modified_time', '')
            else:
                logger.warning(f'Unsupported connection type: {connection_type}')
                continue

            processed_results.append(processed_result)

        top_results = processed_results[:top_k]

        return top_results


    def summary_vector_search(self, project_uuid, connection_ids, query_vector, top_k=10, score_threshold=0.5):
        project_connections = self.seaqa_db.get_connection_by_ids(project_uuid, connection_ids)

        if not project_connections:
            return []

        all_results = []
        per_connection_limit = max(top_k, 20)

        for project_connection in project_connections:
            connection_type = project_connection.type
            connection_id = project_connection.id

            index_name = gen_conn_summary_index_name(connection_id, connection_type)

            index_exists = self.summary_vector_index.check_index(index_name)
            if not index_exists:
                logger.debug('Index %s does not exist, skipping connection %s', index_name, connection_id)
                continue

            try:
                search_results = self.summary_vector_index.semantic_search(
                        index_name, query_vector, per_connection_limit
                )
                filtered_results = [r for r in search_results if r.get('score', 0) >= score_threshold]
                all_results.extend(filtered_results)
            except Exception as e:
                logger.exception('Vector search failed for connection %s: %s', connection_id, e)
                continue

        all_results.sort(key=lambda x: x.get('score', 0), reverse=True)
        top_results = all_results[:top_k]

        return top_results
