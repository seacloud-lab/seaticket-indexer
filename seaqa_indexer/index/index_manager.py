import logging
import json
from urllib.parse import urlparse

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.constants import SUPPORT_VECTOR_RETRIEVAL_TYPES, ConnectionType, REPO_NAME_CACHE_TIME_OUT
from seaqa_indexer.index.index_store.knowledge_base_index import KnowledgeBaseIndex
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.index.index_store.site_index import SiteIndex
from seaqa_indexer.index.index_store.seafile_index import SeafileIndex
from seaqa_indexer.index.index_store.github_issues_index import GitHubIssuesIndex
from seaqa_indexer.index.index_store.discourse_index import DiscourseIndex
from seaqa_indexer.index.index_store.email_index import EmailIndex

from seaqa_indexer.index.index_store.summary_vector_index import SummaryVectorIndex
from seaqa_indexer.index.index_store.content_vector_index import ContentVectorIndex
from seaqa_indexer.index.index_store.knowledge_base_summary_vector_index import KnowledgeBaseSummaryVectorIndex
from seaqa_indexer.index.index_store.ticket_summary_vector_index import TicketSummaryVectorIndex

from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.index.utils import gen_index_name_by_type, gen_conn_summary_index_name, gen_content_vector_index_name, \
    gen_kb_index_name, gen_kb_summary_index_name, gen_ticket_index_name, gen_ticket_summary_index_name
from seaqa_indexer.mq import get_mq
from seaqa_indexer.app.event_redis import redis_cache
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.ai_processor.utils import generate_embedding
from seaqa_indexer.index.index_store.ticket_index import TicketIndex
from seaqa_indexer.handlers import get_handler
from seaqa_indexer.seadb_models.models import KnowledgeBaseTable, TicketsTable


logger = logging.getLogger(__name__)


def get_result_related_seadb_records(project_uuid, all_results, connection_id_to_connection, seadb_api):
    def query_records_by_pks(table_name, pks):
        pks_str = ','.join(map(str, pks))
        sql = f"SELECT * FROM `{table_name}` WHERE _pk IN ({pks_str})"
        res = seadb_api.query_rows(project_uuid, sql)
        return res.get('results', [])

    connection_id_to_pks = {}  # {connection_id: [pk1, pk2]}
    kb_pks = set()
    ticket_pks = set()

    for result in all_results:
        if result.get('source_type') == 'knowledge_base_summary':
            kb_pks.add(result.get('pk'))
        elif result.get('source_type') == 'ticket_summary':
            ticket_pks.add(result.get('pk'))
        else:
            connection_id = int(result.get('connection_id'))
            if connection_id not in connection_id_to_pks:
                connection_id_to_pks[connection_id] = []
            connection_id_to_pks[connection_id].append(result.get('pk'))

    connection_pk_to_record = {}  # {connection_id: {pk: record}}
    for connection_id, connection_pks in connection_id_to_pks.items():
        connection = connection_id_to_connection[connection_id]
        handler = get_handler(connection.type)
        if not handler:
            logger.warning(f'Handler not found for connection {connection_id}')
            continue
        connection_table_name = handler.gen_table_name(connection_id)

        records = query_records_by_pks(connection_table_name, connection_pks)
        for record in records:
            if connection_id not in connection_pk_to_record:
                connection_pk_to_record[connection_id] = {}
            connection_pk_to_record[connection_id][record.get('_pk')] = record

    kb_pk_to_record = {}
    if kb_pks:
        kb_table_name = KnowledgeBaseTable.gen_table_name()
        records = query_records_by_pks(kb_table_name, kb_pks)
        kb_pk_to_record = {record.get('_pk'): record for record in records}

    ticket_pk_to_record = {}
    if ticket_pks:
        ticket_table_name = TicketsTable.gen_table_name()
        records = query_records_by_pks(ticket_table_name, ticket_pks)
        ticket_pk_to_record = {record.get('_pk'): record for record in records}
    return connection_pk_to_record, kb_pk_to_record, ticket_pk_to_record


def format_vector_search_result(result, connection_id_to_connection, kb_pk_to_record, ticket_pk_to_record, connection_pk_to_record):
    pk = result.get('pk')
    source_type = result.get('source_type')
    score = result.get('score')
    ai_summary = result.get('ai_summary', '')

    formatted_result = {
        '_id': pk,
        'score': score,
        'content': ai_summary,
        'source_type': source_type,
    }

    if source_type == 'knowledge_base_summary':
        record = kb_pk_to_record.get(int(pk))
        formatted_result['type'] = 'knowledge_base'
        formatted_result['title'] = record.get('title', '')
        formatted_result['modified_time'] = record.get('modified_time', '')

    elif source_type == 'ticket_summary':
        record = ticket_pk_to_record.get(int(pk))
        formatted_result['type'] = 'ticket'
        formatted_result['title'] = record.get('title', '')
        formatted_result['modified_time'] = record.get('modified_time', '')

    else:
        # format connection vector search result
        connection_id = result.get('connection_id')
        connection = connection_id_to_connection.get(int(connection_id))
        record = connection_pk_to_record.get(int(connection_id), {}).get(int(pk))
        connection_type = connection.type

        formatted_result['connection_id'] = connection_id
        formatted_result['type'] = connection_type

        conn_handler = get_handler(connection_type)
        if not conn_handler:
            logger.warning(f'Unsupported connection type: {connection_type}')
            return
        type_specific_result = conn_handler.format_vector_search_result(record, connection, result)
        formatted_result.update(type_specific_result)

    return formatted_result


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
        self.ticket_index = TicketIndex(self.seasearch_api)

        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.seadb_api = SeaDBAPI()

        self.summary_vector_index = SummaryVectorIndex(self.seasearch_api)
        self.content_vector_index = ContentVectorIndex(self.seasearch_api)
        self.kb_summary_vector_index = KnowledgeBaseSummaryVectorIndex(self.seasearch_api)
        self.ticket_summary_vector_index = TicketSummaryVectorIndex(self.seasearch_api)

    def _build_search_params(self, project_uuid, connection_ids, extra_sources, keyword, start, size, time_from, time_to):
        bulk_search_params = []
        connection_id_to_config = {}
        if connection_ids:
            connection_id_list = connection_ids.split(',')
            project_connections = self.seaqa_db.get_connection_by_ids(project_uuid, connection_id_list)
            for project_connection in project_connections:
                connection_type = project_connection.type
                connection_id = project_connection.id
                index_name = gen_index_name_by_type(connection_id, connection_type)

                connection_id_to_config[str(connection_id)] = json.loads(project_connection.config)
                conn_handler = get_handler(connection_type)
                if not conn_handler:
                    logger.warning('Unsupported connection type: %s', connection_type)
                    continue
                index_instance = conn_handler.get_index_instance(self.seasearch_api)
                bulk_search_params.append(conn_handler.get_search_param(
                    index_instance, index_name, keyword, start, size, time_from, time_to
                ))

        if extra_sources and "knowledge_base" in extra_sources:
            kb_index_name = gen_kb_index_name(project_uuid)
            knowledge_base_search_param = self.knowledge_base_index.get_search_param(kb_index_name, keyword, start, size, time_from, time_to)
            bulk_search_params.append(knowledge_base_search_param)

        if extra_sources and "ticket" in extra_sources:
            ticket_index_name = gen_ticket_index_name(project_uuid)
            ticket_search_param = self.ticket_index.get_search_param(ticket_index_name, keyword, start, size, time_from, time_to)
            bulk_search_params.append(ticket_search_param)
        return connection_id_to_config, bulk_search_params

    def delete_index_by_project(self, project_uuid):
        project_connections = self.seaqa_db.get_connections_by_project_uuid(project_uuid)
        for project_connection in project_connections:
            connection_id = project_connection.id
            connection_type = project_connection.type
            self.delete_index_by_connection(connection_id, connection_type)

        kb_index_name = gen_kb_index_name(project_uuid)
        self.knowledge_base_index.delete_index_by_index_name(kb_index_name)
        kb_summary_index_name = gen_kb_summary_index_name(project_uuid)
        self.seasearch_api.delete_index_by_name(kb_summary_index_name)

        ticket_index_name = gen_ticket_index_name(project_uuid)
        self.ticket_index.delete_index_by_index_name(ticket_index_name)
        ticket_summary_index_name = gen_ticket_summary_index_name(project_uuid)
        self.seasearch_api.delete_index_by_name(ticket_summary_index_name)

    def delete_index_by_connection(self, connection_id, connection_type):
        # summary vector index
        summary_index_name = gen_conn_summary_index_name(connection_id, connection_type)
        self.summary_vector_index.delete_index_by_index_name(summary_index_name)

        conn_handler = get_handler(connection_type)
        if not conn_handler:
            logger.warning('Unsupported connection type: %s', connection_type)
            return
        conn_handler.delete_index(self, connection_id)

    def add_connection_index_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
            'type': 'connection',
        }
        self.mq.lpush('index_task', json.dumps(msg_content))

    def search(self, project_uuid, connection_ids, extra_sources, time_from, time_to, keyword, start=0, size=20):
        result_docs = []
        connection_id_to_config, bulk_search_params = self._build_search_params(project_uuid, connection_ids, extra_sources, keyword, start, size, time_from, time_to)

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
            elif index_name.startswith("ticket_"):
                r = {
                    'title': source.get('title'),
                    'content': source.get('content'),
                    'modified_time': source.get('modified_time'),
                    'score': score,
                    '_id': _id,
                    'type': 'ticket',
                }
                result_docs.append(r)

        logger.debug('all type index search keyword: %s, in project: %s , \nsearch result: %s', keyword,
                        project_uuid, result_docs)

        return result_docs

    def get_vector_search_results(self, project_connections, extra_sources, project_uuid, query_vector, count, time_from=None, time_to=None):
        summary_index_names = []
        content_index_names = []
        kb_summary_index_name = None
        ticket_summary_index_name = None

        for project_connection in project_connections:
            connection_type = project_connection.type
            connection_id = project_connection.id

            summary_index_name = gen_conn_summary_index_name(connection_id, connection_type)
            summary_index_names.append(summary_index_name)

            if connection_type in SUPPORT_VECTOR_RETRIEVAL_TYPES:
                content_index_name = gen_content_vector_index_name(connection_id, connection_type)
                if self.content_vector_index.check_index(content_index_name):
                    content_index_names.append(content_index_name)
        if extra_sources and 'knowledge_base' in extra_sources:
            kb_summary_index_name = gen_kb_summary_index_name(project_uuid)
        if extra_sources and 'ticket' in extra_sources:
            ticket_summary_index_name = gen_ticket_summary_index_name(project_uuid)

        all_results = []
        if summary_index_names:
            try:
                summary_results = self.summary_vector_index.semantic_search(
                    summary_index_names, query_vector, count, time_from=time_from, time_to=time_to
                )
                all_results.extend(summary_results)
            except Exception as e:
                logger.warning('summary vector search failed: %s', e)

        if content_index_names:
            try:
                chunk_results = self.content_vector_index.semantic_search(
                    content_index_names, query_vector, count, time_from=time_from, time_to=time_to
                )
                all_results.extend(chunk_results)
            except Exception as e:
                logger.warning('Chunk semantic search failed: %s', e)

        if kb_summary_index_name:
            try:
                kb_summary_results = self.kb_summary_vector_index.semantic_search(
                    kb_summary_index_name, query_vector, count, time_from=time_from, time_to=time_to
                )
                all_results.extend(kb_summary_results)
            except Exception as e:
                logger.warning('Knowledge base summary semantic search failed: %s', e)
        if ticket_summary_index_name:
            try:
                ticket_summary_results = self.ticket_summary_vector_index.semantic_search(
                    ticket_summary_index_name, query_vector, count, time_from=time_from, time_to=time_to
                )
                all_results.extend(ticket_summary_results)
            except Exception as e:
                logger.warning('Ticket summary semantic search failed: %s', e)

        return all_results

    def vector_search(self, project_uuid, connection_ids, extra_sources, query_vector, time_from=None, time_to=None, top_k=10, score_threshold=None):
        project_connections = []
        if connection_ids:
            connection_id_list = connection_ids if isinstance(connection_ids, list) else connection_ids.split(',')
            project_connections = self.seaqa_db.get_connection_by_ids(project_uuid, connection_id_list) or []

        result_count = max(top_k, 20)
        all_results = self.get_vector_search_results(project_connections, extra_sources, project_uuid, query_vector, result_count, time_from, time_to)

        if score_threshold:
            all_results = [r for r in all_results if r.get('score', 0) >= score_threshold]
        all_results.sort(key=lambda x: x.get('score', 0), reverse=True)
        all_results = all_results[:top_k]

        connection_id_to_connection = {connection.id: connection for connection in project_connections}
        connection_pk_to_record, kb_pk_to_record, ticket_pk_to_record = \
            get_result_related_seadb_records(project_uuid, all_results, connection_id_to_connection, self.seadb_api)

        formatted_results = []
        for result in all_results:
            formatted_result = format_vector_search_result(result, connection_id_to_connection, kb_pk_to_record, ticket_pk_to_record, connection_pk_to_record)
            if formatted_result:
                formatted_results.append(formatted_result)

        return formatted_results

    def semantic_search(self, project_uuid, connection_ids, extra_sources, query_text, username, time_from=None, time_to=None, top_k=10):

        query_vector = generate_embedding(query_text, project_uuid, username=username)
        if not query_vector:
            logger.warning('Failed to generate query vector for: %s', query_text)
            return []

        return self.vector_search(project_uuid, connection_ids, extra_sources, query_vector, time_from, time_to, top_k)
