import logging
import json

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.constants import SUPPORT_VECTOR_RETRIEVAL_TYPES, ConnectionType, AIScenario
from seaqa_indexer.index.index_store.knowledge_base_index import KnowledgeBaseIndex
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

from seaqa_indexer.index.index_store.summary_vector_index import SummaryVectorIndex
from seaqa_indexer.index.index_store.content_vector_index import ContentVectorIndex
from seaqa_indexer.index.index_store.knowledge_base_summary_vector_index import KnowledgeBaseSummaryVectorIndex
from seaqa_indexer.index.index_store.ticket_summary_vector_index import TicketSummaryVectorIndex
from seaqa_indexer.index.index_store.portal_issue_summary_vector_index import PortalIssueSummaryVectorIndex

from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.index.utils import gen_index_name_by_type, gen_conn_summary_index_name, gen_content_vector_index_name, \
    gen_kb_index_name, gen_kb_summary_index_name, gen_ticket_index_name, gen_ticket_summary_index_name, \
    gen_portal_issue_index_name, gen_portal_issue_summary_index_name
from seaqa_indexer.mq import get_mq
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.ai_processor.utils import generate_embedding
from seaqa_indexer.index.index_store.ticket_index import TicketIndex
from seaqa_indexer.index.index_store.portal_issue_index import PortalIssueIndex
from seaqa_indexer.handlers import get_handler


logger = logging.getLogger(__name__)

def format_vector_search_result(result, connection_id_to_connection):

    pk = result.get('pk')
    source_type = result.get('source_type')
    score = result.get('score')
    modified_time = result.get('modified_time', '')

    formatted_result = {
        '_id': pk,
        'score': score,
        'source_type': source_type,
        'modified_time': modified_time
    }

    if 'ai_summary' in result:
        formatted_result['ai_summary'] = result['ai_summary']

    if source_type == 'knowledge_base_summary':
        formatted_result['type'] = 'knowledge_base'

    elif source_type == 'ticket_summary':
        formatted_result['type'] = 'ticket'

    elif source_type == 'portal_issue_summary':
        formatted_result['type'] = 'portal_issue'

    else:
        # format connection vector search result
        connection_id = result.get('connection_id')
        connection = connection_id_to_connection.get(int(connection_id))
        connection_type = connection.type

        formatted_result['connection_id'] = connection_id
        formatted_result['type'] = connection_type

        if source_type == 'chunk':
            formatted_result['snippet'] = result.get('content', '')

    return formatted_result


class IndexManager:
    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        self.knowledge_base_index = KnowledgeBaseIndex(self.seasearch_api)
        self.ticket_index = TicketIndex(self.seasearch_api)
        self.portal_issue_index = PortalIssueIndex(self.seasearch_api)

        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.seadb_api = SeaDBAPI()

        self.summary_vector_index = SummaryVectorIndex(self.seasearch_api)
        self.content_vector_index = ContentVectorIndex(self.seasearch_api)
        self.kb_summary_vector_index = KnowledgeBaseSummaryVectorIndex(self.seasearch_api)
        self.ticket_summary_vector_index = TicketSummaryVectorIndex(self.seasearch_api)
        self.portal_issue_summary_vector_index = PortalIssueSummaryVectorIndex(self.seasearch_api)

    def _build_search_params(self, project_uuid, connection_ids, extra_sources, keyword, start, size, time_from, time_to):
        bulk_search_params = []
        if connection_ids:
            connection_id_list = connection_ids.split(',')
            project_connections = self.seaqa_db.get_connection_by_ids(project_uuid, connection_id_list)
            for project_connection in project_connections:
                connection_type = project_connection.type
                connection_id = project_connection.id
                index_name = gen_index_name_by_type(connection_id, connection_type)

                conn_handler = get_handler(connection_type)
                if not conn_handler:
                    logger.warning('Unsupported connection type: %s', connection_type)
                    continue
                bulk_search_params.append(conn_handler.get_search_param(
                    self.seasearch_api, index_name, keyword, start, size, time_from, time_to
                ))

        if extra_sources and "knowledge_base" in extra_sources:
            kb_index_name = gen_kb_index_name(project_uuid)
            knowledge_base_search_param = self.knowledge_base_index.get_search_param(kb_index_name, keyword, start, size, time_from, time_to)
            bulk_search_params.append(knowledge_base_search_param)

        if extra_sources and "ticket" in extra_sources:
            ticket_index_name = gen_ticket_index_name(project_uuid)
            ticket_search_param = self.ticket_index.get_search_param(ticket_index_name, keyword, start, size, time_from, time_to)
            bulk_search_params.append(ticket_search_param)

        if extra_sources and "portal_issue" in extra_sources:
            portal_issue_index_name = gen_portal_issue_index_name(project_uuid)
            portal_issue_search_param = self.portal_issue_index.get_search_param(
                portal_issue_index_name, keyword, start, size, time_from, time_to
            )
            bulk_search_params.append(portal_issue_search_param)
        return bulk_search_params

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

        portal_issue_index_name = gen_portal_issue_index_name(project_uuid)
        self.portal_issue_index.delete_index_by_index_name(portal_issue_index_name)
        portal_issue_summary_index_name = gen_portal_issue_summary_index_name(project_uuid)
        self.seasearch_api.delete_index_by_name(portal_issue_summary_index_name)

    def delete_index_by_connection(self, connection_id, connection_type):
        conn_handler = get_handler(connection_type)
        if not conn_handler:
            logger.warning('Unsupported connection type: %s', connection_type)
            return
        conn_handler.delete_index_by_connection_id(self.seasearch_api, connection_id)

    def add_connection_index_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
            'type': 'connection',
        }
        self.mq.lpush('index_task', json.dumps(msg_content))

    def keyword_search(self, project_uuid, connection_ids, extra_sources, time_from, time_to, keyword, start=0, size=20):
        result_docs = []
        bulk_search_params = self._build_search_params(project_uuid, connection_ids, extra_sources, keyword, start, size, time_from, time_to)

        if not bulk_search_params:
            return []

        query_body = json.dumps({
            'index_queries': bulk_search_params
        })
        results = self.seasearch_api.unified_search(query_body)

        hits = results.get('hits', []).get('hits', [])

        if not hits:
            return result_docs

        all_source_types = ConnectionType.to_list() + ['knowledge_base', 'ticket', 'portal_issue']
        for hit in hits:
            index_name = hit.get('_index')
            source = hit.get('_source')
            score = hit.get('_score')
            try:
                _id = int(hit.get('_id'))
            except:
                _id = hit.get('_id')

            s_type = None
            for source_type in all_source_types:
                if index_name.startswith(source_type):
                    s_type = source_type
                    break

            if not s_type:
                continue

            r = {
                '_id': _id,
                'type': s_type,
                'score': score,
                'title': source.get('title', ''),
                'content': source.get('content', ''),
                'modified_time': source.get('modified_time')
            }
            if s_type not in ('knowledge_base', 'ticket', 'portal_issue'):
                r['connection_id'] = source['connection_id']
            result_docs.append(r)

        logger.debug('all type index search keyword: %s, in project: %s , \nsearch result: %s', keyword,
                        project_uuid, result_docs)

        return result_docs

    def get_vector_search_results(self, project_connections, extra_sources, project_uuid, query_vector, count, time_from=None, time_to=None):
        summary_index_names = []
        content_index_names = []
        kb_summary_index_name = None
        ticket_summary_index_name = None
        portal_issue_summary_index_name = None

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
        if extra_sources and 'portal_issue' in extra_sources:
            portal_issue_summary_index_name = gen_portal_issue_summary_index_name(project_uuid)

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

        if portal_issue_summary_index_name:
            try:
                portal_issue_summary_results = self.portal_issue_summary_vector_index.semantic_search(
                    portal_issue_summary_index_name, query_vector, count, time_from=time_from, time_to=time_to
                )
                all_results.extend(portal_issue_summary_results)
            except Exception as e:
                logger.warning('Portal issue summary semantic search failed: %s', e)

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

        formatted_results = []
        for result in all_results:
            formatted_result = format_vector_search_result(result, connection_id_to_connection)
            if formatted_result:
                formatted_results.append(formatted_result)

        return formatted_results

    def vector_search_by_text(self, project_uuid, connection_ids, extra_sources, query_text, username, time_from=None, time_to=None, top_k=10):

        query_vector = generate_embedding(query_text, project_uuid, username=username, scenario=AIScenario.SEARCH.value)
        if not query_vector:
            logger.warning('Failed to generate query vector for: %s', query_text)
            return []

        return self.vector_search(project_uuid, connection_ids, extra_sources, query_vector, time_from, time_to, top_k)
