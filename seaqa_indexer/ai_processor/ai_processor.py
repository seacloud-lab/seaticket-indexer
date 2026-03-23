import logging
import datetime
import json

from seaqa_indexer.ai_processor.utils import generate_summary
from seaqa_indexer.seadb_models.models import (
    KnowledgeBaseTable,
    TicketsTable
)
from seaqa_indexer.seadb_models.knowledge_base_seadb_api import KnowledgeBaseSeaDBAPI
from seaqa_indexer.utils.constants import AIStatus
from seaqa_indexer.seadb_models.ticket_seadb_api import TicketSeaDBAPI
from seaqa_indexer.handlers import get_handler

logger = logging.getLogger(__name__)

BATCH_SIZE = 50

def generate_summaries_and_vectors(seadb_api, storage, seaqa_db, mq, connection_id):
    logger.info('start generate summaries and vectors, connection: %s', connection_id)
    project_connection = seaqa_db.get_project_connection_by_id(connection_id)
    if not project_connection:
        logger.warning('connection: %s not found', connection_id)
        return

    connection_is_active = project_connection.is_active
    if not connection_is_active:
        logger.warning('connection: %s is inactive', connection_id)
        return

    project_uuid = project_connection.project_uuid
    connection_type = project_connection.type
    ai_status = project_connection.ai_status
    if not ai_status:
        ai_status = {
            'last_ai_processing_status': AIStatus.PENDING,
            'last_ai_processing_count': 0,
        }
    else:
        ai_status = json.loads(ai_status)

    ai_status['last_ai_processing_status'] = AIStatus.PROCESSING
    seaqa_db.update_ai_status_and_time(connection_id, json.dumps(ai_status))

    try:
        conn_handler = get_handler(connection_type)
        if not conn_handler:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            return
        
        ai_processing_count = conn_handler.process_ai(seadb_api, storage, connection_id, project_uuid, mq=mq)
        
        logger.info('AI processing completed for connection %s', connection_id)
        ai_status['last_ai_processing_status'] = AIStatus.COMPLETED
        ai_status['last_ai_processing_count'] = ai_processing_count
        last_ai_processing_time = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_ai_status_and_time(connection_id, json.dumps(ai_status), last_ai_processing_time)
        summary_index_msg = {
            'connection_id': connection_id,
            'type': 'connection',
        }
        mq.lpush('summary_index_task', json.dumps(summary_index_msg))
    except Exception as e:
        logger.exception('Connection %s process AI generate summaries and vectors failed, error: %s', connection_id, e)
        ai_status['last_ai_processing_status'] = AIStatus.FAILED
        seaqa_db.update_ai_status_and_time(connection_id, json.dumps(ai_status))
    logger.info('Finish generate summaries and vectors, connection: %s', connection_id)

def process_knowledge_base_ai(seadb_api, project_uuid):
    page_size = 100
    updates = []
    table_name = KnowledgeBaseTable.gen_table_name()
    knowledge_base_seadb_api = KnowledgeBaseSeaDBAPI(project_uuid)

    logger.info('Start AI process knowledge base of project: %s', project_uuid)
    ai_processing_count = 0
    while True:
        rows = knowledge_base_seadb_api.get_need_ai_knowledge_bases_by_page(page_size)
        if not rows:
            break
        for row in rows:
            pk = row.get('_pk')
            title = row.get('title', '')
            content = row.get('content', '')
            full_content = f"Title: {title}\n\nContent: {content}\n"
            summary, vector = generate_summary(full_content, 'knowledge_base', project_uuid)
            updates.append({
                "pk": pk,
                "row": {
                    "ai_summary": summary,
                    "ai_summary_vector": vector,
                    "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                }
            })
            ai_processing_count += 1
            if len(updates) >= BATCH_SIZE:
                seadb_api.update_rows(project_uuid, table_name, updates)
                updates = []
        if updates:
            seadb_api.update_rows(project_uuid, table_name, updates)

    logger.info('Finish AI process knowledge base of project: %s', project_uuid)

    return ai_processing_count


def process_ticket_ai(seadb_api, project_uuid, mq=None):
    page_size = 100
    updates = []
    table_name = TicketsTable.gen_table_name()
    ticket_seadb_api = TicketSeaDBAPI(project_uuid)

    logger.info('Start AI process tickets of project: %s', project_uuid)
    ai_processing_count = 0
    while True:
        rows = ticket_seadb_api.get_need_ai_tickets_by_page(page_size)
        if not rows:
            break
        for row in rows:
            pk = row.get('_pk')
            title = row.get('title', '')
            content = row.get('content', '')
            full_content = f"Title: {title}\n\nContent: {content}\n"
            summary, vector = generate_summary(full_content, 'ticket', project_uuid)
            updates.append({
                "pk": pk,
                "row": {
                    "ai_summary": summary,
                    "ai_summary_vector": vector,
                    "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                }
            })
            ai_processing_count += 1
            if len(updates) >= BATCH_SIZE:
                seadb_api.update_rows(project_uuid, table_name, updates)
                updates = []
            if mq and pk is not None:
                try:
                    event = json.dumps({
                        'event_type': 'ai_summary_updated',
                        'source_type': 'ticket',
                        'project_uuid': project_uuid,
                        'record_id': pk,
                    })
                    mq.publish('data_events', event)
                except Exception as e:
                    logger.warning('Failed to publish data_events for ticket pk=%s: %s', pk, e)
        if updates:
            seadb_api.update_rows(project_uuid, table_name, updates)

    logger.info('Finish AI process tickets of project: %s', project_uuid)

    return ai_processing_count
