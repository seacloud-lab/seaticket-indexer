import logging
from seaqa_indexer.seasearch.index_store.ticket_summary_vector_index import TicketSummaryVectorIndex
from seaqa_indexer.seasearch.utils import gen_ticket_summary_index_name, fix_summary_by_tickets
from seaqa_indexer.seadb_models.models import TicketsTable

logger = logging.getLogger(__name__)

def update_tickets_summary_index(project_uuid, seaqa_db, seadb_api, seasearch_api):
    logger.info('Start to index summary vectors for tickets of project: %s', project_uuid)

    ticket = seaqa_db.get_ticket_ai_info_by_project_uuid(project_uuid)
    if not ticket:
        logger.warning('Project %s does not exist', project_uuid)
        return

    last_ticket_ai_indexed_at = ticket.ticket_ai_indexed_at

    table_name = TicketsTable.gen_table_name()

    fixed_count = fix_summary_by_tickets(project_uuid, table_name, seadb_api)
    if fixed_count > 0:
        logger.info('Fixed %d missing embeddings for tickets of project: %s before indexing', fixed_count, project_uuid)

    summary_vector_index_ticket = TicketSummaryVectorIndex(seasearch_api, seadb_api)
    ai_index_name = gen_ticket_summary_index_name(project_uuid)
    summary_vector_index_ticket.create_index_if_missing(ai_index_name)

    index_count = summary_vector_index_ticket.update_tickets(
        table_name=table_name,
        index_name=ai_index_name,
        project_uuid=project_uuid,
        seaqa_db=seaqa_db,
        last_ticket_ai_indexed_at=last_ticket_ai_indexed_at,
    )
    return index_count