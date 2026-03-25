import logging

from seaqa_indexer.index.index_store.base_extra_source_index import BaseExtraSourceIndex
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT

logger = logging.getLogger(__name__)


class TicketIndex(BaseExtraSourceIndex):
    def update(self, index_name, project_uuid, ticket_seadb_api, since):
        page_size = 1000
        bulk_params = []
        deleted_ids = set()
        deleted_pk_set = set()
        indexed_count = 0

        offset = 0
        while True:
            tickets = ticket_seadb_api.get_tickets_by_page(since, page_size, offset)
            offset += page_size
            if not tickets:
                break
            for row in tickets:
                pk = row.get('_pk')
                title = row.get('title')
                content = row.get('content')
                deleted = row.get('deleted')
                doc_id = str(pk)
                if deleted:
                    deleted_ids.add(doc_id)
                    deleted_pk_set.add(pk)
                    continue
                index_info = {'index': {'_index': index_name, '_id': doc_id}}
                doc_info = {
                    'project_uuid': project_uuid,
                    'title': title,
                    'content': content,
                    'modified_time': row.get('modified_time')
                }
                bulk_params.append(index_info)
                bulk_params.append(doc_info)
                indexed_count += 1
                if len(bulk_params) >= SEASEARCH_BULK_OPERATE_LIMIT:
                    self.seasearch_api.bulk(index_name, bulk_params)
                    bulk_params = []

            offset += page_size
        if bulk_params:
            self.seasearch_api.bulk(index_name, bulk_params)
        if deleted_ids:
            self.delete_indexes_by_ids(index_name, deleted_ids)

        return indexed_count

