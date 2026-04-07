import json
import logging
from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.constants import INDEX_CONTENT_LIMIT, SEASEARCH_BULK_OPERATE_LIMIT
from seaqa_indexer.index.index_store.base_connection_index import BaseConnectionIndex

logger = logging.getLogger(__name__)


class NotionIndex(BaseConnectionIndex):
    def update(self, index_name, connection_id, connection_status, seaqa_db, notion_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        need_cleared_pages_index_set = set()
        bulk_params = []

        while True:
            pages = notion_seadb_api.get_notion_pages_by_page(connection_id, since, page_size, offset)
            if not pages:
                break

            for notion_page in pages:
                pk = notion_page.get('_pk')
                key = str(pk)
                if notion_page.get('deleted') or notion_page.get('outdated'):
                    need_cleared_pages_index_set.add(key)
                else:
                    content = notion_page.get('content') or ''
                    content = content[:INDEX_CONTENT_LIMIT]

                    doc = {
                        'connection_id': str(connection_id),
                        'title': notion_page.get('title'),
                        'content': content,
                        'modified_time': notion_page.get('modified_time'),
                    }
                    index_info = {'index': {'_index': index_name, '_id': key}}
                    bulk_params.append(index_info)
                    bulk_params.append(doc)
                    if len(bulk_params) >= SEASEARCH_BULK_OPERATE_LIMIT:
                        self.seasearch_api.bulk(index_name, bulk_params)
                        bulk_params = []

                    index_count += 1
                    if need_update_connection_status(index_count):
                        connection_status['last_indexed_count'] = index_count
                        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
            offset += page_size

        if bulk_params:
            self.seasearch_api.bulk(index_name, bulk_params)

        if need_cleared_pages_index_set:
            self.delete_indexes_by_ids(index_name, need_cleared_pages_index_set)

        return index_count
