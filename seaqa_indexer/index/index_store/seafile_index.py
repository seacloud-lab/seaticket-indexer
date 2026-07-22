import json
import logging

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.index.index_store.base_connection_index import BaseConnectionIndex
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT, ConnectionType

logger = logging.getLogger(__name__)


class SeafileIndex(BaseConnectionIndex):
    def update(self, index_name, connection_id, connection_status, seaqa_db, seafile_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        deleted_index_id_set = set()
        bulk_params = []

        while True:
            files = seafile_seadb_api.get_files_by_page(connection_id, since, page_size, offset)
            offset += page_size
            if not files:
                break

            for file in files:
                title = file.get('title')
                deleted = file.get('deleted')
                outdated = file.get('outdated')
                pk = file.get('_pk')
                key = str(pk)
                if deleted or outdated:
                    deleted_index_id_set.add(key)
                else:
                    doc = {
                        'connection_id': str(connection_id),
                        'title': title,
                        'modified_time': file.get('modified_time'),
                        'content': file.get('content'),
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
                        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status), ConnectionType.SEAFILE.value)
        if bulk_params:
            self.seasearch_api.bulk(index_name, bulk_params)

        if deleted_index_id_set:
            self.delete_indexes_by_ids(index_name, deleted_index_id_set)

        return index_count
