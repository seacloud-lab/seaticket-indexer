import logging
import json

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.constants import INDEX_CONTENT_LIMIT, SEASEARCH_BULK_OPERATE_LIMIT
from seaqa_indexer.index.index_store.base_connection_index import BaseConnectionIndex


logger = logging.getLogger(__name__)


class SlackIndex(BaseConnectionIndex):
    def update(self, index_name, connection_id, connection_status, seaqa_db, slack_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        need_cleared_index_id_set = set()
        bulk_params = []
        while True:
            messages = slack_seadb_api.get_messages_by_page(connection_id, since, page_size, offset)
            offset += page_size
            if not messages:
                break
            for message in messages:
                message_id = message.get('message_id')
                pk = message.get('_pk')
                key = str(pk)
                if message.get('deleted') or message.get('outdated'):
                    need_cleared_index_id_set.add(key)
                else:
                    # Concatenate thread reply content for full-text search.
                    body_content = (message.get('content') or '') \
                        + slack_seadb_api.get_replies_for_index(connection_id, message_id)

                    doc = {
                        'connection_id': str(connection_id),
                        'title': message.get('title', ''),
                        'content': body_content[:INDEX_CONTENT_LIMIT],
                        'modified_time': message.get('modified_time'),
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
        if bulk_params:
            self.seasearch_api.bulk(index_name, bulk_params)
        if need_cleared_index_id_set:
            self.delete_indexes_by_ids(index_name, need_cleared_index_id_set)
        return index_count
