import logging
import json

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.constants import INDEX_CONTENT_LIMIT
from seaqa_indexer.index.index_store.base_connection_index import BaseConnectionIndex
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT


logger = logging.getLogger(__name__)


class DiscordIndex(BaseConnectionIndex):
    def update(self, index_name, connection_id, connection_status, seaqa_db, discord_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        need_cleared_message_index_id_set = set()
        bulk_params = []
        while True:
            messages = discord_seadb_api.get_messages_by_page(connection_id, since, page_size, offset)
            offset += page_size
            if not messages:
                break
            for msg in messages:
                message_id = msg.get('message_id')
                pk = msg.get('_pk')
                key = str(pk)
                if msg.get('deleted') or msg.get('outdated'):
                    need_cleared_message_index_id_set.add(key)
                else:
                    # Gather reply content for full-text search
                    body_content = discord_seadb_api.get_message_replies_for_index(connection_id, message_id)

                    doc = {
                        'connection_id': str(connection_id),
                        'title': msg.get('title', ''),
                        'content': (msg.get('content') or '') + ' ' + body_content,
                        'modified_time': msg.get('modified_time'),
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
        if need_cleared_message_index_id_set:
            self.delete_indexes_by_ids(index_name, need_cleared_message_index_id_set)
        return index_count
