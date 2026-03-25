import logging
import json

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.constants import INDEX_CONTENT_LIMIT
from seaqa_indexer.index.index_store.base_connection_index import BaseConnectionIndex
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT


logger = logging.getLogger(__name__)


class DiscourseIndex(BaseConnectionIndex):
    def update(self, index_name, connection_id, connection_status, seaqa_db, discourse_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        need_cleared_topic_index_id_set = set()
        bulk_params = []
        while True:
            topics = discourse_seadb_api.get_topics_by_page(connection_id, since, page_size, offset)
            offset += page_size
            if not topics:
                break
            for topic in topics:
                topic_id = topic.get('topic_id')
                pk = topic.get('_pk')
                key = str(pk)
                if topic.get('deleted') or topic.get('outdated'):
                    need_cleared_topic_index_id_set.add(key)
                else:
                    replies_offset = 0
                    replies_page_size = 1000
                    body_content = ''
                    while True:
                        replies_data = discourse_seadb_api.get_topic_replies_by_page(connection_id, topic_id,
                                                                                     replies_page_size, replies_offset)
                        if not replies_data or len(body_content) > INDEX_CONTENT_LIMIT:
                            break
                        for reply in replies_data:
                            body_content += reply.get('content') or ''
                        replies_offset += replies_page_size

                    doc = {
                        'connection_id': str(connection_id),
                        'title': topic['title'],
                        'content': body_content,
                        'modified_time': topic.get('modified_time'),
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
        if need_cleared_topic_index_id_set:
            self.delete_indexes_by_ids(index_name, need_cleared_topic_index_id_set)
        return index_count
