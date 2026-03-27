import json
import logging

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.constants import INDEX_CONTENT_LIMIT, SEASEARCH_BULK_OPERATE_LIMIT
from seaqa_indexer.index.utils import EmailMessage
from seaqa_indexer.index.index_store.base_connection_index import BaseConnectionIndex

logger = logging.getLogger(__name__)


class EmailIndex(BaseConnectionIndex):
    def update(self, index_name, connection_id, connection_status, seaqa_db, email_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        deleted_index_id_set = set()

        bulk_params = []

        while True:
            threads = email_seadb_api.get_threads_by_page(connection_id, since, page_size, offset)
            offset += page_size
            if not threads:
                break

            for thread in threads:
                deleted = thread.get('deleted')
                pk = thread.get('_pk')
                outdated = thread.get('outdated')
                key = str(pk)
                if deleted or outdated:
                    deleted_index_id_set.add(key)
                else:
                    content = ''
                    email_offset = 0
                    email_page_size = 100
                    while True:
                        thread_emails = email_seadb_api.get_thread_emails_by_page(connection_id, pk, email_page_size, email_offset)
                        if not thread_emails:
                            break
                        for email in thread_emails:
                            email_content = email.get('content')
                            content += EmailMessage().parse_reply(email.get('content')) if email_content else ''

                        if len(content) > INDEX_CONTENT_LIMIT:
                            break
                        email_offset += email_page_size
                    doc = {
                        'connection_id': str(connection_id),
                        'title': thread.get('title'),
                        'content': content,
                        'modified_time': thread.get('modified_time'),
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

        if deleted_index_id_set:
            self.delete_indexes_by_ids(index_name, deleted_index_id_set)

        return index_count
