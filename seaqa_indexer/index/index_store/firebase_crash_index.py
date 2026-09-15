import json

from seaqa_indexer.index.index_store.base_connection_index import BaseConnectionIndex
from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT


class FirebaseCrashIndex(BaseConnectionIndex):
    def update(self, index_name, connection_id, connection_status, seaqa_db,
               firebase_crash_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        need_cleared_issues_index_set = set()
        bulk_params = []

        while True:
            issues = firebase_crash_seadb_api.get_issues_by_page(
                connection_id, since, page_size, offset
            )
            if not issues:
                break

            for issue in issues:
                pk = issue.get('_pk')
                key = str(pk)
                if issue.get('deleted') or issue.get('outdated'):
                    need_cleared_issues_index_set.add(key)
                else:
                    doc = {
                        'connection_id': str(connection_id),
                        'title': issue.get('title'),
                        'content': issue.get('content') or '',
                        'modified_time': issue.get('modified_time'),
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

        if need_cleared_issues_index_set:
            self.delete_indexes_by_ids(index_name, need_cleared_issues_index_set)

        return index_count
