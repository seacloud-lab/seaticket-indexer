import json

from seaqa_indexer.index.index_store.base_connection_index import BaseConnectionIndex
from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT


class FirebaseCrashIndex(BaseConnectionIndex):
    def update(self, index_name, connection_id, connection_status, seaqa_db,
               firebase_crash_seadb_api, since):
        index_count = 0
        offset = 0
        page_size = 1000
        bulk_params = []
        removed_ids = set()

        while True:
            issues = firebase_crash_seadb_api.get_issues_by_page(
                connection_id, since, page_size, offset
            )
            if not issues:
                break
            for issue in issues:
                key = str(issue['_pk'])
                if issue.get('deleted') or issue.get('outdated'):
                    removed_ids.add(key)
                    continue
                bulk_params.append({'index': {'_index': index_name, '_id': key}})
                bulk_params.append({
                    'connection_id': str(connection_id),
                    'issue_id': issue.get('issue_id') or '',
                    'source_table': issue.get('source_table') or '',
                    'app_id': issue.get('app_id') or '',
                    'title': issue.get('title') or '',
                    'subtitle': issue.get('subtitle') or '',
                    'error_type': issue.get('error_type') or '',
                    'platform': issue.get('platform') or '',
                    'content': issue.get('content') or '',
                    'modified_time': issue.get('modified_time'),
                })
                index_count += 1
                if len(bulk_params) >= SEASEARCH_BULK_OPERATE_LIMIT:
                    self.seasearch_api.bulk(index_name, bulk_params)
                    bulk_params = []
                if need_update_connection_status(index_count):
                    connection_status['last_indexed_count'] = index_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
            offset += page_size

        if bulk_params:
            self.seasearch_api.bulk(index_name, bulk_params)
        if removed_ids:
            self.delete_indexes_by_ids(index_name, removed_ids)
        return index_count
