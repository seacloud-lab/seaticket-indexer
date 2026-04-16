import json
import logging

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.constants import INDEX_CONTENT_LIMIT, SEASEARCH_BULK_OPERATE_LIMIT
from seaqa_indexer.index.index_store.base_connection_index import BaseConnectionIndex

logger = logging.getLogger(__name__)


class GitHubPullRequestsIndex(BaseConnectionIndex):
    def update(self, index_name, connection_id, repo_owner, repo_name, connection_status, seaqa_db, github_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        need_cleared_pr_index_set = set()
        bulk_params = []

        while True:
            prs = github_seadb_api.get_pull_requests_by_page(connection_id, since, page_size, offset)
            if not prs:
                break

            for pr in prs:
                pr_id = pr.get('pr_id')
                pk = pr.get('_pk')
                key = str(pk)
                if pr.get('deleted') or pr.get('outdated'):
                    need_cleared_pr_index_set.add(key)
                else:
                    content = pr.get('content') or ''
                    comments_offset = 0
                    comments_page_size = 1000
                    while True:
                        comments_data = github_seadb_api.get_pull_request_comments_by_page(
                            connection_id, pr_id, comments_page_size, comments_offset
                        )
                        if not comments_data or len(content) > INDEX_CONTENT_LIMIT:
                            break
                        for comment in comments_data:
                            content += comment.get('content') or ''
                        comments_offset += comments_page_size

                    doc = {
                        'connection_id': str(connection_id),
                        'title': pr.get('title'),
                        'content': content,
                        'modified_time': pr.get('modified_time')
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

        if need_cleared_pr_index_set:
            self.delete_indexes_by_ids(index_name, need_cleared_pr_index_set)

        return index_count
