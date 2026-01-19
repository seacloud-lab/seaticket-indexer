import json
import logging

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.constants import INDEX_CONTENT_LIMIT, SEASEARCH_BULK_OPERATE_LIMIT
from seaqa_indexer.seasearch.index_store.base_index import BaseIndex

logger = logging.getLogger(__name__)


class GitHubIssuesIndex(BaseIndex):
    mapping = {
        'properties': {
            'connection_id': {
                'type': 'keyword',
            },
            'repo_owner': {
                'type': 'keyword',
            },
            'repo_name': {
                'type': 'keyword',
            },
            'issue_id': {
                'type': 'long',
            },
            'issue_number': {
                'type': 'integer',
            },
            'title': {
                'type': 'text',
                'analyzer': 'gse_standard_analyzer',
                'fields': {
                    'keyword': {
                        'type': 'keyword'
                    }
                }
            },
            'content': {
                'type': 'text',
                'analyzer': 'gse_standard_analyzer',
                'highlightable': True
            },
            'state': {
                'type': 'keyword',
            },
            'labels': {
                'type': 'keyword',
            },
            'modified_time': {
                'type': 'date',
            }
        }
    }

    def get_search_param(self, index_name, keyword, start, size, time_from, time_to, search_title_only=False):
        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword, search_title_only)
        query_map['bool']['should'] = searches

        query_map = self._add_time_range_filter(query_map, time_from, time_to)

        data = {
            'query': query_map,
            'from': start,
            'size': size,
            '_source': ['connection_id', 'repo_owner', 'repo_name', 'issue_number', 'title', 'content',
                        'state', 'labels'],
            'sort': [
                '_score'
            ],
            'highlight': {
                'pre_tags': ['<mark>'],
                'post_tags': ['</mark>'],
                'fields': {
                    'content': {}
                }
            }
        }

        repo_query_info = {
            'index': index_name,
            'query': data
        }

        return repo_query_info

    def update(self, index_name, connection_id, repo_owner, repo_name, connection_status, seaqa_db, github_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        need_cleared_issues_index_set = set()
        bulk_params = []

        while True:
            issues = github_seadb_api.get_issues_by_page(connection_id, since, page_size, offset)
            if not issues:
                break

            for issue in issues:
                issue_id = issue.get('issue_id')
                pk = issue.get('_pk')
                key = str(pk)
                if issue.get('deleted') or issue.get('outdated'):
                    need_cleared_issues_index_set.add(key)
                else:
                    content = issue.get('content') or ''
                    comments_offset = 0
                    comments_page_size = 1000
                    while True:
                        comments_data = github_seadb_api.get_issue_comments_by_page(connection_id, issue_id,
                                                                                    comments_page_size, comments_offset)
                        if not comments_data or len(content) > INDEX_CONTENT_LIMIT:
                            break
                        for comment in comments_data:
                            content += comment.get('content') or ''
                        comments_offset += comments_page_size

                    doc = {
                        'connection_id': connection_id,
                        'repo_owner': repo_owner,
                        'repo_name': repo_name,
                        'issue_id': issue_id,
                        'issue_number': issue.get('issue_number'),
                        'title': issue.get('title'),
                        'content': content,
                        'state': issue.get('state'),
                        'labels': issue.get('labels'),
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
