import json
import logging

from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.config import LANG
from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.constants import INDEX_CONTENT_LIMIT

logger = logging.getLogger(__name__)

SEASEARCH_BULK_OPETATE_LIMIT = 100


class GitHubIssuesIndex(object):
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

    index_settings = {
        'analysis': {
            'analyzer': {
                'gse_standard_analyzer': {
                    'type': 'gse_standard',
                    "char_filter": [
                        "gse_filter"
                    ]
                },
            },
            "char_filter": {
                "gse_filter": {
                    "type": "mapping",
                    "mappings": [
                        "\n =>  "
                    ]
                }
            }
        }
    }

    def __init__(self, seasearch_api, shard_num=1):
        self.seasearch_api = seasearch_api
        self.shard_num = shard_num
        self.lang = LANG

    def create_index_if_missing(self, index_name):
        if self.lang != 'chinese':
            self.mapping['properties']['title']['analyzer'] = 'standard'
            self.mapping['properties']['content']['analyzer'] = 'standard'
            self.index_settings['analysis'].pop('char_filter', None)
            self.index_settings['analysis']['analyzer'].pop('gse_standard_analyzer', None)

        if not self.seasearch_api.check_index_mapping(index_name).get('is_exist'):
            data = {
                'shard_num': self.shard_num,
                'mappings': self.mapping,
                'settings': self.index_settings
            }
            self.seasearch_api.create_index(index_name, data)
            logger.info(f"Created GitHub issue index: {index_name}")

    def check_index(self, index_name):
        return self.seasearch_api.check_index_mapping(index_name).get('is_exist')

    def _make_query_searches(self, keyword, search_title_only):
        match_query_kwargs = {'minimum_should_match': '-25%'}
        title_boost_kwargs = {'boost': 0.7}
        content_boost_kwargs = {'boost': 0.3}

        def _make_match_query(field, key_word, **kw):
            q = {'query': key_word}
            q.update(kw)
            return {'match': {field: q}}

        searches = []
        searches.append(_make_match_query('title', keyword, **match_query_kwargs, **title_boost_kwargs))
        if not search_title_only:
            searches.append(_make_match_query('content', keyword, **match_query_kwargs, **content_boost_kwargs))

        return searches

    def _ensure_filter_exists(self, query_map):
        if 'filter' not in query_map['bool']:
            query_map['bool']['filter'] = []
        return query_map

    def get_search_param(self, index_name, keyword, start, size, time_from, time_to, search_title_only=False):
        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword, search_title_only)
        query_map['bool']['should'] = searches

        if time_from or time_to:
            range_filter = {'range': {'modified_time': {}}}
            if time_from:
                time_from = time_to_utc_time(time_from).isoformat()
                range_filter['range']['modified_time']['gte'] = time_from
            if time_to:
                time_to = time_to_utc_time(time_to).isoformat()
                range_filter['range']['modified_time']['lte'] = time_to

            self._ensure_filter_exists(query_map)
            query_map['bool']['filter'].append(range_filter)

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

    def search(self, index_name, keyword, start, size, time_from, time_to, search_filename_only=False):
        bulk_search_params = []
        search_param = self.get_search_param(index_name, keyword, start, size, time_from, time_to, search_filename_only)
        bulk_search_params.append(search_param)

        query_body = json.dumps({
            'index_queries': bulk_search_params
        })
        results = self.seasearch_api.unified_search(query_body)
        issues = []

        hits = results.get('hits', {}).get('hits', [])
        total = results.get('hits', {}).get('total', {}).get('value', 0)

        if not hits:
            return issues, total

        for hit in hits:
            source = hit.get('_source')
            issue = {
                'repo_owner': source.get('repo_owner'),
                'repo_name': source.get('repo_name'),
                'issue_number': source.get('issue_number'),
                'title': source.get('title'),
                'connection_id': source.get('connection_id'),
                'content': source.get('content'),
                'state': source.get('state'),
                'labels': source.get('labels', []),
                'score': hit.get('_score'),
                '_id': hit.get('_id'),
            }
            issues.append(issue)

        logger.debug(f"GitHub issue search: {keyword} in {index_name}, found {len(issues)} results")
        return issues

    def normal_search(self, index_name, dsl):
        doc_item = self.seasearch_api.normal_search(index_name, dsl)
        total = doc_item['hits']['total']['value']
        return doc_item['hits']['hits'], total

    def delete_indexes_by_ids(self, index_name, ids):
        delete_params = []
        for _id in ids:
            delete_params.append({'delete': {'_id': _id, '_index': index_name}})
            if len(delete_params) >= SEASEARCH_BULK_OPETATE_LIMIT:
                self.seasearch_api.bulk(index_name, delete_params)
                delete_params = []
        if delete_params:
            self.seasearch_api.bulk(index_name, delete_params)

    def delete_index_by_index_name(self, index_name):
        self.seasearch_api.delete_index_by_name(index_name)

    def update(self, index_name, connection_id, repo_owner, repo_name, connection_status, seaqa_db, github_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        need_cleared_issues_index_set = set()
        need_cleared_issues_pk_set = set()
        need_cleared_issues_id_set = set()
        bulk_params = []

        while True:
            issues = github_seadb_api.get_issues_by_page(connection_id, since, page_size, offset)
            if not issues:
                break

            for issue in issues:
                issue_id = issue.get('issue_id')
                github_seadb_api.delete_comments_from_db(connection_id, issue_id)
                pk = issue.get('_pk')
                key = str(pk)
                if issue.get('deleted'):
                    need_cleared_issues_index_set.add(key)
                    need_cleared_issues_pk_set.add(issue_id)
                    need_cleared_issues_id_set.add(pk)
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
                            content += comment.get('body') or ''
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
                    if len(bulk_params) >= SEASEARCH_BULK_OPETATE_LIMIT:
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
            github_seadb_api.delete_issues_from_db(connection_id, list(need_cleared_issues_pk_set), list(need_cleared_issues_id_set))

        return index_count
