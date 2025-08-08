import json
import logging
from datetime import datetime
from pickletools import long1

from seaqa_indexer.utils import md5
from seaqa_indexer.config import LANG

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
            'body': {
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
            'created_at': {
                'type': 'date',
                'format': 'epoch_millis'
            },
            'updated_at': {
                'type': 'date',
                'format': 'epoch_millis'
            },
            'closed_at': {
                'type': 'date',
                'format': 'epoch_millis'
            },
            'comments_count': {
                'type': 'integer',
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
            self.mapping['properties']['body']['analyzer'] = 'standard'
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

        def _make_match_query(field, key_word, **kw):
            q = {'query': key_word}
            q.update(kw)
            return {'match': {field: q}}

        searches = []
        searches.append(_make_match_query('title', keyword, **match_query_kwargs))
        searches.append({
            'match': {
                'title.ngram': {
                    'query': keyword,
                    'minimum_should_match': '80%',
                }
            }
        })
        if not search_title_only:
            searches.append(_make_match_query('body', keyword, **match_query_kwargs))
            searches.append(_make_match_query('labels', keyword, **match_query_kwargs))

        return searches

    def get_search_param(self, project_uuid, keyword, start=0, size=10, search_title_only=False):
        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword, search_title_only)
        query_map['bool']['should'] = searches

        data = {
            'query': query_map,
            'from': start,
            'size': size,
            '_source': ['connection_id', 'repo_owner', 'repo_name', 'issue_number', 'title', 'body', 'state', 'labels', 'url'],
            'sort': [
                {'updated_at': {'order': 'desc'}},
                '_score'
            ],
            'highlight': {
                'pre_tags': ['<mark>'],
                'post_tags': ['</mark>'],
                'fields': {
                    'body': {},
                    'title': {}
                }
            }
        }

        repo_query_info = {
            'index': project_uuid,
            'query': data
        }

        return repo_query_info

    def search(self, project_uuid, keyword, start, size, search_title_only=False):
        bulk_search_params = []
        search_param = self.get_search_param(project_uuid, keyword, start, size, search_title_only)
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
            highlight = hit.get('highlight', {})
            issue = {
                'repo_owner': source.get('repo_owner'),
                'repo_name': source.get('repo_name'),
                'issue_number': source.get('issue_number'),
                'title': source.get('title'),
                'connection_id': source.get('connection_id'),
                'body': source.get('body'),
                'state': source.get('state'),
                'labels': source.get('labels', []),
                'score': hit.get('_score'),
                '_id': hit.get('_id'),
                'highlight': {
                    'title': highlight.get('title', [source.get('title')])[0],
                    'body': highlight.get('body', [source.get('body')[:500]])[0]
                }
            }
            issues.append(issue)

        logger.debug(f"GitHub issue search: {keyword} in {project_uuid}, found {len(issues)} results")
        return issues

    def add_or_update_issue(self, index_name, connection_id, repo_owner, repo_name, issue_data):
        _id = md5(f"{connection_id}_{repo_owner}_{repo_name}_{issue_data['id']}")

        def convert_time(t):
            if not t:
                return None
            try:
                return int(datetime.fromisoformat(t).timestamp() * 1000)
            except ValueError:
                try:
                    dt = datetime.strptime(t, "%Y-%m-%dT%H:%M:%S")
                    return int(dt.timestamp() * 1000)
                except ValueError:
                    try:
                        dt = datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ")
                        return int(dt.timestamp() * 1000)
                    except ValueError:
                        logger.warning(f"Failed to parse the time {t}")
                        return None

        doc = {
            'connection_id': connection_id,
            'repo_owner': repo_owner,
            'repo_name': repo_name,
            'issue_id': issue_data['id'],
            'issue_number': issue_data['number'],
            'title': issue_data['title'],
            'author': issue_data.get('user', {}).get('login', ''),
            'url': issue_data.get('url', ''),
            'body': issue_data['body'] or '',
            'state': issue_data['state'],
            'labels': [label['name'] for label in issue_data.get('labels', [])],
            'created_at': convert_time(issue_data['created_at']),
            'updated_at': convert_time(issue_data['updated_at']),
            'closed_at': convert_time(issue_data.get('closed_at')),
            'comments_count': issue_data['comments']
        }

        bulk_ops = [
            {'index': {'_index': index_name, '_id': _id}},
            doc
        ]

        self.seasearch_api.bulk(index_name, bulk_ops)
        logger.debug(f"Indexed issue: {repo_owner}/{repo_name}#{issue_data['number']}")

    def delete_issue(self, index_name, connection_id, repo_owner, repo_name, issue_id):
        _id = md5(f"{connection_id}_{repo_owner}_{repo_name}_{issue_id}")
        delete_op = [{'delete': {'_index': index_name, '_id': _id}}]
        self.seasearch_api.bulk(index_name, delete_op)
        logger.info(f"Deleted issue: {repo_owner}/{repo_name}#{issue_id}")

    def query_data_by_connection_id(self, index_name, connection_id, start, size):
        dsl = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"connection_id": connection_id}}
                    ]
                }
            },
            "from": start,
            "size": size,
            "_source": ["issue_id", "repo_owner", "repo_name", "title", "url"],
            "sort": ["@url"],
        }

        hits, total = self._normal_search(index_name, dsl)
        return hits, total

    def _normal_search(self, index_name, dsl):
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