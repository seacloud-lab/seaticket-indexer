import json
import logging
from datetime import datetime

from seaqa_indexer.utils import md5
from seaqa_indexer.config import LANG
from sqlalchemy.sql import text
from seaqa_indexer.utils import need_update_connection_status

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

    def __init__(self, seasearch_api, db_session_class, shard_num=1):
        self.seasearch_api = seasearch_api
        self.db_session_class = db_session_class
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
        if not search_title_only:
            searches.append(_make_match_query('body', keyword, **match_query_kwargs))
            searches.append(_make_match_query('labels', keyword, **match_query_kwargs))

        return searches

    def _ensure_filter_exists(self, query_map):
        if 'filter' not in query_map['bool']:
            query_map['bool']['filter'] = []
        return query_map

    def get_search_param(self, index_name, keyword, start, size, search_title_only=False):
        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword, search_title_only)
        query_map['bool']['should'] = searches

        data = {
            'query': query_map,
            'from': start,
            'size': size,
            '_source': ['connection_id', 'repo_owner', 'repo_name', 'issue_number', 'title', 'body', 'state', 'labels', 'url'],
            'sort': [
                '_score'
            ],
            'highlight': {
                'pre_tags': ['<mark>'],
                'post_tags': ['</mark>'],
                'fields': {
                    'body': {}
                }
            }
        }

        repo_query_info = {
            'index': index_name,
            'query': data
        }

        return repo_query_info

    def search(self, index_name, keyword, start, size, search_filename_only=False):
        bulk_search_params = []
        search_param = self.get_search_param(index_name, keyword, start, size, search_filename_only)
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
                'body': source.get('body'),
                'state': source.get('state'),
                'labels': source.get('labels', []),
                'score': hit.get('_score'),
                '_id': hit.get('_id'),
            }
            issues.append(issue)

        logger.debug(f"GitHub issue search: {keyword} in {index_name}, found {len(issues)} results")
        return issues

    def add_or_update_issue(self, index_name, connection_id, issue_data):
        _id = self.gen_index_id(issue_data['id'])

        doc = {
            'connection_id': connection_id,
            'repo_owner': issue_data['repo_owner'],
            'repo_name': issue_data['repo_name'],
            'issue_id': issue_data['id'],
            'issue_number': issue_data['number'],
            'title': issue_data['title'],
            'author': issue_data.get('user', {}).get('login', ''),
            'url': issue_data.get('url', ''),
            'body': issue_data['body'] or '',
            'state': issue_data['state'],
            'labels': [label['name'] for label in issue_data.get('labels', [])],
            'comments_count': issue_data['comments']
        }

        bulk_ops = [
            {'index': {'_index': index_name, '_id': _id}},
            doc
        ]

        self.seasearch_api.bulk(index_name, bulk_ops)

    def gen_index_id(self, issue_id):
        return md5(f"{issue_id}")

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

    def get_all_need_index_issues_from_db(self, issues_sync_id):
        with self.db_session_class() as session:
            sql = """
                   SELECT id, issue_number, title, body, state, created_at,
                          updated_at, closed_at, labels, comments, author, url, deleted
                   FROM github_issues
                   WHERE connection_id = :connection_id AND need_index = 1
               """
            rows = session.execute(text(sql), {'connection_id': issues_sync_id}).fetchall()
        return rows

    def delete_issue_from_db(self, issue_id):
        with self.db_session_class() as session:
            sql = text("DELETE FROM github_issues WHERE id = :id")
            session.execute(sql, {'id': issue_id})
            session.commit()

    def update_issues_index_status(self, issue_id):
        with self.db_session_class() as session:
            sql = text("UPDATE github_issues SET need_index = 0 WHERE id = :id")
            session.execute(sql, {'id': issue_id})
            session.commit()

    def update(self, index_name, connection_id, repo_owner, repo_name, connection_status, seaqa_db):
        issues = self.get_all_need_index_issues_from_db(connection_id)
        index_count = 0
        need_cleared_issues_index_set = set()

        for issue in issues:
            issue_id = issue.id
            issue_dict = {
                'id': issue_id,
                'number': issue.issue_number,
                'title': issue.title,
                'body': issue.body,
                'state': issue.state,
                'labels': [{'name': l} for l in json.loads(issue.labels or "[]")],
                'comments': issue.comments,
                'user': {'login': issue.author},
                'url': issue.url,
                'repo_owner': repo_owner,
                'repo_name': repo_name,
            }
            if issue.deleted == 1:
                key = self.gen_index_id(issue_id)
                need_cleared_issues_index_set.add(key)
            else:
                self.add_or_update_issue(index_name, connection_id, issue_dict)
                self.update_issues_index_status(issue_id)
                index_count += 1

            if need_update_connection_status(index_count):
                connection_status['last_indexed_count'] = index_count
                seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

        if need_cleared_issues_index_set:
            self.delete_indexes_by_ids(index_name, need_cleared_issues_index_set)
            for issue_id in need_cleared_issues_index_set:
                self.delete_issue_from_db(issue_id)

        return index_count
