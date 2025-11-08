import json
import logging

from seaqa_indexer.utils import need_update_connection_status, time_to_utc_time
from seaqa_indexer.config import LANG

logger = logging.getLogger(__name__)

SEASEARCH_BULK_OPETATE_LIMIT = 100


class EmailIndex(object):
    mapping = {
        'properties': {
            'connection_id': {
                'type': 'keyword',
            },
            'email_from': {
                'type': 'keyword',
            },
            'email_to': {
                'type': 'keyword'
            },
            'message_id': {
                'type': 'keyword'
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
            'email_date': {
                'type': 'date',
            },
            'updated_at': {
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
            self.mapping['properties']['content']['analyzer'] = 'standard'
            self.mapping['properties']['title']['analyzer'] = 'standard'
            self.index_settings['analysis'].pop('char_filter')
            self.index_settings['analysis']['analyzer'].pop('gse_standard_analyzer')
        if not self.seasearch_api.check_index_mapping(index_name).get('is_exist'):
            data = {
                'shard_num': self.shard_num,
                'mappings': self.mapping,
                'settings': self.index_settings
            }
            self.seasearch_api.create_index(index_name, data)

    def check_index(self, index_name):
        return self.seasearch_api.check_index_mapping(index_name).get('is_exist')

    def _make_query_searches(self, keyword, search_filename_only):
        match_query_kwargs = {'minimum_should_match': '-25%'}
        title_boost_kwargs = {'boost': 0.7}
        content_boost_kwargs = {'boost': 0.3}

        def _make_match_query(field, key_word, **kw):
            q = {'query': key_word}
            q.update(kw)
            return {'match': {field: q}}

        searches = []
        searches.append(_make_match_query('title', keyword, **match_query_kwargs, **title_boost_kwargs))
        if not search_filename_only:
            searches.append(_make_match_query('content', keyword, **match_query_kwargs, **content_boost_kwargs))

        return searches

    def _ensure_filter_exists(self, query_map):
        if 'filter' not in query_map['bool']:
            query_map['bool']['filter'] = []
        return query_map

    def get_search_param(self, index_name, keyword, start, size, time_from, time_to, search_filename_only=False):
        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword, search_filename_only)
        query_map['bool']['should'] = searches

        if time_from or time_to:
            range_filter = {'range': {'updated_at': {}}}
            if time_from:
                time_from = time_to_utc_time(time_from).isoformat()
                range_filter['range']['updated_at']['gte'] = time_from
            if time_to:
                time_to = time_to_utc_time(time_to).isoformat()
                range_filter['range']['updated_at']['lte'] = time_to

            self._ensure_filter_exists(query_map)
            query_map['bool']['filter'].append(range_filter)

        data = {
            'query': query_map,
            'from': start,
            'size': size,
            '_source': ['connection_id', 'email_from', 'message_id', 'email_to', 'title', 'content', 'email_date'],
            'sort': ['_score'],
            'highlight': {
                'pre_tags': ['<mark>'],
                'post_tags': ['</mark>'],
                'fields': {'content': {}},
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
        email_docs = []

        hits = results.get('hits', []).get('hits', [])
        total = results.get('hits', {}).get('total', {}).get('value', 0)

        if not hits:
            return email_docs

        for hit in hits:
            source = hit.get('_source')
            score = hit.get('_score')
            _id = hit.get('_id')
            r = {
                'title': source['title'],
                'email_from': source['email_from'],
                'email_to': source['email_to'],
                'message_id': source['message_id'],
                'connection_id': source['connection_id'],
                'content': source['content'],
                'email_date': source['email_date'],
                'score': score,
                '_id': _id,
            }
            # if highlight_content := hit.get('highlight', {}).get('content', [None])[0]:
            #     r.update(content=highlight_content)
            email_docs.append(r)

        logger.debug('email index search keyword: %s, in project: %s , \nsearch result: %s', keyword, index_name, email_docs)

        return email_docs

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

    def update(self, index_name, connection_id, connection_status, seaqa_db, email_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        deleted_index_id_set = set()
        deleted_seadb_pk_set = set()

        bulk_params = []

        while True:
            emails = email_seadb_api.get_emails_by_page(connection_id, since, page_size, offset)
            offset += page_size
            if not emails:
                break

            for email in emails:
                deleted = email.get('deleted')
                pk = email.get('_pk')
                key = str(pk)
                if deleted:
                    deleted_index_id_set.add(key)
                    deleted_seadb_pk_set.add(pk)
                else:
                    doc = {
                        'connection_id': connection_id,
                        'title': email.get('subject'),
                        'email_from': email.get('email_from'),
                        'email_to': email.get('email_to'),
                        'message_id': email.get('message_id'),
                        'content': email.get('content'),
                        'email_date': email.get('email_date'),
                        'updated_at': email.get('updated_at'),
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
        if bulk_params:
            self.seasearch_api.bulk(index_name, bulk_params)

        if deleted_index_id_set:
            self.delete_indexes_by_ids(index_name, deleted_index_id_set)
            email_seadb_api.delete_emails_from_db(connection_id, list(deleted_seadb_pk_set))

        return index_count
