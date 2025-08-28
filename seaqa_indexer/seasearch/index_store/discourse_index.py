import logging
import json
from sqlalchemy.sql import text

from seaqa_indexer.utils import md5
from seaqa_indexer.config import LANG
from seaqa_indexer.utils import need_update_connection_status

logger = logging.getLogger(__name__)

SEASEARCH_BULK_OPETATE_LIMIT = 100


class DiscourseIndex(object):
    mapping = {
        'properties': {
            'connection_id': {
                'type': 'keyword'
            },
            'topic_id': {
                'type': 'long'
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
            'category_id': {
                'type': 'long'
            },
            'slug': {
                'type': 'keyword'
            },
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

        return searches

    def _ensure_filter_exists(self, query_map):
        if 'filter' not in query_map['bool']:
            query_map['bool']['filter'] = []
        return query_map

    def _add_connection_id_filter(self, query_map, connection_ids):
        if connection_ids is None:
            return query_map

        query_map = self._ensure_filter_exists(query_map)

        if isinstance(connection_ids, list):
            query_map['bool']['filter'].append({'terms': {'connection_id': connection_ids}})
        return query_map

    def get_search_param(self, project_uuid, keyword, start=0, size=10, connection_ids=None, search_title_only=False):
        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword, search_title_only)
        query_map['bool']['should'] = searches
        query_map = self._add_connection_id_filter(query_map, connection_ids)

        data = {
            'query': query_map,
            'from': start,
            'size': size,
            '_source': ['connection_id', 'topic_id', 'title', 'body', 'slug'],
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
            'index': project_uuid,
            'query': data
        }

        return repo_query_info

    def search(self, project_uuid, keyword, start, size, connection_ids=None, search_filename_only=False):
        bulk_search_params = []
        search_param = self.get_search_param(project_uuid, keyword, start, size, connection_ids, search_filename_only)
        bulk_search_params.append(search_param)

        query_body = json.dumps({
            'index_queries': bulk_search_params
        })
        results = self.seasearch_api.unified_search(query_body)

        hits = results.get('hits', {}).get('hits', [])
        total = results.get('hits', {}).get('total', {}).get('value', 0)
        if not hits:
            return hits, total
        discourse_posts = []
        for hit in hits:
            source = hit['_source']

            post = {
                'connection_id': source['connection_id'],
                'topic_id': source['topic_id'],
                'title': source['title'],
                'body': source['body'],
                'category_id': source.get('category_id'),
                'slug': source['slug'],
            }
            discourse_posts.append(post)

        logger.debug(f"Discourse search: {keyword} in {project_uuid}, found {len(discourse_posts)} results")
        return discourse_posts

    def add_or_update_topic_with_replies(self, index_name, connection_id, topic_data, replies_data):
        _id = self.gen_index_id(connection_id, topic_data['topic_id'])

        body_content = ''
        for reply in replies_data:
            if reply.content:
                if body_content:
                    body_content += '\n\n' + reply.content
                else:
                    body_content = reply.content

        doc = {
            'connection_id': connection_id,
            'topic_id': topic_data['topic_id'],
            'title': topic_data['title'],
            'body': body_content,
            'category_id': topic_data.get('category_id'),
            'slug': topic_data.get('slug', ''),
        }

        bulk_ops = [
            {'index': {'_index': index_name, '_id': _id}},
            doc
        ]

        self.seasearch_api.bulk(index_name, bulk_ops)

    def gen_index_id(self, connection_id, topic_id):
        return md5(f"{connection_id}{topic_id}")

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
            "_source": False,
            "sort": ["@topic_id"],
        }

        hits, total = self.normal_search(index_name, dsl)
        return hits, total

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

    def get_all_need_index_topics_from_db(self, connection_id):
        with self.db_session_class() as session:
            sql = """
                   SELECT topic_id, title, slug, category_id, deleted
                   FROM discourse_topics
                   WHERE connection_id = :connection_id AND need_index = 1
               """
            rows = session.execute(text(sql), {'connection_id': connection_id}).fetchall()
        return rows

    def delete_topic_from_db(self, topic_id, connection_id):
        """Physically delete topic from database"""
        with self.db_session_class() as session:
            sql = text("DELETE FROM discourse_topics WHERE topic_id = :topic_id AND connection_id = :connection_id")
            session.execute(sql, {'topic_id': topic_id, 'connection_id': connection_id})
            session.commit()

    def update_topic_index_status(self, topic_id, connection_id):
        with self.db_session_class() as session:
            sql = text("UPDATE discourse_topics SET need_index = 0 WHERE topic_id = :topic_id AND connection_id = :connection_id")
            session.execute(sql, {'topic_id': topic_id, 'connection_id': connection_id})
            session.commit()

    def get_replies_for_topic(self, connection_id, topic_id):
        with self.db_session_class() as session:
            sql = """
                   SELECT post_number, content
                   FROM discourse_replies
                   WHERE connection_id = :connection_id AND topic_id = :topic_id
               """
            rows = session.execute(text(sql), {'connection_id': connection_id, 'topic_id': topic_id}).fetchall()
        return rows

    def update(self, index_name, connection_id, connection_status, seaqa_db):
        topics = self.get_all_need_index_topics_from_db(connection_id)
        index_count = 0
        need_cleared_topic_id_set = set()
        need_cleared_topic_index_id_set = set()

        for topic in topics:
            replies = self.get_replies_for_topic(connection_id, topic.topic_id)

            topic_data = {
                'topic_id': topic.topic_id,
                'title': topic.title,
                'slug': topic.slug,
                'category_id': topic.category_id,
            }

            if topic.deleted == 1:
                need_cleared_topic_id_set.add(topic.topic_id)
                key = self.gen_index_id(connection_id, topic.topic_id)
                need_cleared_topic_index_id_set.add(key)
            else:
                self.add_or_update_topic_with_replies(index_name, connection_id, topic_data, replies)
                self.update_topic_index_status(topic.topic_id, connection_id)
                index_count += 1

            if need_update_connection_status(index_count):
                connection_status['last_indexed_count'] = index_count
                seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

        if need_cleared_topic_index_id_set:
            self.delete_indexes_by_ids(index_name, need_cleared_topic_index_id_set)
            for topic_id in need_cleared_topic_id_set:
                self.delete_topic_from_db(topic_id, connection_id)

        return index_count
