import logging
import json

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
            'bumped_at': {
                'type': 'date'
            },
            'created_at': {
                'type': 'date'
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

    def __init__(self, seasearch_api, discourse_seadb_api, shard_num=1):
        self.seasearch_api = seasearch_api
        self.discourse_seadb_api = discourse_seadb_api
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

    def get_search_param(self, index_name, keyword, start=0, size=10, search_title_only=False):
        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword, search_title_only)
        query_map['bool']['should'] = searches

        data = {
            'query': query_map,
            'from': start,
            'size': size,
            '_source': ['connection_id', 'topic_id', 'title', 'body', 'slug', 'bumped_at', 'created_at'],
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
                'bumped_at': source['bumped_at'],
                'created_at': source['created_at'],
            }
            discourse_posts.append(post)

        logger.debug(f"Discourse search: {keyword} in {index_name}, found {len(discourse_posts)} results")
        return discourse_posts

    def add_or_update_topic_with_replies(self, index_name, connection_id, topic_data, replies_data):
        _id = self.gen_index_id(topic_data['topic_id'])

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
            'bumped_at': topic_data.get('bumped_at'),
            'created_at': topic_data.get('created_at'),
        }

        bulk_ops = [
            {'index': {'_index': index_name, '_id': _id}},
            doc
        ]

        self.seasearch_api.bulk(index_name, bulk_ops)

    def gen_index_id(self, topic_id):
        return md5(f"{topic_id}")

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

    def get_all_need_index_topics_from_db(self, connection_id, last_indexed_at):
        """From seadb obtain the topics that need to be indexed"""
        try:
            topics = self.discourse_seadb_api.get_topics_by_connection_id(
                connection_id=connection_id,
                update_at_gt=last_indexed_at,
            )

            result = []
            for topic in topics:
                class TopicRow:
                    def __init__(self, data):
                        self.topic_id = data['topic_id']
                        self.title = data['title']
                        self.slug = data['slug']
                        self.category_id = data['category_id']
                        self.deleted = data['deleted']
                        self.bumped_at = data['bumped_at']
                        self.updated_at = data['updated_at']
                        self.created_at = data['created_at']

                result.append(TopicRow(topic))

            return result
        except Exception as e:
            logger.error(f"Error getting topics from seadb: {e}")
            return []

    def delete_topic_from_db(self, topic_id, connection_id):
        """Delete topic from seadb physically"""
        try:
            self.discourse_seadb_api.delete_topic(connection_id, topic_id)
            logger.debug(f"Deleted topic {topic_id} from seadb")
        except Exception as e:
            logger.error(f"Error deleting topic {topic_id} from seadb: {e}")

    def get_replies_for_topic(self, connection_id, topic_id):
        """Get the reply to the topic from seadb"""
        try:
            replies = self.discourse_seadb_api.get_replies_by_topic(
                connection_id=connection_id,
                topic_id=topic_id
            )

            result = []
            for reply in replies:
                class ReplyRow:
                    def __init__(self, data):
                        self.post_number = data['post_number']
                        self.content = data['content']

                result.append(ReplyRow(reply))

            return result
        except Exception as e:
            logger.error(f"Error getting replies from seadb: {e}")
            return []

    def update(self, index_name, connection_id, connection_status, seaqa_db, last_indexed_at):
        topics = self.get_all_need_index_topics_from_db(connection_id, last_indexed_at)
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
                'bumped_at': topic.bumped_at,
                'updated_at': topic.updated_at,
                'created_at': topic.created_at,
            }

            if topic.deleted == True:
                need_cleared_topic_id_set.add(topic.topic_id)
                key = self.gen_index_id(topic.topic_id)
                need_cleared_topic_index_id_set.add(key)
            else:
                self.add_or_update_topic_with_replies(index_name, connection_id, topic_data, replies)
                index_count += 1

            if need_update_connection_status(index_count):
                connection_status['last_indexed_count'] = index_count
                seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

        if need_cleared_topic_index_id_set:
            self.delete_indexes_by_ids(index_name, need_cleared_topic_index_id_set)
            for topic_id in need_cleared_topic_id_set:
                self.delete_topic_from_db(topic_id, connection_id)

        return index_count
