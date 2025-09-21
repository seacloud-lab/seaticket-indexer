import json
import logging
import os
import datetime

from seaqa_indexer.utils import md5, need_update_connection_status
from seaqa_indexer.config import LANG
from seaqa_indexer.utils.constants import REPO_FILE_SIZE_LIMIT
from seaqa_indexer.seasearch.utils import get_content_by_download_link
from seaqa_indexer.utils.seahub_api import FileNotFoundException
from seaqa_indexer.seasearch.extract import ExtractorFactory

logger = logging.getLogger(__name__)

SEASEARCH_BULK_OPETATE_LIMIT = 100


class SeafileIndex(object):
    mapping = {
        'properties': {
            'connection_id': {
                'type': 'keyword',
            },
            'repo_id': {
                'type': 'keyword',
            },
            'path': {
                'type': 'keyword'
            },
            'filename': {
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
            'mtime': {
                'type': 'date',
                'format': 'epoch_millis'
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

        def _make_match_query(field, key_word, **kw):
            q = {'query': key_word}
            q.update(kw)
            return {'match': {field: q}}

        searches = []
        searches.append(_make_match_query('filename', keyword, **match_query_kwargs))
        if not search_filename_only:
            searches.append(_make_match_query('content', keyword, **match_query_kwargs))

        return searches

    def _ensure_filter_exists(self, query_map):
        if 'filter' not in query_map['bool']:
            query_map['bool']['filter'] = []
        return query_map

    def get_search_param(self, index_name, keyword, start, size, search_filename_only=False):
        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword, search_filename_only)
        query_map['bool']['should'] = searches

        data = {
            'query': query_map,
            'from': start,
            'size': size,
            '_source': ['connection_id', 'repo_id', 'path', 'filename', 'content'],
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

    def search(self, index_name, keyword, start, size, search_filename_only=False):
        bulk_search_params = []
        search_param = self.get_search_param(index_name, keyword, start, size, search_filename_only)
        bulk_search_params.append(search_param)
        query_body = json.dumps({
            'index_queries': bulk_search_params
        })
        results = self.seasearch_api.unified_search(query_body)
        seafile_docs = []

        hits = results.get('hits', []).get('hits', [])
        total = results.get('hits', {}).get('total', {}).get('value', 0)

        if not hits:
            return seafile_docs

        for hit in hits:
            source = hit.get('_source')
            score = hit.get('_score')
            _id = hit.get('_id')
            r = {
                'filename': source['filename'],
                'path': source['path'],
                'repo_id': source['repo_id'],
                'connection_id': source['connection_id'],
                'content': source['content'],
                'score': score,
                '_id': _id,
            }
            # if highlight_content := hit.get('highlight', {}).get('content', [None])[0]:
            #     r.update(content=highlight_content)
            seafile_docs.append(r)

        logger.debug('seafile index search keyword: %s, in project: %s , \nsearch result: %s', keyword, index_name, seafile_docs)

        return seafile_docs

    def update_document(self, index_name, connection_id, content, repo_id, parent_dir, filename, mtime):
        self.add_document(index_name, connection_id, content, repo_id, parent_dir, filename, mtime)

    def gen_index_id(self, repo_id, parent_dir, filename):
        return md5(repo_id + parent_dir + filename)

    def add_document(self, index_name, connection_id, content, repo_id, parent_dir, filename, mtime):
        bulk_add_params = []
        _id = self.gen_index_id(repo_id, parent_dir, filename)
        index_info = {'index': {'_index': index_name, '_id': _id}}
        doc_info = {
            'connection_id': connection_id,
            'repo_id': repo_id,
            'content': content,
            'path': parent_dir,
            'filename': filename,
            'mtime': mtime * 1000
        }

        bulk_add_params.append(index_info)
        bulk_add_params.append(doc_info)
        self.seasearch_api.bulk(index_name, bulk_add_params)

    def query_all_data(self, index_name, start, size):
        dsl = {
            "query": {
                "match_all": {}
            },
            "from": start,
            "size": size,
            "_source": ["repo_id", "path", "filename", "mtime"],
            "sort": ["@mtime"],
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

    def get_path_to_index_info(self, index_name):
        per_size = 1000
        start = 0
        path_to_index_info = {}
        while True:
            hits, total = self.query_all_data(index_name, start, per_size)
            for hit in hits:
                source = hit['_source']
                # _id = hit['_id']
                repo_id = source['repo_id']
                path = source['path']  # parent_dir
                filename = source['filename']
                index_id = self.gen_index_id(repo_id, path, filename)
                path_to_index_info[index_id] = source

            start += per_size
            if len(hits) < per_size:
                break
        return path_to_index_info

    def update(self, seafile_index_name, connection_id, repo_id, seahub_api, connection_status, seaqa_db):
        index_id_to_index_info = self.get_path_to_index_info(seafile_index_name)
        old_index_ids_set = set(index_id_to_index_info.keys())
        new_index_ids_set = set()

        item_info = seahub_api.list_items_in_dir('/', 'f')
        index_count = 0
        dirent_list = item_info.get('dirent_list')
        for dirent in dirent_list:
            parent_dir = dirent.get('parent_dir')
            dirent_name = dirent.get('name')
            mtime = dirent.get('mtime')
            size = dirent.get('size')

            key = self.gen_index_id(repo_id, parent_dir, dirent_name)
            new_index_ids_set.add(key)
            index_info = index_id_to_index_info.get(key, {})

            logger.debug('start to connection: %s repo_id: %s, parent_dir: %s, name: %s', connection_id, repo_id,
                         parent_dir, dirent_name)

            old_mtime = index_info.get('mtime')
            timestamp_mtime = datetime.datetime.fromisoformat(mtime).timestamp()
            if old_mtime:
                old_mtime = old_mtime / 1000

            if timestamp_mtime == old_mtime:
                logger.debug('connection: %s, repo_id: %s, parent_dir: %s, name: %s is not change.',
                             connection_id, repo_id, parent_dir, dirent_name)
                continue

            content = None
            if size > REPO_FILE_SIZE_LIMIT:
                self.update_document(seafile_index_name, connection_id, content, repo_id, parent_dir,
                                                   dirent_name, timestamp_mtime)
                continue

            file_path = os.path.join(parent_dir, dirent_name)
            extractor = ExtractorFactory.get_extractor(dirent_name)
            if extractor:
                try:
                    download_link = seahub_api.get_download_link(os.path.join(parent_dir, dirent_name))
                except FileNotFoundException:
                    logger.warning('connection: %s, repo_id: %s, parent_dir: %s, name: %s file not found',
                                   connection_id, repo_id, parent_dir, dirent_name)
                    continue
                content = get_content_by_download_link(download_link)
                content = extractor.extract(connection_id, repo_id, content, file_path)
            self.update_document(seafile_index_name, connection_id, content, repo_id, parent_dir,
                                               dirent_name, timestamp_mtime)
            index_count += 1
            if need_update_connection_status(index_count):
                connection_status['last_indexed_count'] = index_count
                seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
        # clear deleted site crawled url
        need_cleared_seafile_index_set = old_index_ids_set - new_index_ids_set
        self.delete_indexes_by_ids(seafile_index_name, need_cleared_seafile_index_set)
        return index_count, len(dirent_list)

