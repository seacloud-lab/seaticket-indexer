import json
import logging

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.crawler.utils import url_to_filename
from seaqa_indexer.seadb_models.utils import get_table_name
from seaqa_indexer.index.index_store.base_connection_index import BaseConnectionIndex
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT

logger = logging.getLogger(__name__)


class SiteIndex(BaseConnectionIndex):
    def update(self, index_name, connection_id, project_uuid, connection_status, seaqa_db, seadb_api, storage, since):
        new_index_ids_set = set()
        deleted_index_id_set = set()

        index_count = 0
        page_size = 1000
        offset = 0
        bulk_params = []

        def _flush_bulk_if_needed(force=False):
            nonlocal bulk_params
            if bulk_params and (force or len(bulk_params) >= SEASEARCH_BULK_OPERATE_LIMIT):
                self.seasearch_api.bulk(index_name, bulk_params)
                bulk_params = []

        table_name = get_table_name('WebCrawlTable', connection_id=connection_id)
        while True:
            sql = f"""SELECT `_pk`, `url`, `title`, `modified_time`, `deleted`, `outdated` FROM `{table_name}` 
                    WHERE record_modified_time >= '{since}' 
                    LIMIT {page_size} OFFSET {offset}"""
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            for row in rows:
                url = row.get('url')
                modified_time = row.get('modified_time')
                title = row.get('title')
                deleted = row.get('deleted')
                pk = row.get('_pk')
                outdated = row.get('outdated')
                key = str(pk)
                if deleted or outdated:
                    deleted_index_id_set.add(key)
                    continue
                else:
                    new_index_ids_set.add(key)

                logger.debug('start to update url: %s', url)

                filename = url_to_filename(url)
                file_json = storage.read_json(connection_id, project_uuid, filename)
                content = file_json.get('content')

                index_info = {'index': {'_index': index_name, '_id': key}}
                doc_info = {
                    'connection_id': str(connection_id),
                    'content': content,
                    'title': title,
                    'modified_time': modified_time,
                }
                bulk_params.append(index_info)
                bulk_params.append(doc_info)

                index_count += 1
                if need_update_connection_status(index_count):
                    connection_status['last_indexed_count'] = index_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

                _flush_bulk_if_needed()

            offset += page_size

        _flush_bulk_if_needed(force=True)

        if deleted_index_id_set:
            self.delete_indexes_by_ids(index_name, deleted_index_id_set)

        return index_count
