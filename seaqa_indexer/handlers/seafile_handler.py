import json
import datetime
import logging
import os

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.seadb_models.models import SeafileTable
from seaqa_indexer.utils.constants import ConnectionType, REPO_NAME_CACHE_TIME_OUT
from seaqa_indexer.utils import get_file_suffix
from seaqa_indexer.index.extract import ExtractorFactory
from seaqa_indexer.utils.constants import REPO_FILE_SIZE_LIMIT, SYNC_FILE_SUFFIXES
from seaqa_indexer.index.utils import get_content_by_download_link
from seaqa_indexer.utils.seahub_api import FileNotFoundException
from seaqa_indexer.utils.seahub_api import SeahubAPI
from seaqa_indexer.seadb_models.seafile_seadb_api import SeafileSeaDBAPI
from seaqa_indexer.app.event_redis import redis_cache
from seaqa_indexer.utils.constants import IndexStatus
from seaqa_indexer.utils import need_update_connection_last_sync_count, time_to_utc_time
from seaqa_indexer.index.index_store.seafile_index import SeafileIndex
from seaqa_indexer.ai_processor.utils import generate_summary
from seaqa_indexer.index.utils import gen_index_name_by_type, gen_content_vector_index_name
from seaqa_indexer.index.index_store.content_vector_index import ContentVectorIndex

logger = logging.getLogger(__name__)

class SeafileHandler(ConnectionHandler):
    """Handler for SEAFILE connection type."""

    connection_type = ConnectionType.SEAFILE.value
    BATCH_SIZE = 50

    def gen_table_name(self, connection_id):
        return SeafileTable.gen_table_name(connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        seafile_index = SeafileIndex(seasearch_api)
        return seafile_index.get_search_param(index_name, keyword, start, size, time_from, time_to)

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        seafile_index = SeafileIndex(seasearch_api)
        seafile_index.delete_index_by_index_name(index_name)

        # content vector index
        content_index_name = gen_content_vector_index_name(connection_id, self.connection_type)
        content_vector_index = ContentVectorIndex(seasearch_api)
        content_vector_index.delete_index_by_index_name(content_index_name)

        # summary vector index
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

    def format_vector_search_result(self, record, connection, result):
        connection_config = json.loads(connection.config)
        repo_name = connection_config.get('repo_name')
        repo_id = connection_config.get('repo_id')
        server_url = connection_config.get('server_url')
        url = server_url.rstrip('/') + '/' + 'lib' + '/' + repo_id + '/file/' + record.get('path', '').strip(
            '/') + '/' + record.get('title', '')

        formatted = {
            'title': record.get('title', ''),
            'path': record.get('path', ''),
            'repo_id': repo_id,
            'repo_name': repo_name,
            'server_url': server_url,
            'url': url,
        }
        if result.get('source_type') == 'chunk':
            formatted['source_type'] = 'chunk'
            formatted['content'] = result.get('content', '')
        return formatted

    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = self.gen_table_name(connection_id)
        seafile_api = SeafileSeaDBAPI(project_uuid)

        ai_processing_count = 0
        while True:
            rows = seafile_api.get_need_ai_files_by_page(connection_id, page_size)
            if not rows:
                break
            for row in rows:
                pk = row.get('_pk')
                title = row.get('title')
                content = row.get('content')
                summary = ''
                vector = []
                if content:
                    full_content = f"Title: {title}\n\nContent: {content}\n"
                    summary, vector = generate_summary(full_content, ConnectionType.SEAFILE.value, project_uuid)
                updates.append({
                    "pk": pk,
                    "row": {
                        "ai_summary": summary,
                        "ai_summary_vector": vector,
                        "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                    }
                })
                ai_processing_count += 1
                if len(updates) >= self.BATCH_SIZE:
                    seadb_api.update_rows(project_uuid, table_name, updates)
                    updates = []
            if updates:
                seadb_api.update_rows(project_uuid, table_name, updates)
        return ai_processing_count

    def update_index(self, project_connection, seasearch_api, seaqa_db, since):
        connection_id = project_connection.id
        project_uuid = project_connection.project_uuid
        connection_type = project_connection.type
        connection_status = json.loads(project_connection.status)

        seafile_seadb_api = SeafileSeaDBAPI(project_uuid)

        seafile_index = SeafileIndex(seasearch_api)

        seafile_index_name = gen_index_name_by_type(connection_id, connection_type)
        seafile_index.create_index_if_missing(seafile_index_name)
        index_count = seafile_index.update(seafile_index_name, connection_id, connection_status, seaqa_db, seafile_seadb_api, since)

        # finish index
        connection_status['last_indexed_count'] = index_count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        new_indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_connection(connection_id, new_indexed_at, json.dumps(connection_status))

    def update_content_vector_index(self, project_uuid, connection_id, content_vector_status, seaqa_db, seadb_api,
                                    storage, seasearch_api, since):
        table_name = self.gen_table_name(connection_id)
        content_vector_index = ContentVectorIndex(seasearch_api)
        content_index_name = gen_content_vector_index_name(connection_id, self.connection_type)
        content_vector_index.create_index_if_missing(content_index_name)

        index_count = content_vector_index.update(
            table_name,
            content_index_name,
            connection_id,
            project_uuid,
            content_vector_status,
            seaqa_db,
            seadb_api,
            storage,
            since,
            self.connection_type
        )

        content_vector_status['last_content_vector_indexed_count'] = index_count
        content_vector_status['last_content_vector_index_status'] = IndexStatus.COMPLETED
        content_vector_indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_content_vector_status_and_time(
            connection_id,
            json.dumps(content_vector_status),
            content_vector_indexed_at
        )

        logger.info('Finish content vector indexing for connection %s, indexed %d chunks', connection_id, index_count)

    def sync(self, connection_id, project_uuid, config, old_last_sync_time,
             connection_status, seaqa_db, sync_logger):
        old_repo_name = config.get('repo_name')
        server_url = config.get('server_url')
        api_token = config.get('api_token')
        since = old_last_sync_time
        is_first_update = False

        if not since:
            since = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
            is_first_update = True
        since = time_to_utc_time(str(since)).isoformat()

        seahub_api = SeahubAPI(server_url, api_token)
        repo_info = seahub_api.get_repo_info()
        repo_id = repo_info.get('repo_id')
        new_repo_name = repo_info.get('repo_name')

        redis_cache.set(repo_id, json.dumps({server_url: new_repo_name}), REPO_NAME_CACHE_TIME_OUT)
        config['repo_id'] = repo_id
        config['repo_name'] = new_repo_name
        is_update_config = False
        if old_repo_name != new_repo_name:
            is_update_config = True

        seafile_seadb_api = SeafileSeaDBAPI(project_uuid)
        sync_count, total_count = self._sync_seafile(connection_id, repo_id, connection_status, since, seahub_api,
                                                     seaqa_db, seafile_seadb_api, is_first_update, sync_logger)

        # Return config update flag along with sync results
        return sync_count, total_count, is_update_config, config

    def clean_connection_data(self, storage, seadb_api, project_uuid,
                              connection_id, tables):
        """Clean seafile connection data."""
        if tables:
            table_name = self.gen_table_name(connection_id)
            for table in tables:
                if table['name'] == table_name:
                    seadb_api.delete_table(project_uuid, table['id'])

    def _sync_seafile(self, connection_id, repo_id, connection_status, since, seahub_api, seaqa_db, seafile_seadb_api, is_first_update, sync_logger):
        page = 1
        per_page = 1000
        sync_count = 0
        _status_last_update_time = datetime.datetime.now()
        _status_last_update_count = 0
        # sync updated files
        while True:
            resp = seahub_api.get_recently_changed_files(since, page, per_page)
            page += 1
            recently_changed_files = resp.get('files', [])
            if not recently_changed_files:
                break

            for file in recently_changed_files:
                parent_dir = file.get('_parent_dir')
                dirent_name = file.get('_name')
                modified_time = file.get('_file_mtime')
                size = file.get('_size')

                file_suffix = get_file_suffix(dirent_name)
                if file_suffix not in SYNC_FILE_SUFFIXES:
                    continue

                existed_file = seafile_seadb_api.get_file_by_path(connection_id, parent_dir, dirent_name)
                if existed_file:
                    if time_to_utc_time(modified_time) <= time_to_utc_time(str(existed_file.get('modified_time'))):
                        logger.debug(f'connection_id %s parent_dir %s filename: %s is up to date, skipping sync.', connection_id, parent_dir, dirent_name)
                        continue

                content = ''
                if size < REPO_FILE_SIZE_LIMIT:
                    file_path = os.path.join(parent_dir, dirent_name)
                    extractor = ExtractorFactory.get_extractor(dirent_name)
                    if extractor:
                        try:
                            download_link = seahub_api.get_download_link(os.path.join(parent_dir, dirent_name))
                        except FileNotFoundException:
                            logger.warning('connection: %s, repo_id: %s, parent_dir: %s, name: %s file not found',
                                        connection_id, repo_id, parent_dir, dirent_name)
                            sync_logger.log(f'File not found: {parent_dir}/{dirent_name}')
                            continue
                        content = get_content_by_download_link(download_link)
                        content = extractor.extract(connection_id, repo_id, content, file_path)
                file_info = {
                    'path': parent_dir,
                    'filename': dirent_name,
                    'modified_time': modified_time,
                    'size': size,
                    'content': content if content else '',
                }
                seafile_seadb_api.save_or_update_file_to_db(connection_id, file_info, existed_file)
                should, new_time, new_count = need_update_connection_last_sync_count(
                    sync_count, _status_last_update_time, _status_last_update_count
                )
                if should:
                    connection_status['last_sync_count'] = sync_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                    _status_last_update_time = new_time
                    _status_last_update_count = new_count
                sync_count += 1

        # sync deleted files
        page = 1
        per_page = 1000
        while True:
            if is_first_update:
                break
            resp = seahub_api.get_recently_deleted_files(since, page, per_page)
            page += 1
            recently_deleted_files = resp.get('files', [])
            if not recently_deleted_files:
                break

            for file in recently_deleted_files:
                file_suffix = get_file_suffix(file.get('obj_name'))
                if file_suffix not in SYNC_FILE_SUFFIXES:
                    continue
                path = '/' + file.get('path').strip('/')
                file_info = {
                    'path': path,
                    'filename': file.get('obj_name'),
                }
                seafile_seadb_api.mark_file_as_deleted(connection_id, file_info)

        total_count = seafile_seadb_api.get_file_total_count(connection_id)
        return sync_count, total_count
