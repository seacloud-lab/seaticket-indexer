import json
import datetime
import logging
import os

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.seadb_models.models import SeafileTable
from seaqa_indexer.utils.constants import ConnectionType, REPO_NAME_CACHE_TIME_OUT
from seaqa_indexer.handlers.utils import need_update_connection_last_sync_count, time_to_utc_time, get_file_suffix
from seaqa_indexer.index.extract import ExtractorFactory
from seaqa_indexer.utils.constants import REPO_FILE_SIZE_LIMIT, SYNC_FILE_SUFFIXES
from seaqa_indexer.index.utils import get_content_by_download_link
from seaqa_indexer.utils.seahub_api import FileNotFoundException
from seaqa_indexer.utils.seahub_api import SeahubAPI
from seaqa_indexer.seadb_models.seafile_seadb_api import SeafileSeaDBAPI
from seaqa_indexer.app.event_redis import redis_cache

logger = logging.getLogger(__name__)

class SeafileHandler(ConnectionHandler):
    """Handler for SEAFILE connection type."""
    
    connection_type = ConnectionType.SEAFILE.value

    def gen_table_name(self, connection_id):
        return SeafileTable.gen_table_name(connection_id)
    
    def get_index_instance(self, seasearch_api):
        from seaqa_indexer.index.index_store.seafile_index import SeafileIndex
        return SeafileIndex(seasearch_api)
    
    def get_search_param(self, index_instance, index_name, keyword, start, size, time_from, time_to):
        return index_instance.get_search_param(index_name, keyword, start, size, time_from, time_to)
    
    def delete_index(self, index_manager, connection_id):
        from seaqa_indexer.index.utils import gen_index_name_by_type
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        index_manager.seafile_index.delete_index_by_index_name(index_name)
    
    def format_vector_search_result(self, record, connection, result):
        connection_config = json.loads(connection.config)
        repo_name = connection_config.get('repo_name')
        repo_id = connection_config.get('repo_id')
        server_url = connection_config.get('server_url')
        url = server_url.rstrip('/') + '/' + 'lib' + '/' + repo_id + '/file/' + record.get('path', '').strip(
            '/') + '/' + record.get('title', '')
        
        return {
            'title': record.get('title', ''),
            'path': record.get('path', ''),
            'repo_id': repo_id,
            'repo_name': repo_name,
            'server_url': server_url,
            'modified_time': record.get('modified_time', ''),
            'url': url,
        }
    
    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        from seaqa_indexer.ai_processor.ai_processor import process_seafile_ai
        return process_seafile_ai(seadb_api, connection_id, project_uuid)
    
    def update_index(self, connection, seasearch_api, seaqa_db):
        from seaqa_indexer.index.index_utils import update_seafile_index
        update_seafile_index(connection, seasearch_api, seaqa_db)
    
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
        if old_repo_name != new_repo_name:
            is_update_config = True
        
        seafile_seadb_api = SeafileSeaDBAPI(project_uuid)
        sync_count, total_count = self._sync_seafile(connection_id, repo_id, connection_status, since, 
                                               seahub_api, seaqa_db, seafile_seadb_api, 
                                               is_first_update, sync_logger)
        
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
    
    def add_sync_task(self, app, connection_id):
        app.sync_manager.add_sync_task(connection_id)
    
    def add_recovering_sync_task(self, mq, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        mq.lpush('sync_task', json.dumps(msg_content))

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
