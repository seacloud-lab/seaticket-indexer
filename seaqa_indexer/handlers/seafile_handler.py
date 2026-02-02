import json
import datetime

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.seadb_models.models import SeafileTable
from seaqa_indexer.utils.constants import ConnectionType, REPO_NAME_CACHE_TIME_OUT


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
        from seaqa_indexer.utils import time_to_utc_time
        from seaqa_indexer.utils.seahub_api import SeahubAPI
        from seaqa_indexer.seadb_models.seafile_seadb_api import SeafileSeaDBAPI
        from seaqa_indexer.syncer.sync_connection import sync_seafile
        from seaqa_indexer.app.event_redis import redis_cache
        
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
        sync_count, total_count = sync_seafile(connection_id, repo_id, connection_status, since, 
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
