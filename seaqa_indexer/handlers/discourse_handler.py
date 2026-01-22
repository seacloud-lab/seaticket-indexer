import json
import datetime

from dateutil.relativedelta import relativedelta

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.seadb_models.models import DiscourseTopicsTable, DiscourseRepliesTable
from seaqa_indexer.utils.constants import ConnectionType


class DiscourseHandler(ConnectionHandler):
    """Handler for DISCOURSE_FORUM connection type."""
    
    connection_type = ConnectionType.DISCOURSE_FORUM.value
    table_class = DiscourseTopicsTable
    
    def get_index_instance(self, seasearch_api):
        from seaqa_indexer.seasearch.index_store.discourse_index import DiscourseIndex
        return DiscourseIndex(seasearch_api)
    
    def get_search_param(self, index_instance, index_name, keyword, start, size, time_from, time_to):
        return index_instance.get_search_param(index_name, keyword, start, size, time_from, time_to)
    
    def delete_index(self, index_manager, connection_id):
        from seaqa_indexer.seasearch.utils import gen_index_name_by_type
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        index_manager.discourse_index.delete_index_by_index_name(index_name)
    
    def format_vector_search_result(self, record, connection, result):
        connection_config = json.loads(connection.config)
        discourse_forum_url = connection_config.get('url')
        slug = record.get('slug')
        topic_id = record.get('topic_id')
        topic_url = discourse_forum_url.rstrip('/') + '/t/' + slug + '/' + str(topic_id)
        
        return {
            'title': record.get('title', ''),
            'slug': record.get('slug', ''),
            'topic_id': record.get('topic_id', ''),
            'modified_time': record.get('modified_time', ''),
            'url': topic_url,
        }
    
    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        from seaqa_indexer.ai_processor.ai_processor import process_discourse_ai
        return process_discourse_ai(seadb_api, connection_id, project_uuid)
    
    def update_index(self, connection, seasearch_api, seaqa_db):
        from seaqa_indexer.seasearch.index_utils import update_discourse_forum_index
        update_discourse_forum_index(connection, seasearch_api, seaqa_db)
    
    def sync(self, connection_id, project_uuid, config, old_last_sync_time, 
             connection_status, seaqa_db, sync_logger):
        from seaqa_indexer.utils import time_to_utc_time
        from seaqa_indexer.utils.discourse_forum_api import DiscourseForumAPI
        from seaqa_indexer.seadb_models.discourse_seadb_api import DiscourseSeaDBAPI
        from seaqa_indexer.syncer.sync_connection import sync_discourse
        
        forum_url = config['url']
        discourse_api_key = config['api_key']
        discourse_api_username = config['api_username']
        sync_years = config.get('sync_years', 5)
        
        project_connection = seaqa_db.get_project_connection_by_id(connection_id)
        project_created_at = project_connection.created_at
        
        discourse_forum_api = DiscourseForumAPI(forum_url, discourse_api_key, discourse_api_username)
        discourse_seadb_api = DiscourseSeaDBAPI(project_uuid)
        
        # Calculate sync_earliest_time by subtracting sync_years from project_created_time
        project_created_time_utc = time_to_utc_time(str(project_created_at))
        sync_earliest_time = project_created_time_utc - relativedelta(years=sync_years)
        
        return sync_discourse(connection_id, sync_earliest_time, old_last_sync_time, 
                             connection_status, discourse_forum_api, discourse_seadb_api, seaqa_db)
    
    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, 
                              table_name, cutoff_time):
        """Clean deleted discourse topic records and their replies."""
        page_size = 1000
        replies_table_name = DiscourseRepliesTable.gen_table_name(connection_id)
        total_deleted = 0

        while True:
            sql = f"""
                SELECT _pk, topic_id FROM `{table_name}` 
                WHERE deleted = true AND sync_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            deleted_pks = [row.get('_pk') for row in rows]
            deleted_topic_ids = [row.get('topic_id') for row in rows]

            seadb_api.delete_rows(project_uuid, table_name, deleted_pks)
            for topic_id in deleted_topic_ids:
                delete_sql = f"DELETE FROM `{replies_table_name}` WHERE topic_id = {topic_id}"
                seadb_api.query_rows(project_uuid, delete_sql)

            total_deleted += len(deleted_pks)

            if len(rows) < page_size:
                break

        return total_deleted
    
    def clean_connection_data(self, storage, seadb_api, project_uuid, 
                              connection_id, tables):
        """Clean discourse connection data including topics and replies tables."""
        if tables:
            topics_table_name = DiscourseTopicsTable.gen_table_name(connection_id)
            replies_table_name = DiscourseRepliesTable.gen_table_name(connection_id)
            for table in tables:
                if table['name'] in (topics_table_name, replies_table_name):
                    seadb_api.delete_table(project_uuid, table['id'])
    
    def add_sync_task(self, app, connection_id):
        app.sync_manager.add_sync_task(connection_id)
    
    def add_recovering_sync_task(self, mq, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        mq.lpush('sync_task', json.dumps(msg_content))
