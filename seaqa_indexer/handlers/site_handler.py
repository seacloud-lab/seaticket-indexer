import json
import logging
import datetime

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.seadb_models.models import WebCrawlTable
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.index.index_store.site_index import SiteIndex
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.utils.constants import IndexStatus
from seaqa_indexer.crawler.storage import get_storage
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils import get_since_time

logger = logging.getLogger(__name__)

class SiteHandler(ConnectionHandler):
    """Handler for SITE connection type (web crawling)."""
    
    connection_type = ConnectionType.SITE.value

    def gen_table_name(self, connection_id):
        return WebCrawlTable.gen_table_name(connection_id)
    
    def get_index_instance(self, seasearch_api):
        from seaqa_indexer.index.index_store.site_index import SiteIndex
        return SiteIndex(seasearch_api)
    
    def get_search_param(self, index_instance, index_name, keyword, start, size, time_from, time_to):
        return index_instance.get_search_param(index_name, keyword, start, size, time_from, time_to)
    
    def delete_index(self, index_manager, connection_id):
        from seaqa_indexer.index.utils import gen_index_name_by_type, gen_content_vector_index_name
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        # keyword index
        index_manager.site_index.delete_index_by_index_name(index_name)
        # content vector index
        content_index_name = gen_content_vector_index_name(connection_id, self.connection_type)
        index_manager.content_vector_index.delete_index_by_index_name(content_index_name)
    
    def format_vector_search_result(self, record, connection, result):
        url = record.get('url', '')
        source_type = result.get('source_type')
        formatted = {
            'title': record.get('title', ''),
            'url': url,
        }
        if source_type == 'chunk':
            formatted['source_type'] = 'chunk'
            formatted['content'] = result.get('content', '')
        return formatted
    
    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        from seaqa_indexer.ai_processor.ai_processor import process_site_ai
        return process_site_ai(seadb_api, storage, connection_id, project_uuid)
    
    def update_index(self, project_connection, seasearch_api, seaqa_db):
        connection_status = json.loads(project_connection.status)
        site_index = SiteIndex(seasearch_api)

        connection_id = project_connection.id
        project_uuid = project_connection.project_uuid
        connection_type = project_connection.type
        old_indexed_at = project_connection.indexed_at

        site_index_name = gen_index_name_by_type(connection_id, connection_type)
        site_index.create_index_if_missing(site_index_name)

        seadb_api = SeaDBAPI()
        storage = get_storage()

        since = get_since_time(old_indexed_at)

        index_count = site_index.update(site_index_name, connection_id, project_uuid, connection_status, seaqa_db, seadb_api, storage, since)
        connection_status['last_indexed_count'] = index_count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))
    
    def sync(self, connection_id, project_uuid, config, old_last_sync_time, 
             connection_status, seaqa_db, sync_logger):
        # Site sync is handled by web_crawl_manager, not through this method
        # This is a placeholder - actual site crawling uses different mechanism
        raise NotImplementedError("Site sync is handled by web_crawl_manager")
    
    def clean_connection_data(self, storage, seadb_api, project_uuid, 
                              connection_id, tables):
        """Clean site connection data including crawled files."""
        # Delete crawled file storage
        storage.delete_site(connection_id, project_uuid)
        # Delete seadb table
        if tables:
            for table in tables:
                if table['name'] == self.gen_table_name(connection_id):
                    seadb_api.delete_table(project_uuid, table['id'])
    
    def add_sync_task(self, app, connection_id):
        app.web_crawl_manager.add_crawl_site_task(connection_id)
    
    def add_recovering_sync_task(self, mq, connection_id):
        msg_content = {
            'site_id': connection_id,
        }
        mq.lpush('web_crawl_task', json.dumps(msg_content))
