import json
import logging
import datetime

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.seadb_models.models import WebCrawlTable
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.index.utils import gen_index_name_by_type, gen_content_vector_index_name
from seaqa_indexer.utils.constants import IndexStatus
from seaqa_indexer.crawler.storage import get_storage
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.index.index_store.site_index import SiteIndex
from seaqa_indexer.index.index_store.content_vector_index import ContentVectorIndex
from seaqa_indexer.ai_processor.utils import generate_summary
from seaqa_indexer.crawler.utils import url_to_filename

logger = logging.getLogger(__name__)

class SiteHandler(ConnectionHandler):
    """Handler for SITE connection type (web crawling)."""

    connection_type = ConnectionType.SITE.value
    BATCH_SIZE = 50

    def gen_table_name(self, connection_id):
        return WebCrawlTable.gen_table_name(connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        site_index = SiteIndex(seasearch_api)
        return site_index.get_search_param(index_name, keyword, start, size, time_from, time_to)

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        # keyword index
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        github_issues_index = SiteIndex(seasearch_api)
        github_issues_index.delete_index_by_index_name(index_name)
        # content vector index
        content_index_name = gen_content_vector_index_name(connection_id, self.connection_type)
        content_vector_index = ContentVectorIndex(seasearch_api)
        content_vector_index.delete_index_by_index_name(content_index_name)
        # summary vector index
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

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

    def process_ai(self, seadb_api, storage, connection_id, project_uuid, mq=None):
        page_size = 100
        updates = []
        ai_processing_count = 0
        table_name = self.gen_table_name(connection_id)
        while True:
            sql = f"""
                SELECT _pk, url FROM `{table_name}`
                WHERE (ai_processed_time IS NULL OR ai_processed_time < sync_time) AND (deleted IS NULL OR deleted = false)
                LIMIT {page_size}
            """
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                url = row.get('url')
                filename = url_to_filename(url)
                file_json = storage.read_json(connection_id, project_uuid, filename)
                content = file_json.get('content', '')
                summary, vector = generate_summary(content, ConnectionType.SITE.value, project_uuid)
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
                updates = []
        return ai_processing_count

    def update_index(self, project_connection, seasearch_api, seaqa_db, since):
        connection_status = json.loads(project_connection.status)
        site_index = SiteIndex(seasearch_api)

        connection_id = project_connection.id
        project_uuid = project_connection.project_uuid
        connection_type = project_connection.type

        site_index_name = gen_index_name_by_type(connection_id, connection_type)
        site_index.create_index_if_missing(site_index_name)

        seadb_api = SeaDBAPI()
        storage = get_storage()

        index_count = site_index.update(site_index_name, connection_id, project_uuid, connection_status, seaqa_db, seadb_api, storage, since)
        connection_status['last_indexed_count'] = index_count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))

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
