import json
import datetime
import logging

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.index.index_store.notion_index import NotionIndex
from seaqa_indexer.seadb_models.models import NotionTable
from seaqa_indexer.seadb_models.notion_seadb_api import NotionSeaDBAPI
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.utils.constants import IndexStatus
from seaqa_indexer.utils import need_update_connection_last_sync_count, time_to_utc_time, get_since_time
from seaqa_indexer.ai_processor.utils import generate_summary
from seaqa_indexer.utils.notion_api import NotionAPI

logger = logging.getLogger(__name__)


class NotionHandler(ConnectionHandler):
    """Handler for Notion connection type."""

    connection_type = ConnectionType.NOTION.value
    BATCH_SIZE = 50

    def gen_table_name(self, connection_id):
        return NotionTable.gen_table_name(connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        notion_index = NotionIndex(seasearch_api)
        return notion_index.get_search_param(index_name, keyword, start, size, time_from, time_to)

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        notion_index = NotionIndex(seasearch_api)
        notion_index.delete_index_by_index_name(index_name)

        # summary vector index
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

    def format_vector_search_result(self, record, connection, result):
        page_id = record.get('page_id') or ''
        url = f"https://www.notion.so/{page_id}" if page_id else ''
        return {
            'title': record.get('title', ''),
            'modified_time': record.get('modified_time', ''),
            'page_id': page_id,
            'url': url,
        }

    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = NotionTable.gen_table_name(connection_id)
        notion_seadb_api = NotionSeaDBAPI(project_uuid)

        ai_processing_count = 0
        while True:
            rows = notion_seadb_api.get_need_ai_notion_pages_by_page(connection_id, page_size)
            if not rows:
                break
            for row in rows:
                pk = row.get('_pk')
                title = row.get('title', '')
                content = row.get('content', '')

                full_content = f"Title: {title}\n\nBody: {content}\n"
                summary, vector = generate_summary(full_content, ConnectionType.NOTION.value, project_uuid)

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
        connection_type = project_connection.type

        connection_status = json.loads(project_connection.status)
        project_uuid = project_connection.project_uuid
        notion_seadb_api = NotionSeaDBAPI(project_uuid)
        notion_index = NotionIndex(seasearch_api)

        connection_status['last_index_status'] = IndexStatus.INDEXING
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

        notion_index_name = gen_index_name_by_type(connection_id, connection_type)
        notion_index.create_index_if_missing(notion_index_name)

        index_count = notion_index.update(notion_index_name, connection_id, connection_status,
                                          seaqa_db, notion_seadb_api, since)
        # finish index
        indexed_at = datetime.datetime.now(datetime.UTC)
        connection_status['last_indexed_count'] = index_count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))

    def sync(self, connection_id, project_uuid, config, old_last_sync_time,
             connection_status, seaqa_db, sync_logger):
        token = config.get('integration_secret')
        notion_api = NotionAPI(token)
        notion_seadb_api = NotionSeaDBAPI(project_uuid)
        sync_count, total_count = self._sync_notion(
            connection_id,
            connection_status,
            notion_api,
            seaqa_db,
            notion_seadb_api,
            old_last_sync_time,
        )
        return sync_count, total_count, False, config

    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, cutoff_time):
        """Clean deleted notion records."""
        page_size = 1000
        table_name = self.gen_table_name(connection_id)
        total_deleted = 0

        while True:
            sql = f"""
                SELECT _pk FROM `{table_name}` 
                WHERE deleted = true AND sync_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            deleted_pks = [row.get('_pk') for row in rows]
            seadb_api.delete_rows(project_uuid, table_name, deleted_pks)
            total_deleted += len(deleted_pks)

            if len(rows) < page_size:
                break

        return total_deleted

    def clean_connection_data(self, storage, seadb_api, project_uuid,
                              connection_id, tables):
        if tables:
            table_name = self.gen_table_name(connection_id)
            for table in tables:
                if table['name'] == table_name:
                    seadb_api.delete_table(project_uuid, table['id'])

    def _sync_notion(self, connection_id, connection_status, notion_api, seaqa_db, notion_seadb_api, last_sync_time):
        per_page = 100  # max 100
        sync_count = 0
        _status_last_update_time = datetime.datetime.now()
        _status_last_update_count = 0
        cursor = None
        if last_sync_time:
            last_sync_time = time_to_utc_time(str(last_sync_time))

        while True:
            pages, next_cursor, has_more = notion_api.search_pages(
                per_page=per_page,
                start_cursor=cursor,
            )
            if not pages:
                break

            for page in pages:
                page_id = page.get('id')
                if not page_id:
                    continue

                last_edited_time = time_to_utc_time(page.get('last_edited_time'))
                if last_sync_time and last_edited_time < last_sync_time:
                    has_more = False
                    break
                in_trash = page.get('in_trash')
                existed_page = notion_seadb_api.get_page_by_page_id(page_id, connection_id)
                if in_trash:
                    if existed_page:
                        notion_seadb_api.mark_page_deleted(
                            connection_id,
                            existed_page.get('_pk'),
                            modified_time=last_edited_time,
                        )
                    continue

                notion_page = notion_api.fetch_full_page(page)
                if not notion_page:
                    continue

                if existed_page:
                    if last_edited_time <= time_to_utc_time(str(existed_page.get('modified_time'))):
                        logger.debug(f'Notion page {page_id} is up to date, skipping sync.')
                        continue

                sync_count += 1
                notion_seadb_api.save_or_update_page_to_db(connection_id, notion_page, existed_page)
                should, new_time, new_count = need_update_connection_last_sync_count(
                    sync_count, _status_last_update_time, _status_last_update_count
                )
                if should:
                    connection_status['last_sync_count'] = sync_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                    _status_last_update_time = new_time
                    _status_last_update_count = new_count

            if not has_more:
                break
            cursor = next_cursor
            if not cursor:
                break

        total_count = notion_seadb_api.get_notion_pages_count(connection_id)
        return sync_count, total_count
