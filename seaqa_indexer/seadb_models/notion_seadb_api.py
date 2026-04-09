import datetime
from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.utils.converter import notion_page_to_markdown
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import NotionTable

MAX_CONTENT_LENGTH = 10000


class NotionSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def get_notion_pages_count(self, connection_id):
        table_name = NotionTable.gen_table_name(connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        response = self.seadb_api.query_rows(self.base_id, sql)
        page_count = 0
        if response and 'results' in response and response['results']:
            page_count = response['results'][0].get('count', 0)
        return page_count

    def save_or_update_page_to_db(self, connection_id, notion_page, existed_page):
        content = notion_page_to_markdown(notion_page, include_title=False)

        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]

        insert_row = {
            NotionTable.title.name: notion_page.get('title'),
            NotionTable.content.name: content,
            NotionTable.created_time.name: time_to_utc_time(notion_page.get('created_time')).isoformat(),
            NotionTable.modified_time.name: time_to_utc_time(notion_page.get('last_modified_time')).isoformat(),
            NotionTable.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            NotionTable.creator.name: notion_page.get('creator'),
            NotionTable.last_modifier.name: notion_page.get('last_modifier'),
            NotionTable.page_id.name: notion_page.get('page_id'),
            NotionTable.parent_page_id.name: notion_page.get('parent_page_id'),
            NotionTable.deleted.name: notion_page.get('in_trash', False),
        }
        table_name = NotionTable.gen_table_name(connection_id)
        if existed_page:
            _pk = existed_page.get('_pk')
            update_row = {
                "pk": _pk,
                "row": insert_row,
            }
            self.seadb_api.update_rows(self.base_id, table_name, [update_row])
        else:
            self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])


    def get_page_by_page_id(self, page_id, connection_id):
        table_name = NotionTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE page_id='{page_id}'"
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0]
        return {}

    def get_notion_pages_by_page(self, connection_id, last_indexed_at, page_size, offset):
        notion_table_name = NotionTable.gen_table_name(connection_id)
        sql = f"""SELECT * FROM `{notion_table_name}` 
                WHERE record_modified_time >= '{last_indexed_at}' 
                LIMIT {page_size} OFFSET {offset}"""
        res = self.seadb_api.query_rows(self.base_id, sql)
        pages = res.get('results', [])
        return pages


    def get_need_ai_notion_pages_by_page(self, connection_id, page_size):
        notion_table_name = NotionTable.gen_table_name(connection_id)
        sql = f"""
            SELECT * FROM `{notion_table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < sync_time) AND (deleted IS NULL OR deleted = false)
            LIMIT {page_size}
        """
        res = self.seadb_api.query_rows(self.base_id, sql)
        pages = res.get('results', [])
        return pages

    def get_page_id_rows_by_page(self, connection_id, page_size, offset):
        notion_table_name = NotionTable.gen_table_name(connection_id)
        sql = f"""
            SELECT _pk, page_id, deleted FROM `{notion_table_name}`
            WHERE (deleted IS NULL OR deleted = false)
            LIMIT {page_size} OFFSET {offset}
        """
        res = self.seadb_api.query_rows(self.base_id, sql)
        pages = res.get('results', [])
        return pages

    def mark_page_deleted(self, connection_id, pk, modified_time=None):
        table_name = NotionTable.gen_table_name(connection_id)
        now_datetime = datetime.datetime.now(datetime.UTC).isoformat()
        update_row = {
            "pk": pk,
            "row": {
                NotionTable.deleted.name: True,
                NotionTable.modified_time.name: modified_time,
                NotionTable.sync_time.name: now_datetime,
                NotionTable.record_modified_time.name: now_datetime,
            },
        }
        self.seadb_api.update_rows(self.base_id, table_name, [update_row])

    def mark_pages_deleted(self, connection_id, stale_page_ids, local_page_map):
        batch_size_limit = 100
        table_name = NotionTable.gen_table_name(connection_id)
        now_datetime = datetime.datetime.now(datetime.UTC).isoformat()
        updates = []
        for page_id in stale_page_ids:
            pk = local_page_map.get(page_id)
            updates.append({
                'pk': pk,
                'row': {
                    'deleted': True,
                    'sync_time': now_datetime,
                    'record_modified_time': now_datetime,
                }
            })
            if len(updates) >= batch_size_limit:
                self.seadb_api.update_rows(self.base_id, table_name, updates)
                updates = []

        if updates:
            self.seadb_api.update_rows(self.base_id, table_name, updates)
