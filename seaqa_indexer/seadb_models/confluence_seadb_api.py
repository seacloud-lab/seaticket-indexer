import datetime
import html2text

from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.utils.constants import SEADB_CONTENT_SIZE_LIMIT
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import SchemaTables


class ConfluenceSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def get_confluence_pages_count(self, connection_id):
        table_name = SchemaTables.CONFLUENCE.table_name(connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        response = self.seadb_api.query_rows(self.base_id, sql)
        page_count = 0
        if response and 'results' in response and response['results']:
            page_count = response['results'][0].get('count', 0)
        return page_count

    def _convert_content(self, html_content):
        if not html_content:
            return ''

        handler = html2text.HTML2Text()
        handler.ignore_links = False
        handler.ignore_images = False
        handler.body_width = 0
        return handler.handle(html_content).strip()

    def _to_isoformat(self, time_value):
        if not time_value:
            return datetime.datetime.now(datetime.UTC).isoformat()
        return time_to_utc_time(time_value).isoformat()

    def save_or_update_page_to_db(self, connection_id, confluence_page, existed_page):
        content = self._convert_content(confluence_page.get('content'))
        if content and len(content) > SEADB_CONTENT_SIZE_LIMIT:
            content = content[:SEADB_CONTENT_SIZE_LIMIT]

        now_iso = datetime.datetime.now(datetime.UTC).isoformat()
        insert_row = {
            SchemaTables.CONFLUENCE.column.title.name: confluence_page.get('title'),
            SchemaTables.CONFLUENCE.column.content.name: content,
            SchemaTables.CONFLUENCE.column.created_time.name: self._to_isoformat(confluence_page.get('created_time')),
            SchemaTables.CONFLUENCE.column.modified_time.name: self._to_isoformat(confluence_page.get('modified_time')),
            SchemaTables.CONFLUENCE.column.sync_time.name: now_iso,
            SchemaTables.CONFLUENCE.column.record_modified_time.name: now_iso,
            SchemaTables.CONFLUENCE.column.creator.name: confluence_page.get('creator'),
            SchemaTables.CONFLUENCE.column.last_modifier.name: confluence_page.get('last_modifier'),
            SchemaTables.CONFLUENCE.column.page_id.name: confluence_page.get('page_id'),
            SchemaTables.CONFLUENCE.column.parent_page_id.name: confluence_page.get('parent_page_id'),
            SchemaTables.CONFLUENCE.column.deleted.name: bool(confluence_page.get('deleted', False)),
            SchemaTables.CONFLUENCE.column.state.name: confluence_page.get('state') or '',
            SchemaTables.CONFLUENCE.column.labels.name: confluence_page.get('labels') or [],
        }

        table_name = SchemaTables.CONFLUENCE.table_name(connection_id)
        if existed_page:
            insert_row.pop(SchemaTables.CONFLUENCE.column.outdated.name, None)
            update_row = {
                'pk': existed_page.get('_pk'),
                'row': insert_row,
            }
            self.seadb_api.update_rows(self.base_id, table_name, [update_row])
        else:
            insert_row[SchemaTables.CONFLUENCE.column.outdated.name] = False
            self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])

    def get_page_by_page_id(self, page_id, connection_id):
        table_name = SchemaTables.CONFLUENCE.table_name(connection_id)
        sql = f"SELECT `_pk`, `modified_time`, `deleted`, `state` FROM `{table_name}` WHERE page_id='{page_id}'"
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0]
        return {}

    def get_confluence_pages_by_page(self, connection_id, last_indexed_at, page_size, offset):
        table_name = SchemaTables.CONFLUENCE.table_name(connection_id)
        sql = f"""SELECT `_pk`, `title`, `content`, `modified_time`, `page_id`, `deleted`, `outdated`
                FROM `{table_name}`
                WHERE record_modified_time >= '{last_indexed_at}'
                LIMIT {page_size} OFFSET {offset}"""
        response = self.seadb_api.query_rows(self.base_id, sql)
        return response.get('results', [])

    def get_need_ai_confluence_pages_by_page(self, connection_id, page_size):
        table_name = SchemaTables.CONFLUENCE.table_name(connection_id)
        sql = f"""
            SELECT `_pk`, `title`, `content` FROM `{table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < sync_time)
              AND (deleted IS NULL OR deleted = false)
            ORDER BY `_pk`
            LIMIT {page_size}
        """
        response = self.seadb_api.query_rows(self.base_id, sql)
        return response.get('results', [])

    def get_latest_modified_time(self, connection_id):
        table_name = SchemaTables.CONFLUENCE.table_name(connection_id)
        sql = f"""
            SELECT `modified_time` FROM `{table_name}`
            WHERE (deleted IS NULL OR deleted = false)
            ORDER BY `modified_time` DESC
            LIMIT 1
        """
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0].get('modified_time')
        return None

    def get_page_id_rows_by_page(self, connection_id, page_size, offset):
        table_name = SchemaTables.CONFLUENCE.table_name(connection_id)
        sql = f"""
            SELECT `_pk`, `page_id`, `deleted` FROM `{table_name}`
            WHERE (deleted IS NULL OR deleted = false)
            LIMIT {page_size} OFFSET {offset}
        """
        response = self.seadb_api.query_rows(self.base_id, sql)
        return response.get('results', [])

    def mark_page_deleted(self, connection_id, pk, modified_time=None):
        table_name = SchemaTables.CONFLUENCE.table_name(connection_id)
        now_iso = datetime.datetime.now(datetime.UTC).isoformat()
        update_row = {
            'pk': pk,
            'row': {
                SchemaTables.CONFLUENCE.column.deleted.name: True,
                SchemaTables.CONFLUENCE.column.modified_time.name: modified_time or now_iso,
                SchemaTables.CONFLUENCE.column.sync_time.name: now_iso,
                SchemaTables.CONFLUENCE.column.record_modified_time.name: now_iso,
            }
        }
        self.seadb_api.update_rows(self.base_id, table_name, [update_row])

    def mark_pages_deleted(self, connection_id, stale_page_ids, local_page_map):
        batch_size_limit = 100
        table_name = SchemaTables.CONFLUENCE.table_name(connection_id)
        now_iso = datetime.datetime.now(datetime.UTC).isoformat()
        updates = []

        for page_id in stale_page_ids:
            pk = local_page_map.get(page_id)
            if pk is None:
                continue
            updates.append({
                'pk': pk,
                'row': {
                    SchemaTables.CONFLUENCE.column.deleted.name: True,
                    SchemaTables.CONFLUENCE.column.modified_time.name: now_iso,
                    SchemaTables.CONFLUENCE.column.sync_time.name: now_iso,
                    SchemaTables.CONFLUENCE.column.record_modified_time.name: now_iso,
                }
            })
            if len(updates) >= batch_size_limit:
                self.seadb_api.update_rows(self.base_id, table_name, updates)
                updates = []

        if updates:
            self.seadb_api.update_rows(self.base_id, table_name, updates)

    def get_table_metadata(self, connection_id):
        table_name = SchemaTables.CONFLUENCE.table_name(connection_id=connection_id)
        metadata = self.seadb_api.get_base_metadata(self.base_id)
        for table in metadata.get('tables', []):
            if table['name'] == table_name:
                return table
        return None

    def ensure_label_column_options(self, labels, table_info):
        """Ensure all label names exist as column options.
        Labels is a list of name strings (e.g. ['label-a', 'label-b'])."""
        if not labels or not table_info:
            return

        table_id = table_info.get('id')
        columns = table_info.get('columns', [])
        label_column = {}
        for column in columns:
            if column.get('name') == SchemaTables.CONFLUENCE.column.labels.name:
                label_column = column
                break
        if not label_column:
            return

        existing_names = set()
        if label_column.get('data'):
            for option in (label_column['data'].get('options') or []):
                existing_names.add(option.get('name'))

        column_key = label_column.get('key')
        for label_name in labels:
            if label_name not in existing_names:
                self.seadb_api.add_column_option(self.base_id, {
                    'table_id': table_id,
                    'column_key': column_key,
                    'option_name': label_name,
                })
                existing_names.add(label_name)
