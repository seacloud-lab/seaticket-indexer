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
            SchemaTables.CONFLUENCE.column.creator_id.name: confluence_page.get('creator_id'),
            SchemaTables.CONFLUENCE.column.last_modifier_id.name: confluence_page.get('last_modifier_id'),
            SchemaTables.CONFLUENCE.column.page_id.name: confluence_page.get('page_id'),
            SchemaTables.CONFLUENCE.column.parent_page_id.name: confluence_page.get('parent_page_id'),
            SchemaTables.CONFLUENCE.column.deleted.name: bool(confluence_page.get('deleted', False)),
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
        sql = f"SELECT `_pk`, `modified_time`, `deleted` FROM `{table_name}` WHERE page_id='{page_id}'"
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
            WHERE (ai_processed_time IS NULL OR ai_processed_time < record_modified_time)
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

    # ── User table methods ──────────────────────────────────────────────

    def upsert_user(self, connection_id, account_id, display_name):
        """Insert or update a user record with merge logic.

        If the user already exists (by user_id), append connection_id to the
        list if not already present. If new, create with a single-element list.
        """
        if not account_id or not display_name:
            return
        table_name = SchemaTables.CONNECTION_USER.table_name()
        now_iso = datetime.datetime.now(datetime.UTC).isoformat()
        escaped_account_id = account_id.replace("'", "''")

        sql = f"SELECT `_pk`, `connection_id` FROM `{table_name}` WHERE user_id='{escaped_account_id}'"
        response = self.seadb_api.query_rows(self.base_id, sql)
        existing = response.get('results', [])

        if existing:
            pk = existing[0]['_pk']
            existing_conn_ids = existing[0].get('connection_id') or []
            if connection_id in existing_conn_ids:
                # User already known in this connection — just refresh name/time
                update_row = {
                    'pk': pk,
                    'row': {
                        SchemaTables.CONNECTION_USER.column.name.name: display_name,
                        SchemaTables.CONNECTION_USER.column.record_modified_time.name: now_iso,
                    }
                }
                self.seadb_api.update_rows(self.base_id, table_name, [update_row])
            else:
                # User exists in other connections — append this connection_id
                new_conn_ids = list(set(existing_conn_ids) | {connection_id})
                update_row = {
                    'pk': pk,
                    'row': {
                        SchemaTables.CONNECTION_USER.column.name.name: display_name,
                        SchemaTables.CONNECTION_USER.column.connection_id.name: new_conn_ids,
                        SchemaTables.CONNECTION_USER.column.record_modified_time.name: now_iso,
                    }
                }
                self.seadb_api.update_rows(self.base_id, table_name, [update_row])
        else:
            # Brand new user
            insert_row = {
                SchemaTables.CONNECTION_USER.column.user_id.name: account_id,
                SchemaTables.CONNECTION_USER.column.name.name: display_name,
                SchemaTables.CONNECTION_USER.column.connection_id.name: [connection_id],
                SchemaTables.CONNECTION_USER.column.record_modified_time.name: now_iso,
            }
            self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])

    def delete_user_by_account_id(self, connection_id, account_id):
        """Remove a connection from a user's connection_id list.

        Removes the connection_id from the list. If the list becomes empty
        after removal, deletes the entire row. Used for GDPR privacy reporting
        when an Atlassian account is closed.
        """
        if not account_id:
            return
        table_name = SchemaTables.CONNECTION_USER.table_name()
        escaped_account_id = account_id.replace("'", "''")

        sql = f"SELECT `_pk`, `connection_id` FROM `{table_name}` WHERE user_id='{escaped_account_id}'"
        response = self.seadb_api.query_rows(self.base_id, sql)
        results = response.get('results', [])
        if not results:
            return

        now_iso = datetime.datetime.now(datetime.UTC).isoformat()
        for row in results:
            existing_ids = row.get('connection_id') or []
            if connection_id not in existing_ids:
                continue
            existing_ids.remove(connection_id)
            if not existing_ids:
                self.seadb_api.delete_rows(self.base_id, table_name, [row['_pk']])
            else:
                update_row = {
                    'pk': row['_pk'],
                    'row': {
                        SchemaTables.CONNECTION_USER.column.connection_id.name: existing_ids,
                        SchemaTables.CONNECTION_USER.column.record_modified_time.name: now_iso,
                    }
                }
                self.seadb_api.update_rows(self.base_id, table_name, [update_row])

    def get_all_user_accounts(self, connection_id, page_size=100):
        """Get all (user_id, record_modified_time) pairs for a connection."""
        table_name = SchemaTables.CONNECTION_USER.table_name()
        results = []
        offset = 0
        while True:
            sql = (
                f"SELECT `user_id`, `record_modified_time` FROM `{table_name}` "
                f"WHERE {connection_id} IN connection_id "
                f"ORDER BY `_pk` LIMIT {page_size} OFFSET {offset}"
            )
            page = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
            results.extend(page)
            if len(page) < page_size:
                return results
            offset += page_size

    def get_all_users(self):
        """Get all project-level users and their related connection IDs."""
        table_name = SchemaTables.CONNECTION_USER.table_name()
        results = []
        page_size = 100
        offset = 0
        while True:
            sql = (
                f"SELECT `_pk`, `user_id`, `name`, `connection_id`, `record_modified_time` "
                f"FROM `{table_name}` ORDER BY `_pk` LIMIT {page_size} OFFSET {offset}"
            )
            page = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
            results.extend(page)
            if len(page) < page_size:
                return results
            offset += page_size

    def delete_users(self, pks):
        """Delete project-level user rows by primary key."""
        pks = [pk for pk in pks if pk is not None]
        if not pks:
            return
        table_name = SchemaTables.CONNECTION_USER.table_name()
        self.seadb_api.delete_rows(self.base_id, table_name, list(set(pks)))

    def update_user_names(self, user_names):
        """Batch-update project-level user display names by primary key."""
        if not user_names:
            return
        table_name = SchemaTables.CONNECTION_USER.table_name()
        now_iso = datetime.datetime.now(datetime.UTC).isoformat()
        updates = [
            {
                'pk': pk,
                'row': {
                    SchemaTables.CONNECTION_USER.column.name.name: name,
                    SchemaTables.CONNECTION_USER.column.record_modified_time.name: now_iso,
                },
            }
            for pk, name in user_names.items()
            if pk is not None and name
        ]
        if updates:
            self.seadb_api.update_rows(self.base_id, table_name, updates)
