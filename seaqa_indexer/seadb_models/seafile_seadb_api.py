import datetime
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.constants import SEADB_CONTENT_SIZE_LIMIT

from seaqa_indexer.seadb_models.models import SchemaTables



class SeafileSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def save_or_update_file_to_db(self, connection_id, file, existed_file):
        content = file.get('content')
        path = file['path']
        filename = file['filename']
        file_id = file.get('file_id')

        insert_row = {
            SchemaTables.SEAFILE.column.file_id.name: file_id,
            SchemaTables.SEAFILE.column.path.name: path,
            SchemaTables.SEAFILE.column.title.name: filename,
            SchemaTables.SEAFILE.column.content.name: content[:SEADB_CONTENT_SIZE_LIMIT],
            SchemaTables.SEAFILE.column.modified_time.name: file.get('modified_time'),
            SchemaTables.SEAFILE.column.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            SchemaTables.SEAFILE.column.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            SchemaTables.SEAFILE.column.deleted.name: False,
        }
        table_name = SchemaTables.SEAFILE.table_name(connection_id=connection_id)

        if existed_file:
            _pk = existed_file.get('_pk')
            update_row = [
                {
                "pk":_pk,
                "row":insert_row,
                }
            ]
            self.seadb_api.update_rows(self.base_id, table_name, update_row)
        else:
            self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])

    def mark_file_as_deleted(self, connection_id, need_deleted_file_ids, file_id_to_pk):
        batch_size_limit = 100
        table_name = SchemaTables.SEAFILE.table_name(connection_id=connection_id)
        now_time = datetime.datetime.now(datetime.UTC).isoformat()
        updates = []

        for file_id in need_deleted_file_ids:
            pk = file_id_to_pk.get(file_id)
            updates.append({
                'pk': pk,
                'row': {
                    'deleted': True,
                    'sync_time': now_time,
                    'record_modified_time': now_time,
                }
            })
            if len(updates) >= batch_size_limit:
                self.seadb_api.update_rows(self.base_id, table_name, updates)
                updates = []

        if updates:
            self.seadb_api.update_rows(self.base_id, table_name, updates)

    def get_file_total_count(self, connection_id):
        table_name = SchemaTables.SEAFILE.table_name(connection_id=connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        result = self.seadb_api.query_rows(self.base_id, sql)
        return result['results'][0].get('count', 0)

    def get_files_by_page(self, connection_id, since, page_size, offset):
        table_name = SchemaTables.SEAFILE.table_name(connection_id=connection_id)
        sql = f"""SELECT `_pk`, `title`, `deleted`, `outdated`, `modified_time`, `content` FROM `{table_name}` 
                WHERE record_modified_time >= '{since}' 
                LIMIT {page_size} OFFSET {offset}"""
        res = self.seadb_api.query_rows(self.base_id, sql)
        results = res.get('results', [])
        return results

    def delete_files_from_db(self, connection_id, deleted_seadb_pk_list):
        table_name = SchemaTables.SEAFILE.table_name(connection_id=connection_id)
        self.seadb_api.delete_rows(self.base_id, table_name, deleted_seadb_pk_list)

    def get_file_by_file_id(self, connection_id, file_id):
        table_name = SchemaTables.SEAFILE.table_name(connection_id=connection_id)
        sql = f"SELECT _pk, modified_time FROM `{table_name}` WHERE `file_id` = '{file_id}'"
        res = self.seadb_api.query_rows(self.base_id, sql)
        files = res.get('results', [])
        return files[0] if files else {}

    def get_need_ai_files_by_page(self, connection_id, page_size):
        table_name = SchemaTables.SEAFILE.table_name(connection_id=connection_id)
        sql = f"""
            SELECT `_pk`, `title`, `content` FROM `{table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < sync_time) AND (deleted IS NULL OR deleted = false)
            LIMIT {page_size}
        """
        res = self.seadb_api.query_rows(self.base_id, sql)
        results = res.get('results', [])
        return results
