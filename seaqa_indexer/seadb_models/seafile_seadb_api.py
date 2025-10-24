import datetime
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import SeafileTable

MAX_CONTENT_LENGTH = 10 * 1000


class SeafileSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def save_or_update_file_to_db(self, connection_id, file):
        content = file.get('content')
        path = file['path']
        filename = file['filename']
        if content:
            content = content[:MAX_CONTENT_LENGTH]

        insert_row = {
            SeafileTable.path.name: path,
            SeafileTable.filename.name: filename,
            SeafileTable.content.name: content,
            SeafileTable.mtime.name: file.get('mtime'),
            SeafileTable.updated_at.name: datetime.datetime.now(datetime.UTC).isoformat(),
            SeafileTable.deleted.name: False,
        }
        table_name = SeafileTable.gen_table_name(connection_id)

        sql = f"SELECT * FROM `{table_name}` WHERE `path`='{path}' AND filename='{filename}'"
        issues = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if issues:
            _pk = issues[0].get('_pk')
            update_row = [
                {
                "pk":_pk,
                "row":insert_row,
                }
            ]
            self.seadb_api.update_rows(self.base_id, table_name, update_row)
        else:
            self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])

    def mark_file_as_deleted(self, connection_id, file):
        table_name = SeafileTable.gen_table_name(connection_id)
        path = file.get('path')
        filename = file.get('filename')
        sql = f"""SELECT _pk FROM `{table_name}` WHERE `path`='{path}' AND filename='{filename}'"""
        resp_results = self.seadb_api.query_rows(self.base_id, sql)
        results = resp_results.get('results', [])
        if results:
            rows = []
            for result in results:
                row = {
                    "pk": result.get('_pk'),
                    "row": {
                        SeafileTable.deleted.name: True,
                        SeafileTable.updated_at.name: datetime.datetime.now(datetime.UTC).isoformat(),
                    }
                }
                rows.append(row)
            self.seadb_api.update_rows(self.base_id, table_name, rows)

    def get_file_total_count(self, connection_id):
        table_name = SeafileTable.gen_table_name(connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        result = self.seadb_api.query_rows(self.base_id, sql)
        return result['results'][0].get('count', 0)

    def get_files_by_page(self, connection_id, since, page_size, offset):
        issues_table_name = SeafileTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{issues_table_name}` WHERE updated_at >= '{since}' LIMIT {page_size} OFFSET {offset}"
        res = self.seadb_api.query_rows(self.base_id, sql)
        issues = res.get('results', [])
        return issues

    def delete_files_from_db(self, connection_id, deleted_seadb_pk_list):
        table_name = SeafileTable.gen_table_name(connection_id)
        self.seadb_api.delete_rows(self.base_id, table_name, deleted_seadb_pk_list)

