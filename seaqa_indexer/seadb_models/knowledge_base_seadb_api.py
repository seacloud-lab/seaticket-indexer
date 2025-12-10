from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import KnowledgeBaseTable


class KnowledgeBaseSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def get_knowledge_bases_by_page(self, since, page_size, offset):
        table_name = KnowledgeBaseTable.gen_table_name()
        sql = f"SELECT * FROM `{table_name}` WHERE modified_time >= '{since}' LIMIT {page_size} OFFSET {offset}"
        res = self.seadb_api.query_rows(self.base_id, sql)
        results = res.get('results', [])
        return results

    def delete_knowledge_bases_from_db(self, deleted_seadb_pk_list):
        table_name = KnowledgeBaseTable.gen_table_name()
        self.seadb_api.delete_rows(self.base_id, table_name, deleted_seadb_pk_list)
