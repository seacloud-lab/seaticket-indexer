from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import SchemaTables


class TicketSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def get_need_ai_tickets_by_page(self, page_size):
        table_name = SchemaTables.TICKETS.table_name()
        sql = f"""
            SELECT `_pk`, `title`, `content` FROM `{table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < modified_time) 
            AND (deleted IS NULL OR deleted = false)
            LIMIT {page_size}
        """
        res = self.seadb_api.query_rows(self.base_id, sql)
        tickets = res.get('results', [])
        return tickets

    def get_tickets_by_page(self, since, page_size, offset):
        table_name = SchemaTables.TICKETS.table_name()
        sql = f"""SELECT `_pk`, `title`, `content`, `deleted`, `modified_time` FROM `{table_name}`
                WHERE modified_time >= '{since}' LIMIT {page_size} OFFSET {offset}"""
        res = self.seadb_api.query_rows(self.base_id, sql)
        tickets = res.get('results', [])
        return tickets
    
    def delete_tickets_from_db(self, deleted_seadb_pk_list):
        table_name = SchemaTables.TICKETS.table_name()
        self.seadb_api.delete_rows(self.base_id, table_name, deleted_seadb_pk_list)
