from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import TicketsTable, TicketCommentsTable

class TicketSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def get_need_ai_tickets_by_page(self, page_size):
        table_name = TicketsTable.gen_table_name()
        sql = f"""
            SELECT * FROM `{table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < modified_time) 
            AND (deleted IS NULL OR deleted != true)
            LIMIT {page_size}
        """
        res = self.seadb_api.query_rows(self.base_id, sql)
        tickets = res.get('results', [])
        return tickets