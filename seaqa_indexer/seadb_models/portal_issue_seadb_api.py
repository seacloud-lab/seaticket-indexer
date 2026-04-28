from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import PortalIssuesTable


class PortalIssueSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def get_need_ai_portal_issues_by_page(self, page_size):
        table_name = PortalIssuesTable.gen_table_name()
        sql = f"""
            SELECT `_pk`, `title`, `content` FROM `{table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < modified_time)
            AND (deleted IS NULL OR deleted = false)
            LIMIT {page_size}
        """
        res = self.seadb_api.query_rows(self.base_id, sql)
        return res.get('results', [])

    def get_portal_issues_by_page(self, since, page_size, offset):
        table_name = PortalIssuesTable.gen_table_name()
        sql = f"""SELECT `_pk`, `title`, `content`, `deleted`, `modified_time` FROM `{table_name}`
                WHERE modified_time >= '{since}' LIMIT {page_size} OFFSET {offset}"""
        res = self.seadb_api.query_rows(self.base_id, sql)
        return res.get('results', [])
