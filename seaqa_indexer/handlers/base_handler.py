from abc import ABC, abstractmethod


class ConnectionHandler(ABC):
    connection_type: str = None
    table_class = None
    index_class = None
    
    @classmethod
    def gen_table_name(cls, connection_id) -> str:
        if cls.table_class is None:
            raise NotImplementedError(f"table_class not defined for {cls.__name__}")
        return cls.table_class.gen_table_name(connection_id)
    
    @abstractmethod
    def get_index_instance(self, seasearch_api):
        pass
    
    @abstractmethod
    def get_search_param(self, index_instance, index_name, keyword, start, size, time_from, time_to) -> dict:
        pass
    
    @abstractmethod
    def delete_index(self, index_manager, connection_id) -> None:
        pass
    
    @abstractmethod
    def format_vector_search_result(self, record, connection, result) -> dict:
        pass
    
    @abstractmethod
    def process_ai(self, seadb_api, storage, connection_id, project_uuid) -> int:
        pass
    
    @abstractmethod
    def update_index(self, connection, seasearch_api, seaqa_db) -> None:
        pass
    
    @abstractmethod
    def sync(self, connection_id, project_uuid, config, old_last_sync_time, 
             connection_status, seaqa_db, sync_logger) -> tuple:
        """ Returns: tuple: (sync_count, total_count) """
        pass
    
    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, 
                              table_name, cutoff_time) -> int:
        page_size = 1000
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
    
    @abstractmethod
    def clean_connection_data(self, storage, seadb_api, project_uuid, 
                              connection_id, tables) -> None:
        """Clean all SeaDB tables and storage associated with a deleted connection."""
        pass
    
    @abstractmethod
    def add_sync_task(self, app, connection_id) -> None:
        pass
    
    @abstractmethod
    def add_recovering_sync_task(self, mq, connection_id) -> None:
        pass
