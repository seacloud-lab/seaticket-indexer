import logging
import json
import datetime

from abc import ABC, abstractmethod
from seaqa_indexer.utils.seaqa_ai_api import SeaQAAIAPI
from seaqa_indexer.index.index_store.summary_vector_index import SummaryVectorIndex
from seaqa_indexer.utils.constants import IndexStatus
from seaqa_indexer.index.utils import gen_conn_summary_index_name, fix_summary
from seaqa_indexer.utils import time_to_utc_time
from redis import Redis

logger = logging.getLogger(__name__)


class ConnectionHandler(ABC):
    connection_type: str = None
    mq: Redis = None

    def __init__(self):
        self.seaqa_ai_api = SeaQAAIAPI()

    @abstractmethod
    def gen_table_name(self, connection_id) -> str:
        pass

    @abstractmethod
    def get_search_param(self, index_instance, index_name, keyword, start, size, time_from, time_to) -> dict:
        pass

    def delete_summary_vector_index_by_connection_id(self, seasearch_api, connection_id):
        summary_vector_index_name = gen_conn_summary_index_name(connection_id, self.connection_type)
        summary_vector_index = SummaryVectorIndex(seasearch_api)
        summary_vector_index.delete_index_by_index_name(summary_vector_index_name)

    @abstractmethod
    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        pass

    @abstractmethod
    def format_vector_search_result(self, record, connection, result) -> dict:
        pass

    @abstractmethod
    def process_ai(self, seadb_api, storage, connection_id, project_uuid) -> int:
        pass

    @abstractmethod
    def update_index(self, connection, seasearch_api, seaqa_db, since) -> None:
        pass

    def update_summary_index(self, project_uuid, connection_id, seadb_api, ai_status, seaqa_db, seasearch_api, since):
        table_name = self.gen_table_name(connection_id)

        fixed_count = fix_summary(project_uuid, table_name, seadb_api)
        if fixed_count > 0:
            logger.info('Fixed %d missing embeddings for connection %s before indexing', fixed_count, connection_id)

        summary_vector_index = SummaryVectorIndex(seasearch_api)
        ai_index_name = gen_conn_summary_index_name(connection_id, self.connection_type)
        summary_vector_index.create_index_if_missing(ai_index_name)

        index_count = summary_vector_index.update(
            table_name,
            ai_index_name,
            connection_id,
            project_uuid,
            ai_status,
            seaqa_db,
            seadb_api,
            since,
        )

        ai_status['last_ai_indexed_count'] = index_count
        ai_status['last_ai_index_status'] = IndexStatus.COMPLETED
        ai_indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_ai_indexed_at_and_status(connection_id, ai_indexed_at, json.dumps(ai_status))
        logger.info('Finish summary vector indexing for connection %s, indexed %d documents', connection_id,
                    index_count)

    @abstractmethod
    def sync(self, connection_id, project_uuid, config, old_last_sync_time,
             connection_status, seaqa_db, sync_logger) -> tuple:
        """ Returns: tuple: (sync_count, total_count, is_update_config, config) """
        pass

    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, cutoff_time) -> int:
        page_size = 1000
        total_deleted = 0
        table_name = self.gen_table_name(connection_id)

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

    def build_event(self, event_type, old_row=None, new_row=None, ignored_fields=None):
        old_row = old_row or {}
        new_row = new_row or {}
        old_value = {}
        new_value = {}
        ignored_fields = set(ignored_fields or [])

        for field_name, field_value in new_row.items():
            if field_name in ignored_fields:
                continue
            old_field_value = old_row.get(field_name)
            if old_field_value == field_value:
                continue
            old_value[field_name] = old_field_value
            new_value[field_name] = field_value

        return {
            'type': event_type,
            'old_value': old_value or None,
            'new_value': new_value or None,
        }

    def publish_synced_record_events(
        self,
        *,
        get_latest_row,
        project_uuid,
        connection_id,
        item_id,
        old_row,
        state_event_type,
        additional_items=None,
        additional_event_type=None,
        additional_items_field=None,
        state_event_ignored_fields=None,
        log_object_name='record',
    ):
        if not self.mq:
            return

        try:
            latest_row = get_latest_row()
            record_id = latest_row.get('_pk') if latest_row else None
            if record_id is None:
                return

            base_event = {
                'source_type': self.connection_type,
                'project_uuid': str(project_uuid),
                'connection_id': connection_id,
                'record_id': record_id,
                'modified_time': time_to_utc_time(str(latest_row.get('modified_time'))).isoformat(),
            }

            if old_row and additional_items:
                related_event = {
                    'type': additional_event_type,
                    'old_value': None,
                    'new_value': {
                        additional_items_field: additional_items,
                    },
                }
                self.mq.publish('data_events', json.dumps({
                    **base_event,
                    'event': related_event,
                }))

            state_event = self.build_event(
                state_event_type,
                old_row=old_row,
                new_row=latest_row,
                ignored_fields=state_event_ignored_fields,
            )
            if state_event.get('old_value') is not None or state_event.get('new_value') is not None:
                self.mq.publish('data_events', json.dumps({
                    **base_event,
                    'event': state_event,
                }))
        except Exception as e:
            logger.warning('Failed to publish data_events for synced %s %s: %s', log_object_name, item_id, e)
