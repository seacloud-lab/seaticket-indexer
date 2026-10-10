import logging
import json
import datetime

from abc import ABC, abstractmethod
from seaqa_indexer.utils.seaqa_ai_api import SeaQAAIAPI
from seaqa_indexer.index.index_store.summary_vector_index import SummaryVectorIndex
from seaqa_indexer.utils.constants import IndexStatus, EVENT_CONTENT_SIZE_LIMIT, EVENT_CONTENT_TRUNCATED_NOTICE
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
    def process_ai(self, seadb_api, connection_id, project_uuid) -> int:
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
                WHERE deleted = true AND record_modified_time < '{cutoff_time}'
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

    def build_event(self, event_type, old_row=None, new_row=None, ignored_fields=None, date_fields=None):
        old_row = old_row or {}
        new_row = new_row or {}
        old_value = {}
        new_value = {}
        ignored_fields = set(ignored_fields or [])
        date_fields = set(date_fields or [])

        for field_name, field_value in new_row.items():
            if field_name in ignored_fields:
                continue
            old_field_value = old_row.get(field_name)
            if field_name in date_fields:
                try:
                    old_field_value = self._normalize_event_date(old_field_value)
                    field_value = self._normalize_event_date(field_value)
                except (TypeError, ValueError):
                    logger.warning(
                        'Failed to normalize event date field %s: %r != %r',
                        field_name, old_field_value, field_value,
                    )
            if old_field_value == field_value:
                continue
            old_value[field_name] = old_field_value
            new_value[field_name] = field_value

        return {
            'type': event_type,
            'old_value': old_value or None,
            'new_value': new_value or None,
        }

    @staticmethod
    def _normalize_event_date(value):
        if value in (None, ''):
            return value
        if isinstance(value, datetime.datetime):
            return value.date().isoformat()
        if isinstance(value, datetime.date):
            return value.isoformat()

        value_str = str(value)
        try:
            date_value = datetime.date.fromisoformat(value_str)
        except ValueError:
            datetime_value = f'{value_str[:-1]}+00:00' if value_str.endswith('Z') else value_str
            return datetime.datetime.fromisoformat(datetime_value).date().isoformat()
        if date_value.isoformat() != value_str:
            raise ValueError('Event date must use YYYY-MM-DD.')
        return value_str

    def should_publish_state_event(self, state_event) -> bool:
        if state_event.get('old_value') is not None or state_event.get('new_value') is not None:
            return True
        return False

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
        state_event_ignored_fields=None,
        state_event_date_fields=None,
        state_event_extra_old_value=None,
        state_event_extra_new_value=None,
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

            if old_row and additional_items and additional_event_type:
                for item in additional_items:
                    if not isinstance(item, dict):
                        continue
                    related_event = {
                        'type': additional_event_type,
                        'old_value': None,
                        'new_value': dict(item),
                    }
                    self._publish_data_event(base_event, related_event)

            state_event = self.build_event(
                state_event_type,
                old_row=old_row,
                new_row=latest_row,
                ignored_fields=state_event_ignored_fields,
                date_fields=state_event_date_fields,
            )
            if state_event_extra_old_value and state_event.get('old_value') is not None:
                state_event['old_value'] = {**state_event['old_value'], **state_event_extra_old_value}
            if state_event_extra_new_value and state_event.get('new_value') is not None:
                state_event['new_value'] = {**state_event['new_value'], **state_event_extra_new_value}
            if self.should_publish_state_event(state_event):
                self._publish_data_event(base_event, state_event)
        except Exception as e:
            logger.warning('Failed to publish data_events for synced %s %s: %s', log_object_name, item_id, e)

    def _publish_data_event(self, base_event, event):
        event = {
            **event,
            'old_value': self._limit_event_content(event.get('old_value')),
            'new_value': self._limit_event_content(event.get('new_value')),
        }
        self.mq.publish('data_events', json.dumps({**base_event, 'event': event}))

    @staticmethod
    def _limit_event_content(values):
        """Cap the body an event carries, since it is prompted to an LLM as is."""
        if not values:
            return values

        content = values.get('content')
        if not isinstance(content, str) or len(content) <= EVENT_CONTENT_SIZE_LIMIT:
            return values
        return {
            **values,
            'content': content[:EVENT_CONTENT_SIZE_LIMIT] + EVENT_CONTENT_TRUNCATED_NOTICE,
        }
