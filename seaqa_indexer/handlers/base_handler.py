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
        if not new_row:
            return None

        ignored_fields = set(ignored_fields or [])
        date_fields = set(date_fields or [])
        if not old_row:
            return self._build_added_event(
                event_type,
                new_row=new_row,
                ignored_fields=ignored_fields,
                date_fields=date_fields,
            )
        return self._build_updated_event(
            event_type,
            old_row=old_row,
            new_row=new_row,
            ignored_fields=ignored_fields,
            date_fields=date_fields,
        )

    def _build_added_event(self, event_type, *, new_row, ignored_fields, date_fields):
        new_value = {}
        for field_name, field_value in new_row.items():
            if field_name in ignored_fields:
                continue
            if field_name in date_fields:
                try:
                    field_value = self._normalize_event_date(field_value)
                except (TypeError, ValueError):
                    logger.warning(
                        'Failed to normalize event date field %s: %r',
                        field_name, field_value,
                    )
            if field_value is None:
                continue
            new_value[field_name] = field_value

        if not new_value:
            return None
        return {
            'type': event_type,
            'new_value': new_value,
        }

    def _build_updated_event(self, event_type, *, old_row, new_row, ignored_fields, date_fields):
        old_value = {}
        new_value = {}
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

        if not new_value:
            return None
        return {
            'type': event_type,
            'old_value': old_value,
            'new_value': new_value,
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
        return True

    @staticmethod
    def _build_additional_events(event_type, items):
        if not event_type or not items:
            return []
        return [
            {
                'type': event_type,
                'new_value': dict(item),
            }
            for item in items
            if isinstance(item, dict)
        ]

    def _build_state_event(
        self,
        event_type,
        *,
        old_row,
        new_row,
        ignored_fields=None,
        date_fields=None,
        extra_new_value=None,
    ):
        if not event_type:
            return None
        state_event = self.build_event(
            event_type,
            old_row=old_row,
            new_row=new_row,
            ignored_fields=ignored_fields,
            date_fields=date_fields,
        )
        if state_event is None or not self.should_publish_state_event(state_event):
            return None
        if extra_new_value:
            state_event['new_value'] = {**state_event['new_value'], **extra_new_value}
        return state_event

    def _build_event_envelope(self, project_uuid, connection_id, latest_row):
        return {
            'source_type': self.connection_type,
            'project_uuid': str(project_uuid),
            'connection_id': connection_id,
            'record_id': latest_row['_pk'],
            'modified_time': time_to_utc_time(str(latest_row.get('modified_time'))).isoformat(),
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
        state_event_ignored_fields=None,
        state_event_date_fields=None,
        state_event_extra_new_value=None,
    ):
        if not self.mq:
            return

        try:
            latest_row = get_latest_row()
            if not latest_row or latest_row.get('_pk') is None:
                return

            events = []
            # Opening posts/comments of a new record are covered by its added state event;
            # only existing records emit separate child item events.
            if old_row:
                events.extend(self._build_additional_events(additional_event_type, additional_items))

            state_event = self._build_state_event(
                state_event_type,
                old_row=old_row,
                new_row=latest_row,
                ignored_fields=state_event_ignored_fields,
                date_fields=state_event_date_fields,
                extra_new_value=state_event_extra_new_value,
            )
            if state_event is not None:
                events.append(state_event)

            if not events:
                return

            envelope = self._build_event_envelope(project_uuid, connection_id, latest_row)
            for event in events:
                self._publish_data_event(envelope, event)
        except Exception as e:
            logger.warning(
                'Failed to publish data_events for %s record %s: %s',
                self.connection_type, item_id, e,
            )

    def _publish_data_event(self, base_event, event):
        payload_event = {k: v for k, v in event.items() if k not in ('old_value', 'new_value')}
        for key in ('old_value', 'new_value'):
            value = event.get(key)
            if value is not None:
                payload_event[key] = self._limit_event_content(value)
        self.mq.publish('data_events', json.dumps({**base_event, 'event': payload_event}))

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
