import json
import importlib.util
import os
from unittest.mock import Mock

BASE_HANDLER_PATH = os.path.join(
    os.path.dirname(__file__),
    '..',
    'handlers',
    'base_handler.py',
)
BASE_HANDLER_SPEC = importlib.util.spec_from_file_location('seaqa_indexer_base_handler', BASE_HANDLER_PATH)
BASE_HANDLER_MODULE = importlib.util.module_from_spec(BASE_HANDLER_SPEC)
BASE_HANDLER_SPEC.loader.exec_module(BASE_HANDLER_MODULE)
ConnectionHandler = BASE_HANDLER_MODULE.ConnectionHandler


class DummyHandler(ConnectionHandler):
    connection_type = 'dummy_source'

    def gen_table_name(self, connection_id):
        return 'dummy_table'

    def get_search_param(self, index_instance, index_name, keyword, start, size, time_from, time_to):
        return {}

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        return None

    def format_vector_search_result(self, record, connection, result):
        return {}

    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        return 0

    def update_index(self, connection, seasearch_api, seaqa_db, since):
        return None

    def sync(self, connection_id, project_uuid, config, old_last_sync_time, connection_status, seaqa_db, sync_logger):
        return 0, 0, False, config


class TestBuildEvent:
    def test_build_event_ignores_unchanged_and_ignored_fields(self):
        handler = DummyHandler()

        event = handler.build_event(
            'dummy_updated',
            old_row={'name': 'old', 'modified_time': 't1', 'state': 'open'},
            new_row={'name': 'new', 'modified_time': 't2', 'state': 'open'},
            ignored_fields=('modified_time',),
        )

        assert event['type'] == 'dummy_updated'
        assert event['old_value'] == {'name': 'old'}
        assert event['new_value'] == {'name': 'new'}

    def test_build_event_no_change_returns_none_values(self):
        handler = DummyHandler()

        event = handler.build_event(
            'dummy_updated',
            old_row={'name': 'same'},
            new_row={'name': 'same'},
        )

        assert event['old_value'] is None
        assert event['new_value'] is None

    def test_build_event_handles_none_inputs(self):
        handler = DummyHandler()

        event = handler.build_event('dummy_updated', old_row=None, new_row=None)

        assert event['type'] == 'dummy_updated'
        assert event['old_value'] is None
        assert event['new_value'] is None

    def test_build_event_when_old_row_missing(self):
        handler = DummyHandler()

        event = handler.build_event(
            'dummy_added',
            old_row=None,
            new_row={'title': 'new'},
        )

        assert event['old_value'] == {'title': None}
        assert event['new_value'] == {'title': 'new'}


class TestPublishSyncedRecordEvents:
    def test_publish_skip_when_mq_not_set(self):
        handler = DummyHandler()
        handler.mq = None

        handler.publish_synced_record_events(
            get_latest_row=lambda: {'_pk': 1, 'modified_time': '2026-05-22T10:00:00+00:00'},
            project_uuid='project-0',
            connection_id=1,
            item_id='item-1',
            old_row={'title': 'old'},
            state_event_type='dummy_updated',
        )

    def test_publish_state_event_success(self):
        handler = DummyHandler()
        handler.mq = Mock()

        latest_row = {
            '_pk': 11,
            'title': 'new title',
            'modified_time': '2026-05-22T10:00:00+00:00',
        }

        handler.publish_synced_record_events(
            get_latest_row=lambda: latest_row,
            project_uuid='project-1',
            connection_id=9,
            item_id='item-9',
            old_row={'_pk': 11, 'modified_time': '2026-05-22T09:00:00+00:00', 'title': 'old title'},
            state_event_type='dummy_updated',
            state_event_ignored_fields=('_pk', 'modified_time'),
        )

        handler.mq.publish.assert_called_once()
        channel, payload = handler.mq.publish.call_args[0]
        payload = json.loads(payload)

        assert channel == 'data_events'
        assert payload['source_type'] == 'dummy_source'
        assert payload['project_uuid'] == 'project-1'
        assert payload['connection_id'] == 9
        assert payload['record_id'] == 11
        assert payload['modified_time'] == '2026-05-22T10:00:00+00:00'
        assert payload['event']['type'] == 'dummy_updated'
        assert payload['event']['old_value'] == {'title': 'old title'}
        assert payload['event']['new_value'] == {'title': 'new title'}

    def test_publish_additional_event_and_state_event(self):
        handler = DummyHandler()
        handler.mq = Mock()

        latest_row = {
            '_pk': 20,
            'summary': 'updated',
            'modified_time': '2026-05-22T10:00:00+00:00',
        }

        handler.publish_synced_record_events(
            get_latest_row=lambda: latest_row,
            project_uuid='project-2',
            connection_id=10,
            item_id='item-10',
            old_row={'_pk': 20, 'modified_time': '2026-05-22T09:00:00+00:00', 'summary': 'old'},
            state_event_type='dummy_updated',
            additional_items=[{'id': 1}],
            additional_event_type='dummy_comment_added',
            state_event_ignored_fields=('_pk', 'modified_time'),
        )

        assert handler.mq.publish.call_count == 2
        first_payload = json.loads(handler.mq.publish.call_args_list[0][0][1])
        second_payload = json.loads(handler.mq.publish.call_args_list[1][0][1])

        assert first_payload['event']['type'] == 'dummy_comment_added'
        assert first_payload['event']['new_value'] == {'id': 1}

        assert second_payload['event']['type'] == 'dummy_updated'
        assert second_payload['event']['old_value'] == {'summary': 'old'}
        assert second_payload['event']['new_value'] == {'summary': 'updated'}

    def test_publish_state_event_merges_extra_new_value(self):
        handler = DummyHandler()
        handler.mq = Mock()

        handler.publish_synced_record_events(
            get_latest_row=lambda: {'_pk': 11, 'title': 'new title', 'modified_time': '2026-05-22T10:00:00+00:00'},
            project_uuid='project-1',
            connection_id=9,
            item_id='item-9',
            old_row=None,
            state_event_type='dummy_added',
            state_event_ignored_fields=('modified_time',),
            state_event_extra_new_value={'email_from': 'alice@example.com'},
        )

        handler.mq.publish.assert_called_once()
        payload = json.loads(handler.mq.publish.call_args[0][1])
        assert payload['event']['type'] == 'dummy_added'
        assert payload['event']['new_value']['email_from'] == 'alice@example.com'
        assert payload['event']['new_value']['title'] == 'new title'

    def test_publish_state_event_extra_new_value_does_not_force_publish(self):
        handler = DummyHandler()
        handler.mq = Mock()

        handler.publish_synced_record_events(
            get_latest_row=lambda: {'_pk': 11, 'title': 'same', 'modified_time': '2026-05-22T10:00:00+00:00'},
            project_uuid='project-1',
            connection_id=9,
            item_id='item-9',
            old_row={'_pk': 11, 'title': 'same', 'modified_time': '2026-05-22T09:00:00+00:00'},
            state_event_type='dummy_updated',
            state_event_ignored_fields=('_pk', 'modified_time'),
            state_event_extra_new_value={'email_from': 'alice@example.com'},
        )

        handler.mq.publish.assert_not_called()

    def test_publish_additional_event_preserves_message_content(self):
        handler = DummyHandler()
        handler.mq = Mock()

        handler.publish_synced_record_events(
            get_latest_row=lambda: {'_pk': 20, 'modified_time': '2026-05-22T10:00:00+00:00'},
            project_uuid='project-2',
            connection_id=10,
            item_id='item-10',
            old_row={'_pk': 20},
            state_event_type='dummy_updated',
            additional_items=[{'message_id': 'message-1', 'content': 'New message body'}],
            additional_event_type='dummy_message_added',
        )

        payload = json.loads(handler.mq.publish.call_args_list[0][0][1])
        assert payload['event']['new_value'] == {
            'message_id': 'message-1',
            'content': 'New message body',
        }

    def test_publish_truncates_oversized_additional_event_content(self):
        handler = DummyHandler()
        handler.mq = Mock()

        oversized_content = 'x' * 300_000

        handler.publish_synced_record_events(
            get_latest_row=lambda: {'_pk': 20, 'modified_time': '2026-05-22T10:00:00+00:00'},
            project_uuid='project-2',
            connection_id=10,
            item_id='item-10',
            old_row={'_pk': 20},
            state_event_type='dummy_updated',
            additional_items=[{'message_id': 'message-1', 'content': oversized_content}],
            additional_event_type='dummy_message_added',
        )

        payload = json.loads(handler.mq.publish.call_args_list[0][0][1])
        content = payload['event']['new_value']['content']
        assert content.startswith(oversized_content[:100])
        assert content.endswith('[content truncated]')
        assert len(content) < 20_000

    def test_publish_truncates_oversized_state_event_content(self):
        handler = DummyHandler()
        handler.mq = Mock()

        oversized_content = 'y' * 300_000

        handler.publish_synced_record_events(
            get_latest_row=lambda: {'_pk': 21, 'content': oversized_content, 'modified_time': '2026-05-22T10:00:00+00:00'},
            project_uuid='project-2',
            connection_id=10,
            item_id='item-10',
            old_row={'_pk': 21, 'content': 'small', 'modified_time': '2026-05-22T09:00:00+00:00'},
            state_event_type='dummy_updated',
            state_event_ignored_fields=('modified_time',),
        )

        payload = json.loads(handler.mq.publish.call_args[0][1])
        assert payload['event']['new_value']['content'].endswith('[content truncated]')
        assert payload['event']['old_value'] == {'content': 'small'}

    def test_publish_skip_additional_event_when_old_row_is_none(self):
        handler = DummyHandler()
        handler.mq = Mock()

        latest_row = {
            '_pk': 22,
            'summary': 'new',
            'modified_time': '2026-05-22T10:00:00+00:00',
        }

        handler.publish_synced_record_events(
            get_latest_row=lambda: latest_row,
            project_uuid='project-2',
            connection_id=10,
            item_id='item-10',
            old_row=None,
            state_event_type='dummy_added',
            additional_items=[{'id': 2}],
            additional_event_type='dummy_comment_added',
            state_event_ignored_fields=('_pk', 'modified_time'),
        )

        handler.mq.publish.assert_called_once()
        payload = json.loads(handler.mq.publish.call_args[0][1])
        assert payload['event']['type'] == 'dummy_added'

    def test_publish_skip_additional_event_when_items_empty(self):
        handler = DummyHandler()
        handler.mq = Mock()

        latest_row = {
            '_pk': 23,
            'summary': 'new',
            'modified_time': '2026-05-22T10:00:00+00:00',
        }

        handler.publish_synced_record_events(
            get_latest_row=lambda: latest_row,
            project_uuid='project-2',
            connection_id=10,
            item_id='item-10',
            old_row={'_pk': 23, 'modified_time': '2026-05-22T09:00:00+00:00', 'summary': 'old'},
            state_event_type='dummy_updated',
            additional_items=[],
            additional_event_type='dummy_comment_added',
            state_event_ignored_fields=('_pk', 'modified_time'),
        )

        handler.mq.publish.assert_called_once()
        payload = json.loads(handler.mq.publish.call_args[0][1])
        assert payload['event']['type'] == 'dummy_updated'

    def test_publish_skip_when_no_state_change(self):
        handler = DummyHandler()
        handler.mq = Mock()

        latest_row = {
            '_pk': 21,
            'title': 'same',
            'modified_time': '2026-05-22T10:00:00+00:00',
        }

        handler.publish_synced_record_events(
            get_latest_row=lambda: latest_row,
            project_uuid='project-2',
            connection_id=10,
            item_id='item-10',
            old_row={'_pk': 21, 'modified_time': '2026-05-22T09:00:00+00:00', 'title': 'same'},
            state_event_type='dummy_updated',
            state_event_ignored_fields=('_pk', 'modified_time'),
        )

        handler.mq.publish.assert_not_called()

    def test_publish_skip_when_latest_row_missing(self):
        handler = DummyHandler()
        handler.mq = Mock()

        handler.publish_synced_record_events(
            get_latest_row=lambda: None,
            project_uuid='project-3',
            connection_id=11,
            item_id='item-11',
            old_row={'title': 'old'},
            state_event_type='dummy_updated',
        )

        handler.mq.publish.assert_not_called()

    def test_publish_skip_when_record_id_missing(self):
        handler = DummyHandler()
        handler.mq = Mock()

        handler.publish_synced_record_events(
            get_latest_row=lambda: {'modified_time': '2026-05-22T10:00:00+00:00'},
            project_uuid='project-3',
            connection_id=11,
            item_id='item-11',
            old_row={'title': 'old'},
            state_event_type='dummy_updated',
        )

        handler.mq.publish.assert_not_called()

    def test_publish_swallow_exception(self):
        handler = DummyHandler()
        handler.mq = Mock()

        handler.publish_synced_record_events(
            get_latest_row=lambda: {'_pk': 24, 'modified_time': 'not-a-time', 'title': 'new'},
            project_uuid='project-4',
            connection_id=12,
            item_id='item-12',
            old_row={'title': 'old'},
            state_event_type='dummy_updated',
        )

        handler.mq.publish.assert_not_called()
