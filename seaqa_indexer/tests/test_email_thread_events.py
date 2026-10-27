import json
from unittest.mock import Mock

from seaqa_indexer.handlers.email_handler import EmailHandler

THREAD_ID = 7
THREAD_ROW = {
    '_pk': THREAD_ID,
    'title': 'Export fails',
    'modified_time': '2026-10-27T02:00:00+00:00',
    'unread': True,
    'linked_ticket': None,
    'deleted': False,
    'tags': None,
}


def _email(message_id, email_date, *, is_sender=False, email_from='alice@example.com',
           reply_to_message_id=None, is_new=True):
    return {
        'message_id': message_id,
        'reply_to_message_id': reply_to_message_id,
        'email_date': email_date,
        'subject': 'Export fails',
        'is_sender': is_sender,
        'email_from': email_from,
        'content': f'body of {message_id}',
        '_is_new_email': is_new,
    }


def _publish(email_list, old_thread_rows_by_id=None):
    handler = EmailHandler.__new__(EmailHandler)
    handler.mq = Mock()
    email_seadb_api = Mock(base_id='project-1')
    email_seadb_api.get_email_by_message_id.return_value = {'thread_id': THREAD_ID}
    email_seadb_api.get_thread_by_pk.return_value = THREAD_ROW

    handler._publish_thread_events(email_seadb_api, 3, email_list, old_thread_rows_by_id or {})
    return [json.loads(call.args[1]) for call in handler.mq.publish.call_args_list]


class TestEmailThreadAddedEvent:
    def test_carries_opening_email(self):
        events = _publish([_email('<m1>', '2026-10-27T01:00:00+00:00')])

        assert len(events) == 1
        event = events[0]['event']
        assert event['type'] == 'email_thread_added'
        assert event['new_value'] == {
            '_pk': THREAD_ID,
            'title': 'Export fails',
            'deleted': False,
            'message_id': '<m1>',
            'email_date': '2026-10-27T01:00:00+00:00',
            'is_sender': False,
            'email_from': 'alice@example.com',
            'content': 'body of <m1>',
        }

    def test_opening_email_is_earliest_in_batch(self):
        events = _publish([
            _email('<m2>', '2026-10-27T02:00:00+00:00', email_from='bob@example.com', reply_to_message_id='<m1>'),
            _email('<m1>', '2026-10-27T01:00:00+00:00'),
        ])

        new_value = events[-1]['event']['new_value']
        assert new_value['message_id'] == '<m1>'
        assert new_value['email_from'] == 'alice@example.com'
        assert new_value['content'] == 'body of <m1>'

    def test_opening_email_compares_dates_across_timezones(self):
        events = _publish([
            _email('<m2>', '2026-10-27T09:30:00+08:00', reply_to_message_id='<m1>'),
            _email('<m1>', '2026-10-27T02:00:00+00:00'),
        ])

        assert events[-1]['event']['new_value']['message_id'] == '<m2>'

    def test_opening_email_can_be_outbound(self):
        events = _publish([
            _email('<m1>', '2026-10-27T01:00:00+00:00', is_sender=True, email_from='support@example.com'),
            _email('<m2>', '2026-10-27T02:00:00+00:00', reply_to_message_id='<m1>'),
        ])

        assert len(events) == 1
        new_value = events[0]['event']['new_value']
        assert new_value['message_id'] == '<m1>'
        assert new_value['is_sender'] is True

    def test_skip_thread_with_only_new_outbound_email(self):
        events = _publish([_email('<m1>', '2026-10-27T01:00:00+00:00', is_sender=True)])

        assert events == []


class TestEmailMessageAddedEvent:
    def test_state_event_keeps_latest_inbound_sender(self):
        old_thread = {**THREAD_ROW, 'title': 'Old title'}
        events = _publish(
            [
                _email('<m3>', '2026-10-27T03:00:00+00:00', email_from='carol@example.com', reply_to_message_id='<m1>'),
                _email('<m2>', '2026-10-27T02:00:00+00:00', email_from='bob@example.com', reply_to_message_id='<m1>'),
            ],
            old_thread_rows_by_id={THREAD_ID: old_thread},
        )

        message_events = [event['event'] for event in events]
        assert [event['type'] for event in message_events] == ['email_message_added'] * 3
        assert [event['new_value']['message_id'] for event in message_events[:2]] == ['<m3>', '<m2>']
        assert message_events[2]['new_value'] == {'title': 'Export fails', 'email_from': 'carol@example.com'}
