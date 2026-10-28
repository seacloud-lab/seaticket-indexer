import datetime
import json
from unittest.mock import Mock, patch

from seaqa_indexer.handlers.discord_handler import DiscordHandler
from seaqa_indexer.handlers.discourse_handler import DiscourseHandler
from seaqa_indexer.utils import time_to_utc_time

TOPIC_ROW = {
    '_pk': 9,
    'topic_id': 42,
    'title': 'Sync stuck',
    'slug': 'sync-stuck',
    'views': 3,
    'category_id': 5,
    'modified_time': '2026-10-27T02:00:00+00:00',
    'resolved': False,
    'deleted': False,
    'outdated': False,
    'record_modified_time': '2026-10-27T02:00:00+00:00',
    'sync_time': '2026-10-27T02:00:00+00:00',
    'created_time': '2026-10-27T01:00:00+00:00',
    'ai_processed_time': None,
}

THREAD_ROW = {
    '_pk': 4,
    'thread_id': '200',
    'title': 'Export fails',
    'created_time': '2026-10-27T01:00:00+00:00',
    'modified_time': '2026-10-27T02:00:00+00:00',
    'deleted': False,
    'outdated': False,
    'record_modified_time': '2026-10-27T02:00:00+00:00',
    'sync_time': '2026-10-27T02:00:00+00:00',
    'ai_processed_time': None,
}


def _events(handler):
    return [json.loads(call.args[1]) for call in handler.mq.publish.call_args_list]


def _sync_topic(existing_topic, added_replies, latest_row):
    handler = DiscourseHandler.__new__(DiscourseHandler)
    handler.mq = Mock()
    forum = Mock()
    forum.fetch_latest_topics_by_page.side_effect = [
        [{'id': 42, 'bumped_at': '2026-10-27T02:00:00+00:00', 'pinned': False}],
        [],
    ]
    forum.fetch_topic_replies.return_value = []
    seadb = Mock(base_id='project-1')
    seadb.get_topic_by_topic_id.side_effect = [existing_topic, latest_row]
    seadb.update_topic_replies.return_value = added_replies
    seadb.get_topic_count.return_value = 1

    with patch('seaqa_indexer.handlers.discourse_handler.time.sleep'):
        handler._sync_discourse(
            3,
            datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
            None,
            {},
            forum,
            seadb,
            Mock(),
        )
    return _events(handler)


def _discord_handler(raw_messages, *, last_message_id, existing_thread, latest_row):
    """Sync one thread, keeping only messages newer than the fetch cutoff."""
    handler = DiscordHandler.__new__(DiscordHandler)
    handler.mq = Mock()
    handler._fetch_all_archived_threads = Mock(return_value=[])

    def fetch_messages(discord_api, thread_id, after=None):
        if after is None:
            return list(raw_messages)
        return [message for message in raw_messages if int(message['id']) > int(after)]

    handler._fetch_all_messages = Mock(side_effect=fetch_messages)
    handler.discord_api = Mock()
    handler.discord_api.fetch_forum_threads.return_value = [{
        'id': '200',
        'last_message_id': last_message_id,
    }]
    seadb = Mock(base_id='project-1')
    seadb.get_thread_by_thread_id.side_effect = [existing_thread, latest_row]

    def add_or_update(connection_id, thread_id, thread_messages):
        return {'added_messages': [
            {
                'message_id': message['id'],
                'author': message['author'],
                'content': message['content'],
                'modified_time': message['modified_time'],
            }
            for message in thread_messages
        ]}

    seadb.add_or_update_messages.side_effect = add_or_update
    seadb.get_thread_count.return_value = 1
    handler.seadb = seadb
    return handler, handler._fetch_all_messages


def _sync_thread(existing_thread, added_messages, latest_row):
    handler = DiscordHandler.__new__(DiscordHandler)
    handler.mq = Mock()
    handler._fetch_all_archived_threads = Mock(return_value=[])
    handler._fetch_all_messages = Mock(return_value=[{'id': '1'}])
    discord_api = Mock()
    discord_api.fetch_forum_threads.return_value = [{'id': '200', 'last_message_id': '300'}]
    seadb = Mock(base_id='project-1')
    seadb.get_thread_by_thread_id.side_effect = [existing_thread, latest_row]
    seadb.add_or_update_messages.return_value = {'added_messages': added_messages}
    seadb.get_thread_count.return_value = 1

    handler._sync_discord(3, 'channel', 'guild', None, {}, discord_api, seadb, Mock())
    return _events(handler)


class TestDiscourseTopicAddedEvent:
    def test_new_topic_carries_earliest_opening_post(self):
        events = _sync_topic({}, [
            {
                'post_number': 2, 'author': 'bob', 'content': 'later reply',
                'modified_time': '2026-10-27T02:00:00+00:00',
            },
            {
                'post_number': 1, 'author': 'alice', 'content': 'opening post',
                'modified_time': '2026-10-27T01:00:00+00:00',
            },
        ], TOPIC_ROW)

        assert len(events) == 1
        event = events[0]['event']
        assert event['type'] == 'discourse_topic_added'
        assert event['new_value']['post_number'] == 1
        assert event['new_value']['author'] == 'alice'
        assert event['new_value']['content'] == 'opening post'
        assert event['new_value']['title'] == 'Sync stuck'

    def test_updated_topic_does_not_carry_opening_post(self):
        existing = {**TOPIC_ROW, 'title': 'Old title', 'modified_time': '2026-10-27T01:00:00+00:00'}
        events = _sync_topic(existing, [
            {
                'post_number': 3, 'author': 'carol', 'content': 'new reply',
                'modified_time': '2026-10-27T03:00:00+00:00',
            },
        ], TOPIC_ROW)

        assert [event['event']['type'] for event in events] == [
            'discourse_topic_comment_added',
            'discourse_topic_updated',
        ]
        assert events[0]['event']['new_value'] == {
            'post_number': 3,
            'author': 'carol',
            'content': 'new reply',
            'modified_time': '2026-10-27T03:00:00+00:00',
        }
        assert 'post_number' not in events[1]['event']['new_value']
        assert 'content' not in events[1]['event']['new_value']
        assert events[1]['event']['new_value']['title'] == 'Sync stuck'


class TestDiscordThreadAddedEvent:
    def test_new_thread_carries_earliest_opening_message(self):
        events = _sync_thread(None, [
            {
                'message_id': '300', 'author': 'bob', 'content': 'later message',
                'modified_time': '2026-10-27T02:00:00+00:00',
            },
            {
                'message_id': '100', 'author': 'alice', 'content': 'opening message',
                'modified_time': '2026-10-27T01:00:00+00:00',
            },
        ], THREAD_ROW)

        assert len(events) == 1
        event = events[0]['event']
        assert event['type'] == 'discord_thread_added'
        assert event['new_value']['message_id'] == '100'
        assert event['new_value']['author'] == 'alice'
        assert event['new_value']['content'] == 'opening message'
        assert event['new_value']['title'] == 'Export fails'

    def test_updated_thread_does_not_carry_opening_message(self):
        existing = {**THREAD_ROW, 'title': 'Old title'}
        events = _sync_thread(existing, [
            {
                'message_id': '400', 'author': 'carol', 'content': 'new message',
                'modified_time': '2026-10-27T03:00:00+00:00',
            },
        ], THREAD_ROW)

        assert [event['event']['type'] for event in events] == [
            'discord_thread_message_added',
            'discord_thread_updated',
        ]
        assert events[0]['event']['new_value'] == {
            'message_id': '400',
            'author': 'carol',
            'content': 'new message',
            'modified_time': '2026-10-27T03:00:00+00:00',
        }
        assert 'message_id' not in events[1]['event']['new_value']
        assert 'content' not in events[1]['event']['new_value']
        assert events[1]['event']['new_value']['title'] == 'Export fails'

    def test_newly_seen_old_thread_uses_full_history_opening_message(self):
        last_sync_time = datetime.datetime(2026, 10, 27, 2, 0, tzinfo=datetime.timezone.utc)
        opening_id = DiscordHandler._datetime_to_snowflake(
            datetime.datetime(2026, 10, 27, 0, 0, tzinfo=datetime.timezone.utc)
        )
        reply_id = DiscordHandler._datetime_to_snowflake(
            datetime.datetime(2026, 10, 27, 3, 0, tzinfo=datetime.timezone.utc)
        )
        raw_messages = [
            {
                'id': reply_id, 'author': 'bob', 'content': 'later reply',
                'modified_time': '2026-10-27T03:00:00+00:00',
            },
            {
                'id': opening_id, 'author': 'alice', 'content': 'opening post',
                'modified_time': '2026-10-27T00:00:00+00:00',
            },
        ]
        handler, fetch_messages = _discord_handler(
            raw_messages, last_message_id=reply_id, existing_thread=None, latest_row=THREAD_ROW,
        )

        handler._sync_discord(
            3, 'channel', 'guild', last_sync_time, {}, handler.discord_api, handler.seadb, Mock(),
        )

        fetch_messages.assert_called_once()
        assert fetch_messages.call_args.kwargs['after'] is None
        events = _events(handler)
        assert len(events) == 1
        assert events[0]['event']['type'] == 'discord_thread_added'
        assert events[0]['event']['new_value']['message_id'] == opening_id
        assert events[0]['event']['new_value']['content'] == 'opening post'

    def test_existing_thread_keeps_incremental_message_fetch(self):
        last_sync_time = datetime.datetime(2026, 10, 27, 2, 0, tzinfo=datetime.timezone.utc)
        opening_id = DiscordHandler._datetime_to_snowflake(
            datetime.datetime(2026, 10, 27, 0, 0, tzinfo=datetime.timezone.utc)
        )
        reply_id = DiscordHandler._datetime_to_snowflake(
            datetime.datetime(2026, 10, 27, 3, 0, tzinfo=datetime.timezone.utc)
        )
        raw_messages = [
            {
                'id': reply_id, 'author': 'bob', 'content': 'later reply',
                'modified_time': '2026-10-27T03:00:00+00:00',
            },
            {
                'id': opening_id, 'author': 'alice', 'content': 'opening post',
                'modified_time': '2026-10-27T00:00:00+00:00',
            },
        ]
        existing = {**THREAD_ROW, 'title': 'Old title'}
        handler, fetch_messages = _discord_handler(
            raw_messages, last_message_id=reply_id, existing_thread=existing, latest_row=THREAD_ROW,
        )

        handler._sync_discord(
            3, 'channel', 'guild', last_sync_time, {}, handler.discord_api, handler.seadb, Mock(),
        )

        expected_after = DiscordHandler._datetime_to_snowflake(time_to_utc_time(str(last_sync_time)))
        assert fetch_messages.call_args.kwargs['after'] == expected_after
        events = _events(handler)
        assert [event['event']['type'] for event in events] == [
            'discord_thread_message_added',
            'discord_thread_updated',
        ]
        assert events[0]['event']['new_value']['message_id'] == reply_id
        assert 'message_id' not in events[1]['event']['new_value']

    def test_opening_message_skips_non_numeric_ids(self):
        extra = DiscordHandler._build_opening_message_extra([
            {'message_id': 'not-a-snowflake', 'content': 'ignored'},
            {'message_id': '20', 'author': 'alice', 'content': 'opening'},
        ])

        assert extra['message_id'] == '20'
        assert extra['content'] == 'opening'
