import json
import datetime
import logging

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.seadb_models.models import SchemaTables
from seaqa_indexer.utils.constants import ConnectionType, IndexStatus, DataEventType
from seaqa_indexer.seadb_models.slack_seadb_api import SlackSeaDBAPI
from seaqa_indexer.index.index_store.slack_index import SlackIndex
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.utils import need_update_connection_last_sync_count, time_to_utc_time
from seaqa_indexer.utils.slack_api import SlackAPI
from seaqa_indexer.config import SLACK_CLIENT_ID, SLACK_CLIENT_SECRET


logger = logging.getLogger(__name__)

# System / non-content message subtypes that carry no user content to sync.
SKIP_SUBTYPES = {
    'channel_join', 'channel_leave', 'channel_archive', 'channel_unarchive',
    'channel_topic', 'channel_purpose', 'channel_name', 'group_join',
    'group_leave', 'group_archive', 'group_unarchive', 'group_topic',
    'group_purpose', 'group_name', 'pinned_item', 'unpinned_item',
    'channel_convert_to_private', 'message_deleted', 'message_changed',
    'tabbed_canvas_updated',
}

# Incremental sync look-back window: re-index this many days of top-level
# messages so that new replies to existing threads are picked up.
SLACK_SYNC_LOOKBACK_DAYS = 28


class SlackHandler(ConnectionHandler):
    """Handler for SLACK connection type."""

    connection_type = ConnectionType.SLACK.value
    BATCH_SIZE = 50

    def gen_table_name(self, connection_id):
        return SchemaTables.SLACK_MESSAGES.table_name(connection_id=connection_id)

    def gen_replies_table_name(self, connection_id):
        return SchemaTables.SLACK_MESSAGE_REPLIES.table_name(connection_id=connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        slack_index = SlackIndex(seasearch_api)
        return slack_index.get_search_param(index_name, keyword, start, size, time_from, time_to)

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        slack_index = SlackIndex(seasearch_api)
        slack_index.delete_index_by_index_name(index_name)

        # summary vector index
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

    def format_vector_search_result(self, record, connection, result):
        connection_config = json.loads(connection.config)
        team_domain = connection_config.get('team_domain', '')
        channel_id = connection_config.get('channel_id', '')
        message_id = record.get('message_id', '')
        message_url = f'https://{team_domain}.slack.com/archives/{channel_id}/p{message_id.replace(".", "")}'

        return {
            'title': record.get('title', ''),
            'message_id': message_id,
            'url': message_url,
        }

    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = self.gen_table_name(connection_id)
        slack_seadb_api = SlackSeaDBAPI(project_uuid)

        ai_processing_count = 0
        while True:
            rows = slack_seadb_api.get_need_ai_messages(connection_id, page_size)
            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                message_id = row.get('message_id', '')
                title = row.get('title', '')
                body = row.get('content', '') or ''
                replies_content = slack_seadb_api.get_replies_for_index(connection_id, message_id) or ''
                full_content = f"Title: {title}\n\nContent: {body}\n{replies_content}"
                summary = self.seaqa_ai_api.generate_summary(full_content, ConnectionType.SLACK.value, project_uuid)
                vector = self.seaqa_ai_api.generate_embedding(summary, project_uuid)
                updates.append({
                    "pk": pk,
                    "row": {
                        "ai_summary": summary,
                        "ai_summary_vector": vector,
                        "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                    }
                })
                ai_processing_count += 1
                if len(updates) >= self.BATCH_SIZE:
                    seadb_api.update_rows(project_uuid, table_name, updates)
                    updates = []
            if updates:
                seadb_api.update_rows(project_uuid, table_name, updates)
        return ai_processing_count

    def update_index(self, project_connection, seasearch_api, seaqa_db, since):
        connection_id = project_connection.id
        connection_type = project_connection.type
        project_uuid = project_connection.project_uuid
        connection_status = json.loads(project_connection.status)

        slack_seadb_api = SlackSeaDBAPI(project_uuid)
        slack_index = SlackIndex(seasearch_api)

        connection_status['last_index_status'] = IndexStatus.INDEXING
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
        index_name = gen_index_name_by_type(connection_id, connection_type)
        slack_index.create_index_if_missing(index_name)

        index_count = slack_index.update(index_name, connection_id, connection_status, seaqa_db, slack_seadb_api, since)
        indexed_at = datetime.datetime.now(datetime.UTC)
        connection_status['last_indexed_count'] = index_count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))

    def sync(self, connection_id, project_uuid, config, old_last_sync_time,
             connection_status, seaqa_db, sync_logger):
        team_id = config.get('team_id', '')
        channel_id = config.get('channel_id', '')

        if not channel_id:
            logger.error('Slack channel_id is missing for connection %s', connection_id)
            sync_logger.log('Slack channel_id is missing')
            return None, None, None, None

        oauth = seaqa_db.get_project_connection_oauth(project_uuid, ConnectionType.SLACK.value, connection_id)
        if not oauth:
            logger.error('Slack OAuth not found for connection %s', connection_id)
            sync_logger.log('Slack OAuth not found')
            return None, None, None, None

        access_token = oauth.access_token
        refresh_token = oauth.refresh_token
        expires_at = oauth.expires_at

        # Refresh proactively if token rotation is enabled and the token has expired.
        if refresh_token and expires_at and time_to_utc_time(str(expires_at)) <= datetime.datetime.now(datetime.UTC):
            refresh_api = SlackAPI(access_token, client_id=SLACK_CLIENT_ID, client_secret=SLACK_CLIENT_SECRET)
            new_tokens = refresh_api.refresh_access_token(refresh_token)
            access_token = new_tokens['access_token']
            refresh_token = new_tokens['refresh_token']
            expires_at = new_tokens['expires_at']
            seaqa_db.update_project_connection_oauth(
                project_uuid, ConnectionType.SLACK.value, access_token, expires_at, refresh_token, connection_id
            )

        slack_api = SlackAPI(access_token, client_id=SLACK_CLIENT_ID, client_secret=SLACK_CLIENT_SECRET)
        slack_seadb_api = SlackSeaDBAPI(project_uuid)

        sync_count, total_count = self._sync_slack(
            connection_id, channel_id, team_id, old_last_sync_time,
            connection_status, slack_api, slack_seadb_api, seaqa_db
        )
        return sync_count, total_count, False, config

    def _resolve_user(self, slack_api, user_id, username, user_cache):
        if not user_id:
            return username or ''
        if user_id in user_cache:
            return user_cache[user_id]

        name = user_id
        try:
            data = slack_api.fetch_user_info(user_id)
            user = data.get('user', {}) or {}
            profile = user.get('profile', {}) or {}
            name = profile.get('display_name') or user.get('real_name') or user.get('name') or user_id
        except Exception as e:
            logger.warning('Failed to resolve Slack user %s: %s', user_id, e)
        user_cache[user_id] = name
        return name

    @staticmethod
    def _fetch_all_replies(slack_api, channel_id, thread_ts, oldest=None):
        all_replies = []
        cursor = None
        while True:
            data = slack_api.fetch_replies(channel_id, thread_ts, oldest=oldest, cursor=cursor, limit=200)
            all_replies.extend(data.get('messages', []))
            has_more = bool(data.get('has_more'))
            next_cursor = (data.get('response_metadata') or {}).get('next_cursor')
            if not has_more or not next_cursor:
                break
            cursor = next_cursor
        return all_replies

    def _sync_slack(self, connection_id, channel_id, team_id, old_last_sync_time,
                    connection_status, slack_api, slack_seadb_api, seaqa_db):
        sync_count = 0
        added_count = 0
        updated_count = 0
        _status_last_update_time = datetime.datetime.now()
        _status_last_update_count = 0
        user_cache = {}

        oldest_ts = None
        is_first_sync = not old_last_sync_time
        if old_last_sync_time:
            # Incremental sync: look back a fixed window so that new replies to
            # existing (older) thread parents are re-fetched.
            lookback_start = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=SLACK_SYNC_LOOKBACK_DAYS)
            oldest_ts = str(lookback_start.timestamp())

        cursor = None
        while True:
            data = slack_api.fetch_history(channel_id, oldest=oldest_ts, cursor=cursor, limit=200)
            messages = data.get('messages', [])
            has_more = bool(data.get('has_more'))
            next_cursor = (data.get('response_metadata') or {}).get('next_cursor')

            if not messages:
                break

            for msg in messages:
                if msg.get('subtype') in SKIP_SUBTYPES:
                    continue
                # Defensive: only top-level messages (thread replies are fetched separately).
                thread_ts = msg.get('thread_ts')
                if thread_ts and thread_ts != msg.get('ts'):
                    continue

                ts = msg.get('ts')
                if not ts:
                    continue

                author = self._resolve_user(slack_api, msg.get('user'), msg.get('username'), user_cache)

                existing_message = slack_seadb_api.get_message_by_ts(connection_id, ts)
                is_new_record = not existing_message

                reply_changed = False
                message_unchanged = False
                if existing_message:
                    edited_ts = (msg.get('edited') or {}).get('ts')
                    new_modified = SlackSeaDBAPI.ts_to_datetime(edited_ts or ts)
                    existing_modified = existing_message.get('modified_time')
                    message_unchanged = bool(
                        existing_modified and new_modified
                        and new_modified <= time_to_utc_time(str(existing_modified))
                    )
                    reply_changed = (msg.get('latest_reply') or '') != (existing_message.get('latest_reply') or '')

                has_replies = int(msg.get('reply_count', 0) or 0) > 0
                if message_unchanged and not reply_changed and not (is_first_sync and has_replies):
                    continue

                if is_new_record or not message_unchanged or reply_changed:
                    slack_seadb_api.save_or_update_message(connection_id, channel_id, msg, author, existing_message)
                    if is_new_record:
                        added_count += 1
                    else:
                        updated_count += 1

                added_replies = []
                reply_count = int(msg.get('reply_count', 0) or 0)
                if reply_count > 0 and (is_new_record or reply_changed or is_first_sync):
                    replies = self._fetch_all_replies(
                        slack_api, channel_id, ts,
                        oldest=(None if is_first_sync else (existing_message.get('latest_reply') or None))
                    )
                    resolved_replies = []
                    for reply in replies:
                        reply_id = reply.get('ts')
                        if not reply_id or reply_id == ts:
                            continue
                        enriched = dict(reply)
                        enriched['author'] = self._resolve_user(
                            slack_api, reply.get('user'), reply.get('username'), user_cache
                        )
                        resolved_replies.append(enriched)
                    result = slack_seadb_api.add_or_update_replies(connection_id, ts, resolved_replies)
                    added_replies = result.get('added_replies', [])
                    if result.get('updated'):
                        slack_seadb_api.update_message_sync_and_record_time(connection_id, ts)

                event_type = (
                    DataEventType.SLACK_MESSAGE_UPDATED.value
                    if not is_new_record
                    else DataEventType.SLACK_MESSAGE_ADDED.value
                )
                sync_count += 1

                self.publish_synced_record_events(
                    get_latest_row=lambda t=ts: slack_seadb_api.get_message_by_ts(connection_id, t),
                    project_uuid=slack_seadb_api.base_id,
                    connection_id=connection_id,
                    item_id=ts,
                    old_row=existing_message,
                    state_event_type=event_type,
                    additional_items=added_replies,
                    additional_event_type=DataEventType.SLACK_MESSAGE_REPLY_ADDED.value,
                    state_event_ignored_fields=(
                        'modified_time', 'record_modified_time', 'sync_time'
                    ),
                    log_object_name='slack message',
                )

                should, new_time, new_count = need_update_connection_last_sync_count(
                    sync_count, _status_last_update_time, _status_last_update_count
                )
                if should:
                    connection_status['last_sync_count'] = sync_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                    _status_last_update_time = new_time
                    _status_last_update_count = new_count

            if not has_more or not next_cursor:
                break
            cursor = next_cursor

        connection_status['last_sync_changes'] = {
            'added': added_count,
            'deleted': 0,
            'updated': updated_count,
        }
        total_count = slack_seadb_api.get_message_count(connection_id)
        return sync_count, total_count

    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, cutoff_time):
        """Clean deleted slack message records and their replies."""
        page_size = 1000
        table_name = self.gen_table_name(connection_id)
        replies_table_name = self.gen_replies_table_name(connection_id)
        total_deleted = 0

        while True:
            sql = f"""
                SELECT _pk, message_id FROM `{table_name}`
                WHERE deleted = true AND record_modified_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            deleted_pks = [row.get('_pk') for row in rows]
            deleted_message_ids = [row.get('message_id') for row in rows]

            seadb_api.delete_rows(project_uuid, table_name, deleted_pks)
            for message_id in deleted_message_ids:
                delete_sql = f"DELETE FROM `{replies_table_name}` WHERE message_id = '{message_id}'"
                seadb_api.query_rows(project_uuid, delete_sql)

            total_deleted += len(deleted_pks)

            if len(rows) < page_size:
                break

        return total_deleted
