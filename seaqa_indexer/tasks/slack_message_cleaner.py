import json
import logging
import datetime
from threading import Thread, Event

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils import decrypt_config
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.utils.slack_api import SlackAPI
from seaqa_indexer.seadb_models.slack_seadb_api import SlackSeaDBAPI
from seaqa_indexer.handlers.slack_handler import SKIP_SUBTYPES

logger = logging.getLogger(__name__)


class SlackMessageCleaner(object):
    """Periodically compare remote Slack messages with local records.

    Marks local messages that no longer exist remotely as ``deleted``, and
    triggers a full sync (by clearing ``last_sync_time``) to pick up any new
    messages outside the incremental 28-day window.
    """

    def __init__(self, sync_manager):
        self._enabled = True
        self._sync_manager = sync_manager
        self._interval = 24 * 60 * 60  # check once a day
        self.check_days = 30  # only clean connections not cleaned in the last 30 days

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start slack message cleaner: it is not enabled!')
            return
        SlackMessageCleanerTimer(self._interval, self.check_days, self._sync_manager).start()
        logging.info('slack message cleaner is started')

    def is_enabled(self):
        return self._enabled


class SlackMessageCleanerTimer(Thread):
    """Compare remote Slack top-level messages with local records and mark missing ones as deleted."""

    def __init__(self, interval, check_days, sync_manager):
        Thread.__init__(self)
        self.finished = Event()
        self.interval = interval
        self.check_days = check_days
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.sync_manager = sync_manager
        self.page_size = 200

    def fetch_remote_message_ids(self, slack_api, channel_id):
        """Fetch all remote top-level message ts values via full history pagination."""
        remote_ids = set()
        cursor = None
        while True:
            data = slack_api.fetch_history(channel_id, cursor=cursor, limit=self.page_size)
            messages = data.get('messages', [])
            if not messages:
                break
            for msg in messages:
                if msg.get('subtype') in SKIP_SUBTYPES:
                    continue
                ts = msg.get('ts')
                if ts:
                    remote_ids.add(ts)
            has_more = bool(data.get('has_more'))
            next_cursor = (data.get('response_metadata') or {}).get('next_cursor')
            if not has_more or not next_cursor:
                break
            cursor = next_cursor
        return remote_ids

    def clean_slack_messages(self):
        connections = self.seaqa_db.get_need_cleaned_connections_by_type(
            self.check_days, ConnectionType.SLACK.value
        )
        logger.info('Found %d slack connections to clean', len(connections))

        for connection in connections:
            connection_id = connection.id
            project_uuid = connection.project_uuid
            try:
                connection_config = decrypt_config(json.loads(connection.config)) if connection.config else {}
                channel_id = connection_config.get('channel_id')
                if not channel_id:
                    logger.warning('Slack channel_id missing for connection %s', connection_id)
                    continue

                oauth = self.seaqa_db.get_project_connection_oauth(project_uuid, ConnectionType.SLACK.value, connection_id)
                if not oauth:
                    logger.warning('Slack OAuth not found for connection %s', connection_id)
                    continue

                slack_api = SlackAPI(oauth.access_token)
                slack_seadb_api = SlackSeaDBAPI(project_uuid)

                remote_ids = self.fetch_remote_message_ids(slack_api, channel_id)
                local_ids = slack_seadb_api.get_all_message_ids(connection_id)
                need_deleted_ids = local_ids - remote_ids
                for mid in need_deleted_ids:
                    slack_seadb_api.mark_message_deleted(connection_id, mid)

                # Clear last_sync_time so the next sync is a full sync, picking up
                # any new messages/replies outside the 28-day incremental window.
                self.seaqa_db.reset_last_sync_time(connection_id)
                self.seaqa_db.update_connection_last_cleaned_at(
                    connection_id, datetime.datetime.now(datetime.UTC)
                )
                self.sync_manager.check_and_sync(connection_id)

                logger.info(
                    'Cleaned slack connection %s: marked %d messages deleted',
                    connection_id, len(need_deleted_ids)
                )
            except Exception as e:
                logger.exception('Failed to clean slack messages for connection %s: %s', connection_id, e)

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                logging.info('Starts to clean slack messages task')
                try:
                    self.clean_slack_messages()
                except Exception as e:
                    logging.exception('Clean slack messages task error: %s', e)

    def cancel(self):
        self.finished.set()
