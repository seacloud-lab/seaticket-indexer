import time
import json
import logging
import datetime
from threading import Thread, Event

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils import decrypt_config
from seaqa_indexer.utils.discord_api import DiscordAPI
from seaqa_indexer.seadb_models.discord_seadb_api import DiscordSeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.config import DISCORD_BOT_TOKEN

logger = logging.getLogger(__name__)


class DiscordMessageCleaner(object):
    def __init__(self):
        self._enabled = True
        self._interval = 24 * 60 * 60  # Running interval
        self.check_days = 30  # Only check connections that are more than 30 days old since the last cleanup.

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start discord message cleaner: it is not enabled!')
            return

        DiscordMessageCleanerTimer(self._interval, self.check_days).start()
        logging.info('Discord message cleaner timer is started')

    def is_enabled(self):
        return self._enabled


class DiscordMessageCleanerTimer(Thread):
    """Compare remote Discord messages with local records and mark missing messages as deleted."""

    def __init__(self, interval, check_days):
        Thread.__init__(self)
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)

        self.finished = Event()
        self.interval = interval
        self.check_days = check_days
        self.page_size = 100

    def fetch_remote_thread_message_ids(self, discord_api, guild_id, channel_id):
        """Fetch all threads (active + archived) and their message IDs from a forum channel.

        Returns:
            dict[str, set]: ``{thread_id: set(message_ids_in_thread)}``.
        """
        result = {}
        active_threads = discord_api.fetch_forum_threads(guild_id, channel_id)
        archived_threads = []

        # Archived threads are paginated, fetch them all
        before = None
        while True:
            archived, has_more = discord_api.fetch_archived_forum_threads(channel_id, before=before)
            if not archived:
                break
            archived_threads.extend(archived)
            if not has_more:
                break
            before = archived[-1].get('thread_metadata', {}).get('archive_timestamp')
            if not before:
                break

        for thread in active_threads + archived_threads:
            thread_id = thread.get('id')
            if not thread_id:
                continue
            msg_ids = set()
            before = None
            while True:
                messages = discord_api.fetch_thread_messages(
                    channel_id, thread_id, before=before, limit=self.page_size
                )
                if not messages:
                    break
                for msg in messages:
                    msg_id = msg.get('id')
                    if msg_id:
                        msg_ids.add(msg_id)
                if len(messages) < self.page_size:
                    break
                before = messages[-1].get('id')
                time.sleep(0.5)
            result[thread_id] = msg_ids
            time.sleep(0.5)
        return result

    def get_local_thread_ids(self, discord_seadb_api, connection_id):
        """Fetch all local thread IDs from SeaDB for a connection."""
        offset = 0
        local_ids = set()

        while True:
            rows = discord_seadb_api.get_thread_ids_by_page(connection_id, self.page_size, offset)
            if not rows:
                break
            for row in rows:
                tid = row.get('thread_id')
                if tid:
                    local_ids.add(tid)
            if len(rows) < self.page_size:
                break
            offset += self.page_size

        return local_ids

    def clean_discord_messages(self):
        connections = self.seaqa_db.get_need_cleaned_connections_by_type(self.check_days, ConnectionType.DISCORD.value)
        logger.info('Found %d discord connections to clean', len(connections))

        for connection in connections:
            connection_id = connection.id
            project_uuid = connection.project_uuid

            try:
                connection_config = decrypt_config(json.loads(connection.config)) if connection.config else {}
                channel_id = connection_config.get('channel_id')
                guild_id = connection_config.get('guild_id', '')

                if not channel_id:
                    logger.warning('Discord channel_id missing for connection %s', connection_id)
                    continue

                discord_api = DiscordAPI(DISCORD_BOT_TOKEN)

                # Fetch all threads + their message IDs from the forum channel.
                # thread_message_ids maps thread_id → set of all message IDs in that thread
                # (including the thread ID itself as the message record).
                thread_message_ids = self.fetch_remote_thread_message_ids(
                    discord_api, guild_id, channel_id
                )

                discord_seadb_api = DiscordSeaDBAPI(project_uuid)

                # ---- 1. Thread-level cleanup ----
                # Mark message records as deleted if the thread no longer exists remotely.
                all_remote_ids = set(thread_message_ids.keys())
                local_ids = self.get_local_thread_ids(discord_seadb_api, connection_id)

                need_deleted_ids = local_ids - all_remote_ids
                for msg_id in need_deleted_ids:
                    discord_seadb_api.mark_thread_deleted(connection_id, msg_id)

                logger.info(
                    'Cleaned discord connection %s threads: marked %d messages deleted',
                    connection_id, len(need_deleted_ids)
                )

                # ---- 2. Message-level cleanup ----
                # For threads that still exist, compare local messages against
                # remote message IDs and delete stale ones.
                total_stale_messages = 0
                for thread_id, remote_msg_ids in thread_message_ids.items():
                    # Collect local message IDs for this thread
                    local_message_ids = set()
                    msg_offset = 0
                    msg_page_size = 1000
                    while True:
                        existing = discord_seadb_api.get_thread_messages_by_page(
                            connection_id, thread_id, msg_page_size, msg_offset
                        )
                        for msg in existing:
                            mid = msg.get('message_id')
                            if mid:
                                local_message_ids.add(mid)
                        if not existing or len(existing) < msg_page_size:
                            break
                        msg_offset += msg_page_size

                    stale_message_ids = local_message_ids - remote_msg_ids
                    for mid in stale_message_ids:
                        discord_seadb_api.delete_message_from_db(
                            connection_id, thread_id, mid
                        )
                        total_stale_messages += 1
                    if stale_message_ids:
                        discord_seadb_api.update_thread_record_modified_time(
                            connection_id, thread_id
                        )

                cleaned_at = datetime.datetime.now(datetime.UTC)
                self.seaqa_db.update_connection_last_cleaned_at(connection_id, cleaned_at)
                logger.info(
                    'Cleaned discord connection %s: %d threads deleted, %d messages deleted, '
                    '%d local threads, %d remote threads',
                    connection_id, len(need_deleted_ids), total_stale_messages,
                    len(local_ids), len(all_remote_ids)
                )
            except Exception as e:
                logger.exception('Failed to clean discord messages for connection %s: %s', connection_id, e)

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                logging.info('Starts to clean discord messages task')
                try:
                    self.clean_discord_messages()
                except Exception as e:
                    logging.exception('Clean discord messages task error: %s', e)

    def cancel(self):
        self.finished.set()
