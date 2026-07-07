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

    def fetch_remote_message_ids(self, discord_api, channel_id, channel_type, guild_id):
        """Fetch all message IDs from Discord for a channel.

        For text channels (type 0): paginate through all messages.
        For forum channels (type 15): fetch all threads, then messages in each thread.
        """
        remote_ids = set()

        if channel_type == 15:
            # Forum channel: iterate threads, then messages within each thread
            threads = discord_api.fetch_forum_threads(guild_id, channel_id)
            for thread in threads:
                thread_id = thread.get('id')
                if not thread_id:
                    continue
                # The thread's first message is the thread object itself
                # Fetch all messages in the thread
                before = None
                while True:
                    messages = discord_api.fetch_thread_messages(channel_id, thread_id, before=before)
                    if not messages:
                        break
                    for msg in messages:
                        msg_id = msg.get('id')
                        if msg_id:
                            remote_ids.add(msg_id)
                    if len(messages) < self.page_size:
                        break
                    before = messages[-1].get('id')
                    time.sleep(0.5)
                time.sleep(0.5)
        else:
            # Text channel: paginate through all messages
            before = None
            while True:
                messages = discord_api.fetch_channel_messages(channel_id, before=before, limit=self.page_size)
                if not messages:
                    break
                for msg in messages:
                    msg_id = msg.get('id')
                    if msg_id:
                        remote_ids.add(msg_id)
                if len(messages) < self.page_size:
                    break
                before = messages[-1].get('id')
                time.sleep(0.5)

        return remote_ids

    def get_local_message_ids(self, discord_seadb_api, connection_id):
        """Fetch all local message IDs from SeaDB for a connection."""
        offset = 0
        local_ids = set()

        while True:
            rows = discord_seadb_api.get_message_ids_by_page(connection_id, self.page_size, offset)
            if not rows:
                break
            for row in rows:
                msg_id = row.get('message_id')
                if msg_id:
                    local_ids.add(msg_id)
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
                bot_token = connection_config.get('bot_token', '')

                if not channel_id:
                    logger.warning('Discord channel_id missing for connection %s', connection_id)
                    continue

                if not bot_token:
                    logger.warning('Discord bot_token missing for connection %s', connection_id)
                    continue

                # Detect channel type (forum vs text)
                discord_api = DiscordAPI(bot_token)
                try:
                    channel_info = discord_api.get_channel(channel_id)
                    channel_type = channel_info.get('type', 0)
                except Exception:
                    logger.warning('Failed to get channel info for connection %s, assuming text channel', connection_id)
                    channel_type = 0

                remote_ids = self.fetch_remote_message_ids(
                    discord_api, channel_id, channel_type, guild_id
                )

                discord_seadb_api = DiscordSeaDBAPI(project_uuid)
                local_ids = self.get_local_message_ids(discord_seadb_api, connection_id)

                need_deleted_ids = local_ids - remote_ids
                for msg_id in need_deleted_ids:
                    discord_seadb_api.mark_message_deleted(connection_id, msg_id)

                cleaned_at = datetime.datetime.now(datetime.UTC)
                self.seaqa_db.update_connection_last_cleaned_at(connection_id, cleaned_at)
                logger.info(
                    'Cleaned discord connection %s: marked %d messages deleted, %d local, %d remote',
                    connection_id, len(need_deleted_ids), len(local_ids), len(remote_ids)
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
