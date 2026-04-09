import sys
import time
import json
import logging
import threading
import signal
import datetime

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils import decrypt_config
from seaqa_indexer.utils.notion_api import NotionAPI
from seaqa_indexer.seadb_models.notion_seadb_api import NotionSeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB

logger = logging.getLogger(__name__)


class NotionPageCleaner(object):
    """Compare remote Notion pages with local records and mark missing pages as deleted."""

    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seadb_api = SeaDBAPI()
        self.seaqa_db = SeaqaDB(self.db_session_class)

        self.should_stop = threading.Event()
        self.clear_interval = 24 * 60 * 60
        self.recent_days = 30 # Only check connections that are more than 30 days old since the last cleanup.
        self.page_size = 100
        self.set_signal()

    @property
    def tname(self):
        return threading.current_thread().name

    def start(self):
        threading.Thread(
            target=self.cron_clean_notion_pages,
            name='cron_clean_notion_pages_thread',
            daemon=True
        ).start()

    def cron_clean_notion_pages(self):
        logger.info('Start notion page cleaner timer')
        while not self.should_stop.is_set():
            self.should_stop.wait(self.clear_interval)

            if not self.should_stop.is_set():
                logger.info('Start to clean notion pages')
                try:
                    self.clean_notion_pages()
                except Exception as e:
                    logger.exception('Cron clean notion pages error: %s', e)

    def fetch_remote_page_ids(self, notion_api):
        page_ids = set()
        cursor = None

        while True:
            pages, next_cursor, has_more = notion_api.search_pages(
                per_page=self.page_size,
                start_cursor=cursor,
            )
            if not pages:
                break

            for page in pages:
                page_id = page.get('id')
                if page_id:
                    page_ids.add(page_id)

            if not has_more:
                break
            cursor = next_cursor
            if not cursor:
                break
            time.sleep(0.34)

        return page_ids

    def get_local_page_ids(self, notion_seadb_api, connection_id):
        offset = 0
        local_page_map = {}

        while True:
            rows = notion_seadb_api.get_page_id_rows_by_page(connection_id, self.page_size, offset)
            if not rows:
                break
            for row in rows:
                page_id = row.get('page_id')
                pk = row.get('_pk')
                if page_id and pk:
                    local_page_map[page_id] = pk
            if len(rows) < self.page_size:
                break
            offset += self.page_size
        return local_page_map

    def clean_notion_pages(self):
        connections = self.seaqa_db.get_notion_connections_to_clean(self.recent_days)
        logger.info('Found %d notion connections to check', len(connections))

        for connection in connections:
            connection_id = connection.id
            project_uuid = connection.project_uuid

            try:
                connection_config = decrypt_config(json.loads(connection.config))
                token = connection_config.get('integration_secret')
                if not token:
                    logger.warning('Notion token missing for connection %s, skipping', connection_id)
                    continue

                notion_api = NotionAPI(token)
                notion_seadb_api = NotionSeaDBAPI(project_uuid)

                remote_page_ids = self.fetch_remote_page_ids(notion_api)
                local_page_map = self.get_local_page_ids(notion_seadb_api, connection_id)

                stale_page_ids = set(local_page_map.keys()) - set(remote_page_ids)

                notion_seadb_api.mark_pages_deleted(connection_id, stale_page_ids, local_page_map)

                cleaned_at = datetime.datetime.now(datetime.UTC)
                self.seaqa_db.update_connection_last_cleaned_at(connection_id, cleaned_at)
                logger.info('Marked %d local notion pages deleted for connection %s', len(stale_page_ids), connection_id)
            except Exception as e:
                logger.exception('Failed to clean notion pages for connection %s: %s', connection_id, e)

    def clear(self):
        self.should_stop.set()
        logger.info("Exit notion page cleaner process")
        sys.exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        signal.signal(signal.SIGTERM, self.signal_term_handler)


def start():
    notion_page_cleaner = NotionPageCleaner()
    logger.info("Notion page cleaner process initialized.")
    try:
        notion_page_cleaner.start()
    except Exception as e:
        logger.exception(e)
        notion_page_cleaner.clear()
        sys.exit(1)

    while True:
        time.sleep(2)


def main():
    LogConfigurator(config.LOG_LEVEL, config.LOG_FILE)
    start()


if __name__ == "__main__":
    main()
