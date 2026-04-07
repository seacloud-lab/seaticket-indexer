import sys
import time
import json
import logging
import threading
import signal
import datetime

from sqlalchemy.sql import text

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.utils import decrypt_config
from seaqa_indexer.utils.notion_api import NotionAPI
from seaqa_indexer.seadb_models.notion_seadb_api import NotionSeaDBAPI
from seaqa_indexer.seadb_models.models import NotionTable


logger = logging.getLogger(__name__)


class NotionPageCleaner(object):
    """Compare remote Notion pages with local records and mark missing pages as deleted."""

    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seadb_api = SeaDBAPI()

        self.should_stop = threading.Event()
        self.clear_interval = 30 * 24 * 60 * 60
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

    def get_notion_connections(self):
        with self.db_session_class() as session:
            sql = """
                SELECT pc.id, pc.config, pc.project_uuid
                FROM project_connection pc
                INNER JOIN projects p ON p.uuid = pc.project_uuid
                WHERE p.deleted = false
                AND pc.deleted = false
                AND pc.is_active = true
                AND pc.type = :connection_type
            """
            return session.execute(text(sql), {'connection_type': ConnectionType.NOTION.value}).fetchall()

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

    def mark_deleted_local_pages(self, notion_seadb_api, connection_id, project_uuid, remote_page_ids):
        table_name = NotionTable.gen_table_name(connection_id)
        now_datetime = datetime.datetime.now(datetime.UTC).isoformat()
        offset = 0
        deleted_count = 0
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

        stale_page_ids = set(local_page_map.keys()) - set(remote_page_ids)

        if not stale_page_ids:
            return 0

        updates = []
        for page_id in stale_page_ids:
            pk = local_page_map.get(page_id)
            updates.append({
                'pk': pk,
                'row': {
                    'deleted': True,
                    'sync_time': now_datetime,
                    'record_modified_time': now_datetime,
                }
            })
            if len(updates) >= self.page_size:
                self.seadb_api.update_rows(project_uuid, table_name, updates)
                deleted_count += len(updates)
                updates = []

        if updates:
            self.seadb_api.update_rows(project_uuid, table_name, updates)
            deleted_count += len(updates)

        return deleted_count

    def clean_notion_pages(self):
        connections = self.get_notion_connections()
        logger.info('Found %d notion connections to check', len(connections))

        for connection in connections:
            connection_id = connection.id
            project_uuid = connection.project_uuid

            try:
                connection_config =  decrypt_config(json.loads(connection.config))
                token = connection_config.get('integration_secret')
                if not token:
                    logger.warning('Notion token missing for connection %s, skipping', connection_id)
                    continue

                notion_api = NotionAPI(token)
                notion_seadb_api = NotionSeaDBAPI(project_uuid)

                remote_page_ids = self.fetch_remote_page_ids(notion_api)
                deleted_count = self.mark_deleted_local_pages(
                    notion_seadb_api,
                    connection_id,
                    project_uuid,
                    remote_page_ids,
                )
                logger.info('Marked %d local notion pages deleted for connection %s', deleted_count, connection_id)
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
