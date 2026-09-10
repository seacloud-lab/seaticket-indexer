import time
import logging
import datetime
from threading import Thread, Event

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.utils.notion_api import NotionAPI
from seaqa_indexer.seadb_models.notion_seadb_api import NotionSeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.constants import ConnectionType

logger = logging.getLogger(__name__)


class NotionPageCleaner(object):
    def __init__(self):
        self._enabled = True
        self._interval = 24 * 60 * 60 # Running interval
        self.check_days = 30 # Only check connections that are more than 30 days old since the last cleanup.

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start notion page cleaner: it is not enabled!')
            return

        NotionPageCleanerTimer(self._interval, self.check_days).start()
        logging.info('notion page cleaner timer is started')

    def is_enabled(self):
        return self._enabled


class NotionPageCleanerTimer(Thread):
    """Compare remote Notion pages with local records and mark missing pages as deleted."""

    def __init__(self, interval, check_days):
        Thread.__init__(self)
        self.db_session_class = init_db_session_class()
        self.seadb_api = SeaDBAPI()
        self.seaqa_db = SeaqaDB(self.db_session_class)

        self.finished = Event()
        self.interval = interval
        self.check_days = check_days
        self.page_size = 100

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
        connections = self.seaqa_db.get_need_cleaned_connections_by_type(self.check_days, ConnectionType.NOTION.value)
        logger.info('Found %d notion connections to clean', len(connections))

        for connection in connections:
            connection_id = connection.id
            project_uuid = connection.project_uuid

            try:
                oauth = self.seaqa_db.get_project_connection_oauth(project_uuid, ConnectionType.NOTION.value, connection_id)
                if not oauth:
                    logger.warning('Notion OAuth missing for connection %s, skipping', connection_id)
                    continue

                access_token = oauth.access_token
                refresh_token = oauth.refresh_token
                expires_at = oauth.expires_at

                if refresh_token and (not expires_at or time_to_utc_time(str(expires_at)) <= datetime.datetime.now(datetime.timezone.utc)):
                    try:
                        api = NotionAPI(access_token)
                        new_tokens = api.refresh_access_token(refresh_token)
                        access_token = new_tokens['access_token']
                        refresh_token = new_tokens['refresh_token']
                        expires_at = new_tokens['expires_at']
                        self.seaqa_db.update_project_connection_oauth(project_uuid, ConnectionType.NOTION.value, access_token, expires_at, refresh_token, connection_id)
                    except Exception as e:
                        logger.warning('refresh Notion access_token failed: %s', e)

                notion_api = NotionAPI(access_token)
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

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                logging.info('starts to clean notion pages task')
                try:
                    self.clean_notion_pages()
                except Exception as e:
                    logging.exception('clean notion pages task error: %s', e)

    def cancel(self):
        self.finished.set()
