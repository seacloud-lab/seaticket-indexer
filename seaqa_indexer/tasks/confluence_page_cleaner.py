import time
import json
import logging
import datetime
from threading import Thread, Event

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils import decrypt_config
from seaqa_indexer.utils.confluence_api import ConfluenceAPI
from seaqa_indexer.seadb_models.confluence_seadb_api import ConfluenceSeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.constants import ConnectionType

logger = logging.getLogger(__name__)


class ConfluencePageCleaner(object):
    def __init__(self):
        self._enabled = True
        self._interval = 24 * 60 * 60  # Running interval
        self.check_days = 30  # Only check connections that are more than 30 days old since the last cleanup.

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start confluence page cleaner: it is not enabled!')
            return

        ConfluencePageCleanerTimer(self._interval, self.check_days).start()
        logging.info('confluence page cleaner timer is started')

    def is_enabled(self):
        return self._enabled


class ConfluencePageCleanerTimer(Thread):
    """Compare remote Confluence pages with local records and mark missing pages as deleted."""

    def __init__(self, interval, check_days):
        Thread.__init__(self)
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)

        self.finished = Event()
        self.interval = interval
        self.check_days = check_days
        self.page_size = 100

    def fetch_remote_page_ids(self, confluence_api, workspace_id, space_ids=None):
        page_ids = set()
        space_contexts = space_ids if space_ids else [None]

        for space_id in space_contexts:
            cursor = None
            while True:
                pages, next_cursor, has_more = confluence_api.list_pages(
                    workspace_id,
                    cursor=cursor,
                    limit=self.page_size,
                    space_ids=[space_id] if space_id else None,
                )
                if not pages:
                    if not has_more:
                        break
                else:
                    for page in pages:
                        page_id = page.get('page_id')
                        if page_id:
                            page_ids.add(page_id)

                if not has_more or not next_cursor:
                    break
                cursor = next_cursor
                time.sleep(0.34)

        return page_ids

    def get_local_page_ids(self, confluence_seadb_api, connection_id):
        offset = 0
        local_page_map = {}

        while True:
            rows = confluence_seadb_api.get_page_id_rows_by_page(connection_id, self.page_size, offset)
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

    def clean_confluence_pages(self):
        connections = self.seaqa_db.get_need_cleaned_connections_by_type(self.check_days, ConnectionType.CONFLUENCE.value)
        logger.info('Found %d confluence connections to clean', len(connections))

        for connection in connections:
            connection_id = connection.id
            project_uuid = connection.project_uuid

            try:
                connection_config = decrypt_config(json.loads(connection.config)) if connection.config else {}
                workspace_id = connection_config.get('workspace_id')
                if not workspace_id:
                    logger.warning('Confluence workspace_id missing for connection %s, skipping', connection_id)
                    continue

                space_ids = connection_config.get('space_ids') or []

                oauth = self.seaqa_db.get_project_confluence_oauth(project_uuid)
                if not oauth:
                    logger.warning('Confluence OAuth missing for project %s', project_uuid)
                    continue

                access_token = oauth.access_token
                refresh_token = oauth.refresh_token
                if not access_token:
                    logger.warning('Confluence access_token missing for project %s', project_uuid)
                    continue

                confluence_api = ConfluenceAPI(
                    access_token=access_token,
                    refresh_token=refresh_token,
                )

                remote_page_ids = self.fetch_remote_page_ids(
                    confluence_api,
                    workspace_id,
                    space_ids=space_ids,
                )

                confluence_seadb_api = ConfluenceSeaDBAPI(project_uuid)
                local_page_map = self.get_local_page_ids(confluence_seadb_api, connection_id)

                stale_page_ids = set(local_page_map.keys()) - remote_page_ids

                if stale_page_ids:
                    confluence_seadb_api.mark_pages_deleted(connection_id, stale_page_ids, local_page_map)

                cleaned_at = datetime.datetime.now(datetime.UTC)
                self.seaqa_db.update_connection_last_cleaned_at(connection_id, cleaned_at)
                logger.info('Marked %d confluence pages deleted for connection %s', len(stale_page_ids), connection_id)
            except Exception as e:
                logger.exception('Failed to clean confluence pages for connection %s: %s', connection_id, e)

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                logging.info('starts to clean confluence pages task')
                try:
                    self.clean_confluence_pages()
                except Exception as e:
                    logging.exception('clean confluence pages task error: %s', e)

    def cancel(self):
        self.finished.set()
