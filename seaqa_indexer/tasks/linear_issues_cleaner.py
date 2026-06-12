import time
import json
import logging
import datetime
from threading import Thread, Event

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils import decrypt_config
from seaqa_indexer.utils.linear_api import LinearAPI
from seaqa_indexer.seadb_models.linear_seadb_api import LinearSeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.constants import ConnectionType

logger = logging.getLogger(__name__)


class LinearIssuesCleaner(object):
    def __init__(self):
        self._enabled = True
        self._interval = 24 * 60 * 60  # Running interval
        self.check_days = 30  # Only check connections that are more than 30 days old since the last cleanup.

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start linear issues cleaner: it is not enabled!')
            return

        LinearIssuesCleanerTimer(self._interval, self.check_days).start()
        logging.info('linear issues cleaner timer is started')

    def is_enabled(self):
        return self._enabled


class LinearIssuesCleanerTimer(Thread):
    """Compare remote Linear issues with local records and mark missing issues as deleted."""

    def __init__(self, interval, check_days):
        Thread.__init__(self)
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)

        self.finished = Event()
        self.interval = interval
        self.check_days = check_days
        self.page_size = 100

    def fetch_remote_issue_ids(self, linear_api, team_id, include_archived):
        issue_ids = set()
        cursor = None

        while True:
            issues, page_info = linear_api.list_issues(
                since=None,
                after=cursor,
                first=self.page_size,
                team_id=team_id,
                include_archived=include_archived,
            )
            if not issues:
                break

            for issue in issues:
                issue_id = issue.get('id')
                if not issue_id:
                    continue
                issue_ids.add(issue_id)

            if not page_info.get('hasNextPage'):
                break
            cursor = page_info.get('endCursor')
            if not cursor:
                break
            time.sleep(0.34)

        return issue_ids

    def get_local_issue_ids(self, linear_seadb_api, connection_id):
        offset = 0
        local_issue_ids = set()

        while True:
            rows = linear_seadb_api.get_issue_ids_by_page(connection_id, self.page_size, offset)
            if not rows:
                break
            for row in rows:
                issue_id = row.get('issue_id')
                if issue_id:
                    local_issue_ids.add(issue_id)
            if len(rows) < self.page_size:
                break
            offset += self.page_size

        return local_issue_ids

    def clean_linear_issues(self):
        connections = self.seaqa_db.get_need_cleaned_connections_by_type(self.check_days, ConnectionType.LINEAR.value)
        logger.info('Found %d linear connections to clean', len(connections))

        for connection in connections:
            connection_id = connection.id
            project_uuid = connection.project_uuid

            try:
                connection_config = decrypt_config(json.loads(connection.config)) if connection.config else {}
                team_id = connection_config.get('team_id')
                include_archived = connection_config.get('include_archived', True)

                oauth_info = self.seaqa_db.get_project_linear_oauth(project_uuid)
                access_token = oauth_info.access_token
                refresh_token = oauth_info.refresh_token
                if not access_token:
                    logger.warning('Linear access_token missing for project %s', project_uuid)
                    continue

                linear_api = LinearAPI(
                    access_token=access_token,
                    refresh_token=refresh_token
                )

                remote_issue_ids = self.fetch_remote_issue_ids(
                    linear_api,
                    team_id,
                    include_archived=include_archived,
                )

                linear_seadb_api = LinearSeaDBAPI(project_uuid)
                local_ids = self.get_local_issue_ids(linear_seadb_api, connection_id)

                need_deleted_ids = local_ids - remote_issue_ids
                for issue_id in need_deleted_ids:
                    linear_seadb_api.mark_issue_deleted(connection_id, issue_id)

                cleaned_at = datetime.datetime.now(datetime.UTC)
                self.seaqa_db.update_connection_last_cleaned_at(connection_id, cleaned_at)
                logger.info('Marked %d linear issues deleted and %d restored for connection %s', len(need_deleted_ids), connection_id)
            except Exception as e:
                logger.exception('Failed to clean linear issues for connection %s: %s', connection_id, e)

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                logging.info('starts to clean linear issues task')
                try:
                    self.clean_linear_issues()
                except Exception as e:
                    logging.exception('clean linear issues task error: %s', e)

    def cancel(self):
        self.finished.set()