import sys
import time
import json
import logging
import threading
import datetime

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils import decrypt_config
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.utils.linear_api import LinearAPI
from seaqa_indexer.seadb_models.linear_seadb_api import LinearSeaDBAPI


logger = logging.getLogger(__name__)


class LinearIssuesCleaner(object):
    """
    Periodically mark deleted Linear issues by comparing remote IDs with local IDs.
    """

    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.should_stop = threading.Event()
        self.clear_interval = 30 * 24 * 60 * 60  # 30 days

    @property
    def tname(self):
        return threading.current_thread().name

    def start(self):
        threading.Thread(
            target=self.cron_clean_linear_issues,
            name='cron_clean_linear_issues_thread',
            daemon=True
        ).start()

    def cron_clean_linear_issues(self):
        logger.info('Start linear issues cleaner timer')
        while not self.should_stop.is_set():
            self.should_stop.wait(self.clear_interval)

            if not self.should_stop.is_set():
                logger.info('Start to clean linear issues')
                try:
                    self.clean_linear_issues()
                except Exception as e:
                    logger.exception('Cron clean linear issues error: %s', e)

    def _fetch_remote_issue_ids(self, linear_api, team_id, include_archived):
        per_page = 100
        cursor = None
        remote_ids = set()
        remote_deleted_ids = set()

        while True:
            issues, page_info = linear_api.list_issues(
                since=None,
                after=cursor,
                first=per_page,
                team_id=team_id,
                include_archived=include_archived,
            )
            if not issues:
                break
            for issue in issues:
                issue_id = issue.get('id')
                if not issue_id:
                    continue
                remote_ids.add(issue_id)
                if issue.get('archivedAt'):
                    remote_deleted_ids.add(issue_id)
            if not page_info.get('hasNextPage'):
                break
            cursor = page_info.get('endCursor')

        return remote_ids, remote_deleted_ids

    def clean_linear_issues(self):
        connections = self.seaqa_db.get_need_cleaned_connections_by_type(self.check_days, ConnectionType.LINEAR.value)
        logger.info('Found %d active linear connections to check for deleted issues', len(connections))

        for connection in connections:
            connection_id = connection.id
            project_uuid = connection.project_uuid
            try:
                config_data = json.loads(connection.config) if connection.config else {}
            except Exception:
                config_data = {}
            config_data = decrypt_config(config_data)

            oauth_info = self.seaqa_db.get_project_linear_oauth(project_uuid)
            access_token = oauth_info.get('access_token')
            refresh_token = oauth_info.get('refresh_token')
            if not access_token:
                logger.warning('Linear access_token missing for project %s', project_uuid)
                continue

            linear_api = LinearAPI(
                access_token=access_token,
                refresh_token=refresh_token,
                expires_at=oauth_info.get('expires_at'),
            )

            if linear_api.last_token_update:
                expires_at = LinearAPI.calc_expires_at(linear_api.last_token_update.get('expires_in'))
                self.seaqa_db.upsert_project_linear_oauth(
                    project_uuid,
                    linear_api.access_token,
                    expires_at=expires_at,
                    refresh_token=linear_api.refresh_token,
                )

            team_id = config_data.get('team_id')

            remote_ids, remote_deleted_ids = self._fetch_remote_issue_ids(
                linear_api,
                team_id,
                include_archived=True
            )

            linear_seadb_api = LinearSeaDBAPI(project_uuid)
            page_size = 1000
            offset = 0
            local_ids = set()
            local_deleted_ids = set()
            while True:
                local_rows = linear_seadb_api.get_issue_ids_by_page(connection_id, page_size, offset)
                if not local_rows:
                    break
                for row in local_rows:
                    issue_id = row.get('issue_id')
                    if not issue_id:
                        continue
                    local_ids.add(issue_id)
                    if row.get('deleted'):
                        local_deleted_ids.add(issue_id)
                if len(local_rows) < page_size:
                    break
                offset += page_size

            need_deleted_ids = (local_ids - remote_ids) | (local_ids & remote_deleted_ids)
            need_deleted_ids -= local_deleted_ids
            need_restore_ids = (local_deleted_ids & remote_ids) - remote_deleted_ids

            for issue_id in need_deleted_ids:
                linear_seadb_api.mark_issue_deleted(connection_id, issue_id)
            for issue_id in need_restore_ids:
                linear_seadb_api.mark_issue_active(connection_id, issue_id)

            cleaned_at = datetime.datetime.now(datetime.UTC)
            self.seaqa_db.update_connection_last_cleaned_at(connection_id, cleaned_at)


def start():
    cleaner = LinearIssuesCleaner()
    logger.info("Linear issues cleaner process initialized.")
    try:
        cleaner.start()
    except Exception as e:
        logger.exception(e)
        sys.exit(1)

    while True:
        time.sleep(2)


def main():
    LogConfigurator(config.LOG_LEVEL, config.LOG_FILE)
    start()


if __name__ == "__main__":
    main()
