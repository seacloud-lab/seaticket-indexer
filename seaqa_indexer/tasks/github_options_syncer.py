import json
import logging
from threading import Thread, Event
from urllib.parse import urlparse

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils import decrypt_config
from seaqa_indexer.utils.github_issues_api import GitHubAPI, GitHubAppNotInstalled
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI
from seaqa_indexer.handlers.github_handler import GithubIssueHandler

logger = logging.getLogger(__name__)


class GithubOptionsSyncer(object):
    def __init__(self):
        self._enabled = True
        self._interval = 60 * 60 * 24
        self._options_sync_interval_days = 7

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start github options syncer: it is not enabled!')
            return

        GithubOptionsSyncerTimer(self._interval, self._options_sync_interval_days).start()
        logging.info('github options syncer timer is started')

    def is_enabled(self):
        return self._enabled


class GithubOptionsSyncerTimer(Thread):
    def __init__(self, interval, options_sync_interval_days):
        Thread.__init__(self)
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)

        self.finished = Event()
        self.interval = interval
        self.options_sync_interval_days = options_sync_interval_days

    def _sync_stale_github_options(self):
        connections = self.seaqa_db.get_github_connections_need_options_sync(self.options_sync_interval_days)

        for connection in connections:
            connection_id = connection.id
            project_uuid = connection.project_uuid

            try:
                config = decrypt_config(json.loads(connection.config)) if connection.config else {}
                server_url = config.get('repository')
                installation_id = config.get('installation_id')

                if not server_url or not installation_id:
                    logger.warning('GitHub connection %s missing repository or installation_id, skipping', connection_id)
                    continue

                path = urlparse(server_url).path
                parts = path.strip("/").split("/")
                if len(parts) < 2:
                    logger.warning('GitHub connection %s has invalid repository URL: %s', connection_id, server_url)
                    continue
                repo_owner, repo_name = parts[0], parts[1]

                try:
                    github_api = GitHubAPI(installation_id)
                except GitHubAppNotInstalled:
                    logger.warning('GitHub connection %s: app not installed, skipping options sync', connection_id)
                    continue

                github_seadb_api = GitHubSeaDBAPI(project_uuid)
                handler = GithubIssueHandler()
                handler.sync_github_options(connection_id, github_seadb_api, github_api, repo_owner, repo_name, self.seaqa_db)

            except Exception as e:
                logger.exception('Failed to sync GitHub options for connection %s: %s', connection_id, e)

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                logger.info('Start to check GitHub options sync')
                try:
                    self._sync_stale_github_options()
                except Exception as e:
                    logger.exception('GitHub options sync task error: %s', e)

    def cancel(self):
        self.finished.set()
