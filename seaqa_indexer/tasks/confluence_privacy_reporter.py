import json
import logging
import datetime
from threading import Thread, Event

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils import decrypt_config, time_to_utc_time
from seaqa_indexer.utils.confluence_api import ConfluenceAPI
from seaqa_indexer.seadb_models.confluence_seadb_api import ConfluenceSeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.constants import ConnectionType

logger = logging.getLogger(__name__)


class ConfluencePrivacyReporter(object):
    def __init__(self):
        self._enabled = True
        self._interval = 7 * 24 * 60 * 60  # 7 days (Atlassian requirement)

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start confluence privacy reporter: it is not enabled!')
            return

        ConfluencePrivacyReporterTimer(self._interval).start()
        logging.info('confluence privacy reporter timer is started')

    def is_enabled(self):
        return self._enabled


class ConfluencePrivacyReporterTimer(Thread):
    """Periodically report stored Confluence account IDs to Atlassian's privacy API
    and handle closed/updated user accounts per GDPR requirements."""

    def __init__(self, interval):
        Thread.__init__(self)
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.finished = Event()
        self.interval = interval
        self.batch_size = 90  # Atlassian API max per request

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                logging.info('starts to report confluence privacy data')
                try:
                    self._report_and_cleanup()
                except Exception as e:
                    logging.exception('confluence privacy report error: %s', e)

    def _report_and_cleanup(self):
        connections = self.seaqa_db.get_connections_need_privacy_report(
            7, ConnectionType.CONFLUENCE.value
        )
        logger.info('Found %d confluence connections for privacy reporting', len(connections))

        for connection in connections:
            connection_id = connection.id
            project_uuid = connection.project_uuid

            try:
                connection_config = decrypt_config(json.loads(connection.config)) if connection.config else {}
                workspace_id = connection_config.get('workspace_id')
                if not workspace_id:
                    logger.warning('Confluence workspace_id missing for connection %s, skipping', connection_id)
                    continue

                oauth = self.seaqa_db.get_project_confluence_oauth(project_uuid)
                if not oauth or not oauth.access_token:
                    logger.warning('Confluence OAuth missing for project %s', project_uuid)
                    continue

                confluence_seadb_api = ConfluenceSeaDBAPI(project_uuid)
                accounts = confluence_seadb_api.get_all_user_accounts(connection_id)
                if not accounts:
                    continue

                confluence_api = ConfluenceAPI(oauth.access_token, oauth.refresh_token, oauth.expires_at)

                any_batch_succeeded = False
                # Report accounts in batches of 90
                for i in range(0, len(accounts), self.batch_size):
                    batch = accounts[i:i + self.batch_size]
                    payload = [
                        {'accountId': a['account_id'], 'updatedAt': a['updated_time']}
                        for a in batch
                    ]

                    # Refresh user token if near expiry before each API call
                    now = datetime.datetime.now(datetime.UTC)
                    if not confluence_api.expires_at or time_to_utc_time(str(confluence_api.expires_at)) <= now + datetime.timedelta(seconds=60):
                        logger.info('Confluence access token near expiry, refreshing before privacy report batch')
                        try:
                            new_tokens = confluence_api.refresh_access_token()
                            self.seaqa_db.update_project_confluence_oauth(
                                project_uuid,
                                new_tokens['access_token'],
                                new_tokens['expires_at'],
                                new_tokens['refresh_token'],
                            )
                        except Exception:
                            logger.exception('Failed to refresh access token for project %s', project_uuid)
                            continue

                    try:
                        results = ConfluenceAPI.report_accounts(confluence_api.access_token, payload)
                    except Exception:
                        logger.exception('Privacy report API failed for connection %s batch %d',
                                         connection_id, i // self.batch_size)
                        continue

                    any_batch_succeeded = True

                    for result in results:
                        account_id = result.get('accountId')
                        status = result.get('status')
                        if not account_id:
                            continue

                        if status == 'closed':
                            logger.info('Deleting user data for closed account %s in connection %s',
                                        account_id, connection_id)
                            confluence_seadb_api.delete_user_by_account_id(connection_id, account_id)

                        elif status == 'updated':
                            logger.info('Refreshing display name for updated account %s in connection %s',
                                        account_id, connection_id)
                            try:
                                name = confluence_api.get_user_display_name(account_id, workspace_id)
                                if name:
                                    confluence_seadb_api.upsert_user(connection_id, account_id, name)
                            except Exception:
                                logger.exception('Failed to refresh user %s for connection %s',
                                                 account_id, connection_id)

                if any_batch_succeeded:
                    reported_at = datetime.datetime.now(datetime.UTC)
                    self.seaqa_db.update_connection_last_privacy_reported_at(connection_id, reported_at)
                    logger.info('Privacy report completed for connection %s, processed %d accounts',
                                connection_id, len(accounts))
                else:
                    logger.warning('All privacy report batches failed for connection %s, will retry next cycle',
                                   connection_id)

            except Exception as e:
                logger.exception('Failed privacy report for connection %s: %s', connection_id, e)

    def cancel(self):
        self.finished.set()
