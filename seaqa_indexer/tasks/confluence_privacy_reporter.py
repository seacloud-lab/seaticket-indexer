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

DEFAULT_CYCLE_DAYS = 7


class ConfluencePrivacyReporter(object):
    def __init__(self):
        self._enabled = True
        self._interval = 24 * 60 * 60  # Check daily

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start confluence privacy reporter: it is not enabled!')
            return

        ConfluencePrivacyReporterTimer(self._interval).start()
        logging.info('confluence privacy reporter timer is started')

    def is_enabled(self):
        return self._enabled


class ConfluencePrivacyReporterTimer(Thread):
    """Daily check for projects due for Confluence privacy reporting.

    Queries projects whose next_privacy_report_at is within 1 day,
    reports all stored Confluence account IDs once per project, and
    schedules the next report based on the cyclePeriod returned by the
    Atlassian API.
    """

    def __init__(self, interval):
        Thread.__init__(self)
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.finished = Event()
        self.interval = interval
        self.batch_size = 90  # Atlassian API max per request

    def run(self):
        while not self.finished.is_set():
            self._report_and_cleanup()
            print('confluence privacy reporter finished one cycle, waiting for next cycle...')
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                logging.info('starts to report confluence privacy data')
                try:
                    self._report_and_cleanup()
                except Exception as e:
                    logging.exception('confluence privacy report error: %s', e)

    def _ensure_token_fresh(self, confluence_api, project_uuid):
        """Refresh the Confluence OAuth token if it is near or past expiry."""
        now = datetime.datetime.now(datetime.UTC)
        if confluence_api.expires_at and time_to_utc_time(str(confluence_api.expires_at)) > now + datetime.timedelta(seconds=60):
            return
        logger.info('Refreshing Confluence access token for project %s', project_uuid)
        new_tokens = confluence_api.refresh_access_token()
        self.seaqa_db.update_project_confluence_oauth(
            project_uuid,
            new_tokens['access_token'],
            new_tokens['expires_at'],
            new_tokens['refresh_token'],
        )

    def _report_and_cleanup(self):
        offset = 0
        page_size = 500

        while True:
            oauth_rows = self.seaqa_db.get_confluence_oauth_page(offset, page_size)
            if not oauth_rows:
                break

            for oauth in oauth_rows:
                project_uuid = oauth.project_uuid

                try:
                    if not oauth.access_token:
                        logger.warning('Confluence OAuth access token missing for project %s', project_uuid)
                        continue

                    confluence_seadb_api = ConfluenceSeaDBAPI(project_uuid)

                    # Get users due for reporting (NULL or within 1 day of deadline)
                    accounts = confluence_seadb_api.get_users_needing_report()
                    if not accounts:
                        continue

                    confluence_connections = self.seaqa_db.get_project_connections_by_type(
                        project_uuid, ConnectionType.CONFLUENCE.value
                    )
                    if not confluence_connections:
                        continue

                    confluence_api = ConfluenceAPI(oauth.access_token, oauth.refresh_token, oauth.expires_at)

                    workspace_by_connection = {}
                    for connection in confluence_connections:
                        connection_config = decrypt_config(json.loads(connection.config)) if connection.config else {}
                        workspace_id = connection_config.get('workspace_id')
                        if workspace_id:
                            workspace_by_connection[str(connection.id)] = workspace_id

                    accounts_by_id = {account['user_id']: account for account in accounts}
                    cycle_period = DEFAULT_CYCLE_DAYS

                    # Report the project-level user set in batches.
                    for i in range(0, len(accounts), self.batch_size):
                        batch = accounts[i:i + self.batch_size]
                        payload = [
                            {'accountId': a['user_id'], 'updatedAt': a['record_modified_time']}
                            for a in batch
                        ]

                        # Refresh token if near expiry
                        self._ensure_token_fresh(confluence_api, project_uuid)

                        try:
                            result = confluence_api.report_accounts(payload)
                        except Exception:
                            logger.exception('Privacy report API failed for project %s batch %d',
                                             project_uuid, i // self.batch_size)
                            continue

                        next_report_at = datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=cycle_period)

                        closed_pks = []
                        updated_names = {}
                        failed_pks = set()

                        for account_result in result.get('accounts', []):
                            account_id = account_result.get('accountId')
                            status = account_result.get('status')
                            if not account_id:
                                continue

                            if status == 'closed':
                                logger.info('Deleting user data for closed account %s in project %s',
                                            account_id, project_uuid)
                                account = accounts_by_id.get(account_id)
                                if account and account.get('_pk') is not None:
                                    closed_pks.append(account['_pk'])

                            elif status == 'updated':
                                logger.info('Refreshing display name for updated account %s in project %s',
                                            account_id, project_uuid)
                                account = accounts_by_id.get(account_id)
                                if not account:
                                    logger.warning('Account %s not found in project %s for update', account_id, project_uuid)
                                    continue
                                connection_ids = (account or {}).get('connection_ids') or []
                                workspace_ids = [
                                    workspace_by_connection.get(str(cid))
                                    for cid in connection_ids
                                    if workspace_by_connection.get(str(cid))
                                ]
                                refreshed = False
                                for ws_id in workspace_ids:
                                    try:
                                        name = confluence_api.get_user_display_name(account_id, ws_id)
                                        if name is not None:
                                            updated_names[account['_pk']] = name
                                            refreshed = True
                                            break
                                    except Exception:
                                        logger.warning('Failed to refresh user %s from workspace %s',
                                                       account_id, ws_id, exc_info=True)
                                if not refreshed:
                                    failed_pks.add(account['_pk'])

                        # Apply status changes for this batch immediately
                        confluence_seadb_api.delete_users(closed_pks)
                        confluence_seadb_api.update_user_names(updated_names)

                        # Mark successfully reported users; exclude closed and failed-updated
                        skip_pks = failed_pks | set(closed_pks)
                        batch_pks = [
                            a['_pk'] for a in batch
                            if a.get('_pk') is not None and a['_pk'] not in skip_pks
                        ]
                        confluence_seadb_api.update_users_next_report_at(batch_pks, next_report_at)

                    logger.info('Privacy report completed for project %s, %d users total',
                                project_uuid, len(accounts))

                except Exception as e:
                    logger.exception('Failed privacy report for project %s: %s', project_uuid, e)

            if len(oauth_rows) < page_size:
                break
            offset += page_size

    def cancel(self):
        self.finished.set()
