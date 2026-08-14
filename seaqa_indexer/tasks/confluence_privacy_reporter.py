import json
import logging
import datetime
from threading import Thread, Event

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils import decrypt_config, time_to_utc_time
from seaqa_indexer.utils.confluence_api import ConfluenceAPI, ConfluenceWorkspaceUnavailableException, ConfluenceTokenRevokedError, ConfluenceUserNotFoundException, DEFAULT_CYCLE_DAYS
from seaqa_indexer.seadb_models.confluence_seadb_api import ConfluenceSeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.constants import ConnectionType

logger = logging.getLogger(__name__)

REPORT_LOOKAHEAD_DAYS = 1 # day


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
            logger.info('starts to report confluence privacy data')
            try:
                self._report_and_cleanup()
            except Exception:
                logger.exception('confluence privacy report error')
            self.finished.wait(self.interval)

    def _ensure_token_fresh(self, confluence_api, project_uuid):
        """Refresh the Confluence OAuth token if it is near or past expiry."""
        now = datetime.datetime.now(datetime.UTC)
        if confluence_api.expires_at and time_to_utc_time(str(confluence_api.expires_at)) > now + datetime.timedelta(seconds=120):
            return
        logger.info('Refreshing Confluence access token for project %s', project_uuid)
        new_token = confluence_api.refresh_access_token()
        self.seaqa_db.update_project_confluence_oauth(
            project_uuid,
            new_token['access_token'],
            new_token['expires_at'],
            new_token['refresh_token'],
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
                confluence_seadb_api = ConfluenceSeaDBAPI(project_uuid)
                accounts = []

                try:
                    if not oauth.access_token:
                        logger.warning('Confluence OAuth access token missing for project %s', project_uuid)
                        continue

                    confluence_connections = self.seaqa_db.get_project_connections_by_type(
                        project_uuid, ConnectionType.CONFLUENCE.value
                    )
                    if not confluence_connections:
                        continue

                    confluence_api = ConfluenceAPI(oauth.access_token, oauth.refresh_token, oauth.expires_at)
                    self._ensure_token_fresh(confluence_api, project_uuid)
                    accessible_workspace_ids = {
                        resource.get('id')
                        for resource in confluence_api.get_accessible_resources()
                        if resource.get('id')
                    }

                    connection_id_to_workspace_id = {}
                    for connection in confluence_connections:
                        connection_config = decrypt_config(json.loads(connection.config)) if connection.config else {}
                        workspace_id = connection_config.get('workspace_id')
                        if workspace_id and workspace_id in accessible_workspace_ids:
                            connection_id_to_workspace_id[str(connection.id)] = workspace_id

                    if not connection_id_to_workspace_id:
                        continue

                    # Get users due for reporting (NULL or within 1 day of deadline)
                    accounts = confluence_seadb_api.get_users_needing_report(REPORT_LOOKAHEAD_DAYS)
                    if not accounts:
                        continue

                    accounts_by_id = {account['user_id']: account for account in accounts}

                    # Report the project-level user set in batches.
                    for i in range(0, len(accounts), self.batch_size):
                        batch = accounts[i:i + self.batch_size]
                        payload = [
                            {'accountId': a['user_id'], 'updatedAt': a['record_modified_time']}
                            for a in batch
                        ]

                        # Refresh token if near expiry
                        self._ensure_token_fresh(confluence_api, project_uuid)

                        result = confluence_api.report_accounts(payload)
                        cycle_period = result.get('cycle_period') or DEFAULT_CYCLE_DAYS
                        next_report_at = datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=cycle_period)

                        closed_pks = []
                        updated_names = {}

                        for account_result in result.get('accounts', []):
                            account_id = account_result.get('accountId')
                            status = account_result.get('status')
                            if not account_id:
                                continue

                            if status == 'closed':
                                logger.info('Deleting user data for closed account %s in project %s', account_id, project_uuid)
                                account = accounts_by_id.get(account_id)
                                if account and account.get('_pk') is not None:
                                    closed_pks.append(account['_pk'])

                            elif status == 'updated':
                                logger.info('Refreshing display name for updated account %s in project %s', account_id, project_uuid)
                                account = accounts_by_id.get(account_id)
                                connection_ids = (account or {}).get('connection_ids') or []
                                workspace_ids = set([
                                    connection_id_to_workspace_id.get(str(cid))
                                    for cid in connection_ids
                                    if connection_id_to_workspace_id.get(str(cid))
                                ])
                                for ws_id in workspace_ids:
                                    try:
                                        name = confluence_api.get_user_display_name(account_id, ws_id)
                                    except ConfluenceUserNotFoundException:
                                        logger.warning('project_uuid %s workspace_id %s get display_name Failed for account %s, trying next', project_uuid, ws_id, account_id)
                                        continue
                                    except ConfluenceWorkspaceUnavailableException:
                                        logger.warning('project_uuid %s workspace_id %s unavailable for account %s, trying next', project_uuid, ws_id, account_id)
                                        continue
                                    if name:
                                        updated_names[account['_pk']] = name
                                        break
                                else:
                                    logger.info('No valid workspace for account %s in project %s, deleting user', account_id, project_uuid)
                                    closed_pks.append(account['_pk'])

                        # Apply status changes for this batch immediately
                        confluence_seadb_api.delete_users(closed_pks)
                        confluence_seadb_api.update_user_names(updated_names)

                        # Mark reported users; only exclude closed accounts
                        batch_pks = [
                            a['_pk'] for a in batch
                            if a.get('_pk') is not None and a['_pk'] not in closed_pks
                        ]
                        confluence_seadb_api.update_users_next_report_at(batch_pks, next_report_at)

                    logger.info('Privacy report completed for project %s, %d users total', project_uuid, len(accounts))

                except ConfluenceTokenRevokedError:
                    logger.warning('Confluence OAuth token revoked for project %s, deleting all %d users', project_uuid, len(accounts))
                    for i in range(0, len(accounts), self.batch_size):
                        batch_accounts = accounts[i:i + self.batch_size]
                        confluence_seadb_api.delete_users([a['_pk'] for a in batch_accounts])
                except Exception as e:
                    logger.exception('Failed privacy report for project %s: %s', project_uuid, e)

            if len(oauth_rows) < page_size:
                break
            offset += page_size

    def cancel(self):
        self.finished.set()
