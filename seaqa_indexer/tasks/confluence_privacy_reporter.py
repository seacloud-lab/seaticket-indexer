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
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                logging.info('starts to report confluence privacy data')
                try:
                    self._report_and_cleanup()
                except Exception as e:
                    logging.exception('confluence privacy report error: %s', e)

    def _report_and_cleanup(self):
        projects = self.seaqa_db.get_projects_need_privacy_report(
            ConnectionType.CONFLUENCE.value
        )
        logger.info('Found %d projects for confluence privacy reporting', len(projects))

        for project in projects:
            project_uuid = project.project_uuid

            try:
                oauth = self.seaqa_db.get_project_confluence_oauth(project_uuid)
                if not oauth or not oauth.access_token:
                    logger.warning('Confluence OAuth missing for project %s', project_uuid)
                    continue

                # A project owns one aggregated user table. Connections only
                # provide the workspace used to resolve updated display names.
                connections = self.seaqa_db.get_connections_by_project_uuid(project_uuid)
                confluence_connections = [
                    c for c in connections
                    if c.type == ConnectionType.CONFLUENCE.value and not c.deleted and c.is_active
                ]
                if not confluence_connections:
                    continue

                confluence_api = ConfluenceAPI(oauth.access_token, oauth.refresh_token, oauth.expires_at)
                confluence_seadb_api = ConfluenceSeaDBAPI(project_uuid)
                accounts = confluence_seadb_api.get_all_users()
                if not accounts:
                    continue

                workspace_by_connection = {}
                for connection in confluence_connections:
                    connection_config = decrypt_config(json.loads(connection.config)) if connection.config else {}
                    workspace_id = connection_config.get('workspace_id')
                    if workspace_id:
                        workspace_by_connection[str(connection.id)] = workspace_id

                accounts_by_id = {account['user_id']: account for account in accounts}
                all_batches_succeeded = True
                cycle_period = DEFAULT_CYCLE_DAYS
                closed_pks = []
                updated_names = {}

                # Report the project-level user set in batches.
                for i in range(0, len(accounts), self.batch_size):
                    batch = accounts[i:i + self.batch_size]
                    payload = [
                        {'accountId': a['user_id'], 'updatedAt': a['record_modified_time']}
                        for a in batch
                    ]

                    # Refresh token if near expiry
                    now = datetime.datetime.now(datetime.UTC)
                    if not confluence_api.expires_at or time_to_utc_time(str(confluence_api.expires_at)) <= now + datetime.timedelta(seconds=60):
                        logger.info('Refreshing Confluence access token for project %s', project_uuid)
                        try:
                            new_tokens = confluence_api.refresh_access_token()
                            self.seaqa_db.update_project_confluence_oauth(
                                project_uuid,
                                new_tokens['access_token'],
                                new_tokens['expires_at'],
                                new_tokens['refresh_token'],
                            )
                        except Exception:
                            logger.exception('Failed to refresh token for project %s', project_uuid)
                            all_batches_succeeded = False
                            continue

                    try:
                        result = ConfluenceAPI.report_accounts(confluence_api.access_token, payload)
                    except Exception:
                        logger.exception('Privacy report API failed for project %s batch %d',
                                         project_uuid, i // self.batch_size)
                        all_batches_succeeded = False
                        continue

                    cycle_period = result.get('cycle_period', DEFAULT_CYCLE_DAYS)

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
                            connection_ids = (account or {}).get('connection_id') or []
                            workspace_ids = [
                                workspace_by_connection.get(str(connection_id))
                                for connection_id in connection_ids
                                if workspace_by_connection.get(str(connection_id))
                            ]
                            for workspace_id in workspace_ids:
                                try:
                                    name = confluence_api.get_user_display_name(account_id, workspace_id)
                                    if name and account and account.get('_pk') is not None:
                                        updated_names[account['_pk']] = name
                                        break
                                except Exception:
                                    logger.warning(
                                        'Failed to refresh user %s from workspace %s',
                                        account_id,
                                        workspace_id,
                                        exc_info=True,
                                    )

                # Apply all status changes once per project after processing
                # every batch, while still retaining successful batch results
                # if another batch needs to be retried.
                confluence_seadb_api.delete_users(closed_pks)
                confluence_seadb_api.update_user_names(updated_names)

                if all_batches_succeeded:
                    next_report_at = datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=cycle_period)
                    self.seaqa_db.update_project_next_privacy_report_at(project_uuid, next_report_at)
                    logger.info('Privacy report completed for project %s, next report at %s (cycle=%d days)',
                                project_uuid, next_report_at.isoformat(), cycle_period)
                else:
                    logger.warning('One or more privacy report batches failed for project %s, will retry next cycle',
                                   project_uuid)

            except Exception as e:
                logger.exception('Failed privacy report for project %s: %s', project_uuid, e)

    def cancel(self):
        self.finished.set()
