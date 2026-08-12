import datetime
import json
import logging
from threading import Event, Thread

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.seadb_models.jira_seadb_api import JiraSeaDBAPI
from seaqa_indexer.utils import decrypt_config, time_to_utc_time
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.utils.jira_issues_api import (
    DEFAULT_CYCLE_DAYS,
    JiraAPI,
    JiraSiteUnavailableException,
    JiraTokenRevokedError,
    JiraUserOrSiteNotFoundException,
)
from seaqa_indexer.utils.seaqa_db import SeaqaDB


logger = logging.getLogger(__name__)

REPORT_LOOKAHEAD_DAYS = 1


class JiraPrivacyReporter(object):
    def __init__(self):
        self._enabled = True
        self._interval = 24 * 60 * 60

    def start(self):
        if not self.is_enabled():
            logger.warning('Can not start Jira privacy reporter: it is not enabled!')
            return

        JiraPrivacyReporterTimer(self._interval).start()
        logger.info('Jira privacy reporter timer is started')

    def is_enabled(self):
        return self._enabled


class JiraPrivacyReporterTimer(Thread):
    def __init__(self, interval):
        Thread.__init__(self)
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.finished = Event()
        self.interval = interval
        self.batch_size = 90

    def run(self):
        while not self.finished.is_set():
            logger.info('starts to report Jira privacy data')
            try:
                self._report_and_cleanup()
            except Exception:
                logger.exception('Jira privacy report error')
            self.finished.wait(self.interval)

    def _ensure_token_fresh(self, jira_api, project_uuid):
        now = datetime.datetime.now(datetime.UTC)
        if (
            jira_api.expires_at
            and time_to_utc_time(str(jira_api.expires_at))
            > now + datetime.timedelta(seconds=120)
        ):
            return

        logger.info('Refreshing Jira access token for project %s', project_uuid)
        new_token = jira_api.refresh_access_token()
        self.seaqa_db.update_project_jira_oauth(
            project_uuid,
            new_token['access_token'],
            new_token['expires_at'],
            new_token['refresh_token'],
        )

    def _report_and_cleanup(self):
        offset = 0
        page_size = 500

        while True:
            oauth_rows = self.seaqa_db.get_jira_oauth_page(offset, page_size)
            if not oauth_rows:
                break

            for oauth in oauth_rows:
                project_uuid = oauth.project_uuid
                jira_seadb_api = JiraSeaDBAPI(project_uuid)
                accounts = []

                try:
                    if not oauth.access_token:
                        logger.warning(
                            'Jira OAuth access token missing for project %s',
                            project_uuid,
                        )
                        continue

                    jira_connections = self.seaqa_db.get_project_connections_by_type(
                        project_uuid,
                        ConnectionType.JIRA_ISSUE.value,
                    )
                    if not jira_connections:
                        continue

                    jira_api = JiraAPI(
                        oauth.access_token,
                        oauth.refresh_token,
                        '',
                        '',
                        expires_at=oauth.expires_at,
                    )
                    self._ensure_token_fresh(jira_api, project_uuid)
                    accessible_site_ids = {
                        resource.get('id')
                        for resource in jira_api.get_accessible_resources()
                        if resource.get('id')
                    }

                    connection_id_to_site = {}
                    for connection in jira_connections:
                        config = (
                            decrypt_config(json.loads(connection.config))
                            if connection.config
                            else {}
                        )
                        site_id = config.get('site_id')
                        site_url = config.get('site_url')
                        if site_id and site_id in accessible_site_ids:
                            connection_id_to_site[str(connection.id)] = (
                                site_id,
                                site_url,
                            )

                    if not connection_id_to_site:
                        continue

                    accounts = jira_seadb_api.get_users_needing_report(
                        REPORT_LOOKAHEAD_DAYS
                    )
                    accounts = [
                        account
                        for account in accounts
                        if account.get('user_id') and account.get('user_id') != 'unknown'
                    ]
                    if not accounts:
                        continue

                    accounts_by_id = {
                        account['user_id']: account for account in accounts
                    }

                    for index in range(0, len(accounts), self.batch_size):
                        batch = accounts[index:index + self.batch_size]
                        payload = [
                            {
                                'accountId': account['user_id'],
                                'updatedAt': account['record_modified_time'],
                            }
                            for account in batch
                        ]

                        self._ensure_token_fresh(jira_api, project_uuid)
                        result = jira_api.report_accounts(payload)
                        cycle_period = (
                            result.get('cycle_period') or DEFAULT_CYCLE_DAYS
                        )
                        next_report_at = (
                            datetime.datetime.now(datetime.UTC)
                            + datetime.timedelta(days=cycle_period)
                        )

                        closed_pks = []
                        updated_names = {}
                        for account_result in result.get('accounts', []):
                            account_id = account_result.get('accountId')
                            status = account_result.get('status')
                            account = accounts_by_id.get(account_id)
                            if not account:
                                continue

                            if status == 'closed':
                                closed_pks.append(account['_pk'])
                                continue

                            if status != 'updated':
                                continue

                            for connection_id in account.get('connection_ids') or []:
                                site = connection_id_to_site.get(str(connection_id))
                                if not site:
                                    continue
                                site_id, site_url = site
                                jira_api.site_id = site_id
                                jira_api.site_url = (site_url or '').rstrip('/')
                                try:
                                    name = jira_api.get_user_display_name(account_id)
                                except JiraUserOrSiteNotFoundException:
                                    continue
                                except JiraSiteUnavailableException:
                                    continue
                                if name:
                                    updated_names[account['_pk']] = name
                                    break
                            else:
                                logger.info(
                                    'No valid Jira site for account %s in project %s; '
                                    'deleting user',
                                    account_id,
                                    project_uuid,
                                )
                                closed_pks.append(account['_pk'])

                        jira_seadb_api.delete_users(closed_pks)
                        jira_seadb_api.update_user_names(updated_names)
                        jira_seadb_api.update_users_next_report_at(
                            [
                                account['_pk']
                                for account in batch
                                if account.get('_pk') is not None
                                and account['_pk'] not in closed_pks
                            ],
                            next_report_at,
                        )

                    logger.info(
                        'Privacy report completed for Jira project %s, %d users total',
                        project_uuid,
                        len(accounts),
                    )

                except JiraTokenRevokedError:
                    logger.warning(
                        'Jira OAuth token revoked for project %s, deleting all %d users',
                        project_uuid,
                        len(accounts),
                    )
                    jira_seadb_api.delete_users(
                        [account['_pk'] for account in accounts]
                    )
                except Exception:
                    logger.exception(
                        'Failed privacy report for Jira project %s',
                        project_uuid,
                    )

            if len(oauth_rows) < page_size:
                break
            offset += page_size

    def cancel(self):
        self.finished.set()
