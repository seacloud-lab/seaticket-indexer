"""Backfill status_category (Jira) / state_type (Linear) for existing issue rows.

Rows synced before these columns were added have NULL values, which causes two problems:
1. The Agent cannot tell whether sibling Jira/Linear tasks are completed.
2. The first incremental sync after deployment produces a `None -> completed` diff,
   which looks like a fresh completion event.

This script resolves the mapping from the source API (Jira statuses / Linear workflow
states) and updates rows in place. It writes SeaDB directly and never publishes events.

Usage:
    python backfill_issue_state_fields.py                      # backfill jira + linear
    python backfill_issue_state_fields.py --connection-type linear
    python backfill_issue_state_fields.py --connection-id 12 --dry-run
"""

import argparse
import datetime
import json
import logging
import sys

from sqlalchemy import text

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.index.utils import init_logging
from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.utils.jira_api import JiraAPI
from seaqa_indexer.utils.linear_api import LinearAPI
from seaqa_indexer.utils.seadb_api import SeaDBAPI, TableNotFound
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.seadb_models.models import SchemaTables

logger = logging.getLogger(__name__)

CONNECTION_PAGE_SIZE = 200
ROW_PAGE_SIZE = 1000
UPDATE_BATCH_SIZE = 200


class IssueStateFieldBackfiller:

    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.seadb_api = SeaDBAPI()

    def iter_connections(self, connection_type, connection_id=None):
        start = 0
        while True:
            sql = """SELECT pc.id, pc.config, pc.project_uuid
                     FROM project_connection pc
                     INNER JOIN projects p ON p.uuid = pc.project_uuid
                     WHERE p.deleted = false AND pc.deleted = false AND pc.type = :connection_type"""
            params = {'connection_type': connection_type, 'limit': CONNECTION_PAGE_SIZE, 'start': start}
            if connection_id:
                sql += " AND pc.id = :connection_id"
                params['connection_id'] = connection_id
            sql += " ORDER BY pc.id LIMIT :limit OFFSET :start"
            with self.db_session_class() as session:
                connections = session.execute(text(sql), params).fetchall()
            for connection in connections:
                yield connection
            if len(connections) < CONNECTION_PAGE_SIZE:
                break
            start += CONNECTION_PAGE_SIZE

    def _get_jira_api(self, project_uuid):
        oauth = self.seaqa_db.get_project_connection_oauth(project_uuid, ConnectionType.JIRA_ISSUE.value)
        if not oauth:
            raise RuntimeError('Jira OAuth not found')
        access_token = oauth.access_token
        refresh_token = oauth.refresh_token
        expires_at = oauth.expires_at
        if not expires_at or time_to_utc_time(str(expires_at)) <= datetime.datetime.now(datetime.timezone.utc):
            jira_api = JiraAPI(access_token, refresh_token, expires_at=expires_at)
            new_tokens = jira_api.refresh_access_token()
            access_token = new_tokens['access_token']
            refresh_token = new_tokens['refresh_token']
            expires_at = new_tokens['expires_at']
            self.seaqa_db.update_project_connection_oauth(
                project_uuid, ConnectionType.JIRA_ISSUE.value, access_token, expires_at, refresh_token
            )
        return JiraAPI(access_token, refresh_token, expires_at=expires_at)

    def _get_linear_api(self, project_uuid):
        oauth = self.seaqa_db.get_project_connection_oauth(project_uuid, ConnectionType.LINEAR.value)
        if not oauth or not oauth.access_token:
            raise RuntimeError('Linear OAuth not found')
        return LinearAPI(access_token=oauth.access_token, refresh_token=oauth.refresh_token)

    def _update_rows(self, project_uuid, table_name, updates, dry_run):
        if dry_run:
            logger.info('[dry-run] would update %s rows in %s', len(updates), table_name)
            return
        self.seadb_api.update_rows(project_uuid, table_name, updates)

    def backfill_table(self, project_uuid, table_name, name_field, value_field, value_by_name, dry_run=False):
        """Fill value_field on rows where it is empty, using a name_field -> value mapping.

        Pages over the whole table with OFFSET; updates do not change the scanned set,
        so pagination is stable.
        """
        updated_count = 0
        unmapped_names = {}
        start = 0
        while True:
            sql = f"""SELECT `_pk`, `{name_field}`, `{value_field}` FROM `{table_name}`
                      WHERE `deleted` = false LIMIT {ROW_PAGE_SIZE} OFFSET {start}"""
            result = self.seadb_api.query_rows(project_uuid, sql)
            rows = (result or {}).get('results') or []
            if not rows:
                break

            updates = []
            for row in rows:
                if row.get(value_field):
                    continue
                name = row.get(name_field)
                value = value_by_name.get(name)
                if not value:
                    unmapped_names[name] = unmapped_names.get(name, 0) + 1
                    continue
                updates.append({'pk': row.get('_pk'), 'row': {value_field: value}})
                if len(updates) >= UPDATE_BATCH_SIZE:
                    self._update_rows(project_uuid, table_name, updates, dry_run)
                    updated_count += len(updates)
                    updates = []
            if updates:
                self._update_rows(project_uuid, table_name, updates, dry_run)
                updated_count += len(updates)

            start += ROW_PAGE_SIZE
            if len(rows) < ROW_PAGE_SIZE:
                break
        return updated_count, unmapped_names

    def backfill_jira_connection(self, connection, dry_run=False):
        connection_id = connection.id
        project_uuid = connection.project_uuid
        config = json.loads(connection.config or '{}')
        site_id = config.get('site_id')
        project_key = config.get('project_key')
        if not site_id or not project_key:
            logger.warning('jira connection %s config invalid, skip', connection_id)
            return

        jira_api = self._get_jira_api(project_uuid)
        value_by_name = {}
        for status in jira_api.get_statuses(project_key, site_id) or []:
            name = status.get('name')
            category_key = ((status.get('statusCategory') or {}).get('key') or '').strip()
            if name and category_key:
                value_by_name[name] = category_key

        table_name = SchemaTables.JIRA_ISSUES.table_name(connection_id)
        updated_count, unmapped_names = self.backfill_table(
            project_uuid, table_name, 'status', 'status_category', value_by_name, dry_run
        )
        logger.info(
            'jira connection %s: updated %s rows, unmapped statuses: %s',
            connection_id, updated_count, unmapped_names,
        )

    def backfill_linear_connection(self, connection, dry_run=False):
        connection_id = connection.id
        project_uuid = connection.project_uuid
        config = json.loads(connection.config or '{}')
        team_id = config.get('team_id')

        linear_api = self._get_linear_api(project_uuid)
        value_by_name = {}
        for state in linear_api.get_workflow_states(team_id=team_id) or []:
            name = state.get('name')
            state_type = (state.get('type') or '').strip()
            if name and state_type:
                value_by_name[name] = state_type

        table_name = SchemaTables.LINEAR_ISSUES.table_name(connection_id)
        updated_count, unmapped_names = self.backfill_table(
            project_uuid, table_name, 'state', 'state_type', value_by_name, dry_run
        )
        logger.info(
            'linear connection %s: updated %s rows, unmapped states: %s',
            connection_id, updated_count, unmapped_names,
        )

    def run(self, connection_types, connection_id=None, dry_run=False):
        for connection_type in connection_types:
            for connection in self.iter_connections(connection_type, connection_id):
                try:
                    if connection_type == ConnectionType.JIRA_ISSUE.value:
                        self.backfill_jira_connection(connection, dry_run)
                    elif connection_type == ConnectionType.LINEAR.value:
                        self.backfill_linear_connection(connection, dry_run)
                except TableNotFound:
                    logger.warning('connection %s table not found (never synced?), skip', connection.id)
                except Exception as e:
                    logger.error('connection %s backfill failed: %s', connection.id, e)


def main():
    parser = argparse.ArgumentParser(
        description='Backfill status_category (Jira) / state_type (Linear) for existing issue rows'
    )
    parser.add_argument(
        '--connection-type',
        choices=[ConnectionType.JIRA_ISSUE.value, ConnectionType.LINEAR.value],
        default=None,
        help='Only backfill this connection type (default: both)',
    )
    parser.add_argument(
        '--connection-id',
        type=int,
        default=None,
        help='Only backfill this connection id',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Only count rows, do not write',
    )
    parser.add_argument(
        '--logfile',
        default=sys.stdout,
        type=argparse.FileType('a'),
        help='Log file path (default: stdout)',
    )
    parser.add_argument(
        '--loglevel',
        default='info',
        choices=['debug', 'info', 'warning', 'error'],
        help='Logging level (default: info)',
    )
    args = parser.parse_args()

    init_logging(args)

    connection_types = (
        [args.connection_type]
        if args.connection_type
        else [ConnectionType.JIRA_ISSUE.value, ConnectionType.LINEAR.value]
    )
    backfiller = IssueStateFieldBackfiller()
    backfiller.run(connection_types, args.connection_id, args.dry_run)


if __name__ == '__main__':
    main()
