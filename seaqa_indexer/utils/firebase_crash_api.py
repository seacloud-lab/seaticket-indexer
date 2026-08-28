import datetime
import logging
import re

import requests
from google.cloud import bigquery

from seaqa_indexer.config import (
    FIREBASE_CRASH_CLIENT_ID,
    FIREBASE_CRASH_CLIENT_SECRET,
    FIREBASE_CRASH_MAXIMUM_BYTES_BILLED,
    FIREBASE_CRASH_QUERY_TIMEOUT,
)


logger = logging.getLogger(__name__)

# Firebase derives export table names from the app's package or bundle identifier,
# replacing periods with underscores. Bundle identifiers may also contain hyphens.
APP_TABLE_PATTERN = re.compile(r'^[A-Za-z0-9_-]+_(ANDROID|IOS)$')
FIREBASE_CRASHLYTICS_DATASET_ID = 'firebase_crashlytics'
GOOGLE_TOKEN_URL = 'https://oauth2.googleapis.com/token'
OAUTH_TOKEN_EXPIRY_BUFFER_SECONDS = 60


class FirebaseCrashTokenRevokedError(Exception):
    pass


def refresh_google_access_token(refresh_token, timeout=60):
    if not all([FIREBASE_CRASH_CLIENT_ID, FIREBASE_CRASH_CLIENT_SECRET, refresh_token]):
        raise RuntimeError('Firebase Crashlytics OAuth settings are invalid.')

    response = requests.post(
        GOOGLE_TOKEN_URL,
        data={
            'grant_type': 'refresh_token',
            'client_id': FIREBASE_CRASH_CLIENT_ID,
            'client_secret': FIREBASE_CRASH_CLIENT_SECRET,
            'refresh_token': refresh_token,
        },
        timeout=timeout,
    )
    if response.status_code == 401:
        raise FirebaseCrashTokenRevokedError()
    response.raise_for_status()

    data = response.json()
    access_token = data.get('access_token')
    expires_in = data.get('expires_in') or 3600
    expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        seconds=max(int(expires_in) - OAUTH_TOKEN_EXPIRY_BUFFER_SECONDS, 0)
    )

    return {
        'access_token': access_token,
        'refresh_token': data.get('refresh_token') or refresh_token,
        'expires_at': expires_at,
    }


class FirebaseCrashAPI:
    """Read Crashlytics BigQuery exports as aggregated crash issues."""

    def __init__(self, project_id, credentials, dataset_id,
                 timeout=60, client=None, bigquery_module=None, maximum_bytes_billed=None,
                 refresh_access_token=None, query_timeout=None):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.timeout = timeout
        self.query_timeout = int(
            FIREBASE_CRASH_QUERY_TIMEOUT if query_timeout is None else query_timeout
        )
        self.credentials = credentials
        self.refresh_access_token = refresh_access_token
        self.maximum_bytes_billed = int(
            (FIREBASE_CRASH_MAXIMUM_BYTES_BILLED if maximum_bytes_billed is None
             else maximum_bytes_billed) or 0
        )

        if client is not None:
            self.client = client
            self.bigquery = bigquery_module
            return

        self.client = bigquery.Client(project=project_id, credentials=credentials)
        self.bigquery = bigquery

    def _dataset_ref(self):
        return f'{self.project_id}.{self.dataset_id}'

    @staticmethod
    def _field_paths(fields, prefix=''):
        paths = set()
        for field in fields or []:
            path = f'{prefix}.{field.name}' if prefix else field.name
            paths.add(path)
            paths.update(FirebaseCrashAPI._field_paths(getattr(field, 'fields', ()), path))
        return paths

    @staticmethod
    def _pick_field(field_paths, *candidates):
        return next((field for field in candidates if field in field_paths), None)

    @staticmethod
    def _sql_string(field_name):
        return f'CAST({field_name} AS STRING)' if field_name else 'CAST(NULL AS STRING)'

    @staticmethod
    def _sql_json_value(json_expression, *paths):
        if not json_expression:
            return 'CAST(NULL AS STRING)'
        values = [f"JSON_VALUE({json_expression}, '{path}')" for path in paths]
        return f"COALESCE({', '.join(values)})" if len(values) > 1 else values[0]

    @staticmethod
    def _table_id_for_app(identifier, platform):
        normalized_identifier = (identifier or '').replace('.', '_')
        return f'{normalized_identifier}_{platform}' if normalized_identifier else ''

    def list_app_tables(self):
        tables = list(self.client.list_tables(self._dataset_ref()))

        table_ids = sorted(
            table.table_id for table in tables
            if (
                getattr(table, 'table_type', None) in (None, 'TABLE')
                and APP_TABLE_PATTERN.fullmatch(table.table_id or '')
            )
        )
        return table_ids

    def _source_select(self, table_id, since=None):
        if not APP_TABLE_PATTERN.fullmatch(table_id or ''):
            raise ValueError(f'Invalid Crashlytics BigQuery table name: {table_id}')
        table_ref = f'{self.project_id}.{self.dataset_id}.{table_id}'
        table = self.client.get_table(table_ref)

        table_type = getattr(table, 'table_type', None)
        if table_type not in (None, 'TABLE'):
            raise ValueError(f'{table_id} is not a valid Crashlytics export table')

        fields = self._field_paths(getattr(table, 'schema', ()))
        event_id = self._pick_field(fields, 'event_id')
        issue_id = self._pick_field(fields, 'issue_id')
        timestamp = self._pick_field(fields, 'event_timestamp')
        # These are documented root-level Crashlytics export fields. The
        # query must fail fast when the selected table is not a valid export.
        error_type = self._pick_field(fields, 'error_type')
        if not event_id or not issue_id or not timestamp or not error_type:
            raise ValueError(f'{table_id} is not a valid Crashlytics export table')

        is_android = table_id.endswith('_ANDROID')
        # Crashlytics defines error_type as the root-level event type
        # (for example FATAL, NON_FATAL, or ANR). Do not substitute the
        # exception type or error title for this field.
        build_version = self._pick_field(
            fields,
            'application.build_version',
        )
        display_version = self._pick_field(
            fields,
            'application.display_version',
        )
        exceptions = self._pick_field(fields, 'exceptions')
        error = self._pick_field(fields, 'error')
        threads = self._pick_field(fields, 'threads')
        installation_uuid = self._pick_field(fields, 'installation_uuid')
        user_id = self._pick_field(fields, 'user.id')
        affected_user_id = (
            f"COALESCE(NULLIF({self._sql_string(user_id)}, ''), "
            f"NULLIF({self._sql_string(installation_uuid)}, ''))"
        )

        app_version = (
            f'COALESCE({self._sql_string(display_version)}, {self._sql_string(build_version)})'
        )
        if is_android:
            payload_fields = []
            payload_presence = []
            if exceptions:
                payload_fields.append(f'{exceptions} AS exceptions')
                payload_presence.append(f'ARRAY_LENGTH({exceptions}) > 0')
            if threads:
                payload_fields.append(f'{threads} AS threads')
                payload_presence.append(f'ARRAY_LENGTH({threads}) > 0')
            exceptions_json = (
                f"CASE WHEN {' OR '.join(payload_presence)} "
                f"THEN TO_JSON_STRING(STRUCT({', '.join(payload_fields)})) "
                f"ELSE CAST(NULL AS STRING) END"
                if payload_presence else None
            )
            exception_values = (
                f'JSON_QUERY_ARRAY({exceptions_json}, "$.exceptions")'
                if exceptions_json and exceptions else None
            )
            if exception_values:
                primary_exception = f'{exception_values}[SAFE_OFFSET(0)]'
                title = (
                    f"COALESCE({self._sql_json_value(primary_exception, '$.title')}, "
                    f"{self._sql_json_value(primary_exception, '$.type')}, "
                    f'{self._sql_string(error_type)})'
                )
                subtitle = (
                    f"COALESCE({self._sql_json_value(primary_exception, '$.subtitle')}, "
                    f"{self._sql_json_value(primary_exception, '$.exception_message')})"
                )
            else:
                title = self._sql_string(error_type)
                subtitle = 'CAST(NULL AS STRING)'
            payload_json = exceptions_json or 'CAST(NULL AS STRING)'
            crash_error_type = self._sql_string(error_type)
        else:
            error_json = f'TO_JSON_STRING({error})' if error else None
            payload_fields = []
            payload_presence = []
            if error:
                payload_fields.append(f'{error} AS error')
                payload_presence.append(f'ARRAY_LENGTH({error}) > 0')
            if threads:
                payload_fields.append(f'{threads} AS threads')
                payload_presence.append(f'ARRAY_LENGTH({threads}) > 0')
            payload_json = (
                f"CASE WHEN {' OR '.join(payload_presence)} "
                f"THEN TO_JSON_STRING(STRUCT({', '.join(payload_fields)})) "
                f"ELSE CAST(NULL AS STRING) END"
                if payload_presence else 'CAST(NULL AS STRING)'
            )
            title = (
                f"COALESCE({self._sql_json_value(error_json, '$[0].title')}, "
                f'{self._sql_string(error_type)})'
            )
            subtitle = self._sql_json_value(error_json, '$[0].subtitle')
            crash_error_type = self._sql_string(error_type)

        event_id_expression = self._sql_string(event_id)
        issue_id_expression = self._sql_string(issue_id)
        event_timestamp_expression = f'CAST({timestamp} AS TIMESTAMP)'
        where_clause = [
            f"NULLIF(TRIM({event_id_expression}), '') IS NOT NULL",
            f"NULLIF(TRIM({issue_id_expression}), '') IS NOT NULL",
            f"{event_timestamp_expression} IS NOT NULL",
        ]
        if since:
            where_clause.append(f"{event_timestamp_expression} >= @since")
        where = '\n              AND '.join(where_clause)
        return f"""
            SELECT
              {event_id_expression} AS event_id,
              {issue_id_expression} AS issue_id,
              {title} AS title,
              {subtitle} AS subtitle,
              {crash_error_type} AS error_type,
              {app_version} AS app_version,
              {payload_json} AS exceptions_json,
              {event_timestamp_expression} AS event_timestamp,
              {affected_user_id} AS affected_user_id
            FROM `{table_ref}`
            WHERE {where}
        """

    def build_issues_query(self, table_names, since=None):
        if not table_names:
            return None
        source_queries = [self._source_select(table_name, since) for table_name in table_names]
        source_events = '\nUNION ALL\n'.join(source_queries)

        return f"""
            WITH source_events AS (
              {source_events}
            )
            SELECT
              issue_id,
              COALESCE(
                ARRAY_AGG(title IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)],
                issue_id
              ) AS title,
              ARRAY_AGG(subtitle IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS subtitle,
              ARRAY_AGG(error_type IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS error_type,
              ARRAY_AGG(app_version IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS app_version,
              ARRAY_AGG(exceptions_json IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS exceptions_json,
              MIN(event_timestamp) AS first_seen_time,
              MAX(event_timestamp) AS last_seen_time,
              COUNT(DISTINCT event_id) AS occurrence_count,
              COUNT(DISTINCT affected_user_id) AS affected_users
            FROM source_events
            GROUP BY issue_id
            ORDER BY last_seen_time DESC, issue_id ASC
        """

    def list_crash_issues(self, since=None, table_names=None, page_size=1000):
        table_names = table_names or self.list_app_tables()
        query = self.build_issues_query(table_names, since)
        if not query:
            return []

        if not self.bigquery:
            raise RuntimeError('BigQuery query configuration is unavailable')
        job_config_kwargs = {'use_legacy_sql': False}
        if since:
            job_config_kwargs['query_parameters'] = [
                self.bigquery.ScalarQueryParameter('since', 'TIMESTAMP', since),
            ]
        if self.maximum_bytes_billed:
            job_config_kwargs['maximum_bytes_billed'] = self.maximum_bytes_billed
        job_config = self.bigquery.QueryJobConfig(**job_config_kwargs)
        dataset = self.client.get_dataset(self._dataset_ref())
        query_kwargs = {'job_config': job_config}
        dataset_location = getattr(dataset, 'location', None)
        if dataset_location:
            query_kwargs['location'] = dataset_location
        query_job = self.client.query(query, **query_kwargs)
        result = query_job.result(
            page_size=page_size, timeout=self.query_timeout
        )

        bytes_processed = getattr(query_job, 'total_bytes_processed', None)
        if bytes_processed is not None:
            logger.info(
                'Firebase Crashlytics BigQuery query processed %s bytes for %s.%s',
                bytes_processed,
                self.project_id,
                self.dataset_id,
            )
        return [dict(row.items()) for row in result]
