import datetime
import logging
import re

import requests

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
PROJECT_ID_PATTERN = re.compile(r'^[a-z][a-z0-9-]{4,28}[a-z0-9]$')
DATASET_ID_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]{0,1023}$')
GOOGLE_TOKEN_URL = 'https://oauth2.googleapis.com/token'
FIREBASE_ANDROID_APPS_URL = 'https://firebase.googleapis.com/v1beta1/projects/{project_id}/androidApps'
FIREBASE_IOS_APPS_URL = 'https://firebase.googleapis.com/v1beta1/projects/{project_id}/iosApps'
OAUTH_TOKEN_EXPIRY_BUFFER_SECONDS = 60


class FirebaseCrashTokenRevokedError(Exception):
    pass


class FirebaseCrashOAuthError(Exception):
    pass


class FirebaseCrashAppMappingError(Exception):
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
    if response.status_code in (400, 401):
        raise FirebaseCrashTokenRevokedError()
    response.raise_for_status()

    try:
        data = response.json()
    except (TypeError, ValueError) as exc:
        raise FirebaseCrashOAuthError(
            'Firebase Crashlytics OAuth response is not valid JSON.'
        ) from exc
    if not isinstance(data, dict):
        raise FirebaseCrashOAuthError(
            'Firebase Crashlytics OAuth response must be a JSON object.'
        )
    access_token = data.get('access_token')
    if not access_token:
        raise FirebaseCrashOAuthError(
            'Firebase Crashlytics OAuth response has no access token.'
        )

    try:
        expires_in = int(data.get('expires_in') or 3600)
    except (TypeError, ValueError):
        expires_in = 3600
    expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        seconds=max(expires_in - OAUTH_TOKEN_EXPIRY_BUFFER_SECONDS, 0)
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
        if not isinstance(project_id, str) or not PROJECT_ID_PATTERN.fullmatch(project_id):
            raise ValueError('Firebase project_id is invalid')
        if not isinstance(dataset_id, str) or not DATASET_ID_PATTERN.fullmatch(dataset_id):
            raise ValueError('BigQuery dataset_id is invalid')
        if credentials is None:
            raise ValueError('Google OAuth credentials are required')

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.timeout = timeout
        query_timeout = (
            FIREBASE_CRASH_QUERY_TIMEOUT if query_timeout is None else query_timeout
        )
        try:
            self.query_timeout = int(query_timeout)
        except (TypeError, ValueError) as exc:
            raise ValueError('FIREBASE_CRASH_QUERY_TIMEOUT must be an integer.') from exc
        if self.query_timeout <= 0:
            raise ValueError('FIREBASE_CRASH_QUERY_TIMEOUT must be positive.')
        self.credentials = credentials
        self.refresh_access_token = refresh_access_token
        maximum_bytes_billed = (
            FIREBASE_CRASH_MAXIMUM_BYTES_BILLED
            if maximum_bytes_billed is None else maximum_bytes_billed
        )
        try:
            self.maximum_bytes_billed = int(maximum_bytes_billed or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                'FIREBASE_CRASH_MAXIMUM_BYTES_BILLED must be an integer.'
            ) from exc
        if self.maximum_bytes_billed < 0:
            raise ValueError('FIREBASE_CRASH_MAXIMUM_BYTES_BILLED cannot be negative.')

        if client is not None:
            self.client = client
            self.bigquery = bigquery_module
            self._google_exceptions = ()
            return

        try:
            from google.api_core import exceptions as google_exceptions
            from google.cloud import bigquery
        except ImportError as exc:
            raise RuntimeError(
                'google-cloud-bigquery is required for Firebase Crashlytics connections'
            ) from exc

        self.client = bigquery.Client(project=project_id, credentials=credentials)
        self.bigquery = bigquery
        self._google_exceptions = (
            google_exceptions.Unauthorized,
            google_exceptions.Forbidden,
            google_exceptions.NotFound,
        )

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

    def _translate_error(self, exc):
        if not self._google_exceptions or not isinstance(exc, self._google_exceptions):
            raise exc
        name = exc.__class__.__name__.lower()
        if 'unauthorized' in name:
            raise Exception('401', str(exc)) from exc
        if 'forbidden' in name:
            raise Exception('403', str(exc)) from exc
        raise Exception('404', {'error_msg': str(exc)}) from exc

    def list_app_tables(self):
        try:
            tables = list(self.client.list_tables(self._dataset_ref()))
        except Exception as exc:
            self._translate_error(exc)

        table_ids = sorted(
            table.table_id for table in tables
            if (
                getattr(table, 'table_type', None) in (None, 'TABLE')
                and APP_TABLE_PATTERN.fullmatch(table.table_id or '')
            )
        )
        return table_ids

    def _list_firebase_apps(self, platform):
        access_token = getattr(self.credentials, 'token', None)
        if not access_token:
            raise FirebaseCrashAppMappingError(
                'Firebase Crashlytics Google OAuth access token is unavailable.'
            )

        urls = {
            'ANDROID': FIREBASE_ANDROID_APPS_URL,
            'IOS': FIREBASE_IOS_APPS_URL,
        }
        url = urls[platform].format(project_id=self.project_id)
        apps = []
        page_token = None
        visited_page_tokens = set()
        while True:
            if page_token:
                if page_token in visited_page_tokens:
                    raise FirebaseCrashAppMappingError(
                        'Firebase application API returned a repeated page token.'
                    )
                visited_page_tokens.add(page_token)
            params = {'pageSize': 1000}
            if page_token:
                params['pageToken'] = page_token
            retried = False
            while True:
                try:
                    response = requests.get(
                        url,
                        headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'},
                        params=params,
                        timeout=self.timeout,
                    )
                except requests.RequestException as exc:
                    raise FirebaseCrashAppMappingError(
                        'Failed to load Firebase application identifiers.'
                    ) from exc

                if response.status_code != 401 or retried or not self.refresh_access_token:
                    break
                try:
                    refreshed_tokens = self.refresh_access_token()
                except FirebaseCrashTokenRevokedError:
                    raise FirebaseCrashAppMappingError(
                        'Firebase Crashlytics Google OAuth authorization expired. Please reconnect Google.'
                    )
                except Exception as exc:
                    raise FirebaseCrashAppMappingError(
                        'Failed to refresh Firebase Crashlytics Google OAuth authorization.'
                    ) from exc
                retried = True
                if isinstance(refreshed_tokens, dict):
                    access_token = refreshed_tokens.get('access_token')
                    if hasattr(self.credentials, 'refresh_token'):
                        self.credentials.refresh_token = refreshed_tokens.get(
                            'refresh_token',
                            getattr(self.credentials, 'refresh_token', None),
                        )
                    if hasattr(self.credentials, 'expiry'):
                        self.credentials.expiry = refreshed_tokens.get(
                            'expires_at',
                            getattr(self.credentials, 'expiry', None),
                        )
                else:
                    access_token = refreshed_tokens
                if not access_token:
                    raise FirebaseCrashAppMappingError(
                        'Failed to refresh Firebase Crashlytics Google OAuth authorization.'
                    )
                if hasattr(self.credentials, 'token'):
                    self.credentials.token = access_token

            if response.status_code == 401:
                raise FirebaseCrashAppMappingError(
                    'Firebase Crashlytics Google OAuth authorization expired. Please reconnect Google.'
                )
            if response.status_code == 403:
                raise FirebaseCrashAppMappingError(
                    'The Google account does not have permission to list Firebase applications.'
                )
            if response.status_code != 200:
                raise FirebaseCrashAppMappingError(
                    'Failed to load Firebase application identifiers.'
                )

            try:
                data = response.json()
            except (TypeError, ValueError) as exc:
                raise FirebaseCrashAppMappingError(
                    'Firebase application API returned an invalid JSON response.'
                ) from exc
            if not isinstance(data, dict):
                raise FirebaseCrashAppMappingError(
                    'Firebase application API response must be a JSON object.'
                )
            apps.extend(data.get('apps') or [])
            page_token = data.get('nextPageToken')
            if not page_token:
                break
        return apps

    def get_app_ids_by_table(self):
        """Map Crashlytics BigQuery app tables to Firebase console application IDs."""
        app_ids_by_table = {}
        for platform, identifier_key in (('ANDROID', 'packageName'), ('IOS', 'bundleId')):
            for app in self._list_firebase_apps(platform):
                app_id = app.get('appId')
                identifier = app.get(identifier_key)
                table_id = self._table_id_for_app(identifier, platform)
                if app_id and table_id:
                    app_ids_by_table[table_id] = app_id
        return app_ids_by_table

    def _source_select(self, table_id):
        if not APP_TABLE_PATTERN.fullmatch(table_id or ''):
            raise ValueError(f'Invalid Crashlytics BigQuery table name: {table_id}')
        table_ref = f'{self.project_id}.{self.dataset_id}.{table_id}'
        try:
            table = self.client.get_table(table_ref)
        except Exception as exc:
            self._translate_error(exc)

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
        bundle_identifier = self._pick_field(fields, 'bundle_identifier')
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
        platform = self._pick_field(fields, 'platform')
        affected_user_id = (
            f"COALESCE(NULLIF({self._sql_string(user_id)}, ''), "
            f"NULLIF({self._sql_string(installation_uuid)}, ''))"
        )

        app_version = (
            f'COALESCE({self._sql_string(display_version)}, {self._sql_string(build_version)})'
        )
        # Crashlytics deprecated the source is_fatal field. Derive the
        # product-level fatal flag from the documented event type instead.
        is_fatal_expression = (
            f"COALESCE(UPPER({self._sql_string(error_type)}) = 'FATAL', FALSE)"
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
        return f"""
            SELECT
              '{table_id}' AS source_table,
              CAST(NULL AS STRING) AS app_id,
              {event_id_expression} AS event_id,
              {issue_id_expression} AS issue_id,
              {title} AS title,
              {subtitle} AS subtitle,
              {crash_error_type} AS error_type,
              {self._sql_string(platform)} AS platform,
              {self._sql_string(bundle_identifier)} AS bundle_identifier,
              {app_version} AS app_version,
              {is_fatal_expression} AS is_fatal,
              {payload_json} AS exceptions_json,
              {event_timestamp_expression} AS event_timestamp,
              {affected_user_id} AS affected_user_id
            FROM `{table_ref}`
            WHERE NULLIF(TRIM({event_id_expression}), '') IS NOT NULL
              AND NULLIF(TRIM({issue_id_expression}), '') IS NOT NULL
              AND {event_timestamp_expression} IS NOT NULL
        """

    def build_issues_query(self, table_names, since=None):
        if not table_names:
            return None
        source_queries = [self._source_select(table_name) for table_name in table_names]
        source_events = '\nUNION ALL\n'.join(source_queries)
        having_clause = 'HAVING MAX(event_timestamp) >= @since' if since else ''

        return f"""
            WITH source_events AS (
              {source_events}
            )
            SELECT
              source_table,
              ANY_VALUE(app_id) AS app_id,
              issue_id,
              COALESCE(
                ARRAY_AGG(title IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)],
                issue_id
              ) AS title,
              ARRAY_AGG(subtitle IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS subtitle,
              ARRAY_AGG(error_type IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS error_type,
              ARRAY_AGG(platform IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS platform,
              ARRAY_AGG(bundle_identifier IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS bundle_identifier,
              ARRAY_AGG(app_version IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS app_version,
              ARRAY_AGG(is_fatal IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS is_fatal,
              ARRAY_AGG(exceptions_json IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS exceptions_json,
              MIN(event_timestamp) AS first_seen_time,
              MAX(event_timestamp) AS last_seen_time,
              COUNT(DISTINCT event_id) AS occurrence_count,
              COUNT(DISTINCT affected_user_id) AS affected_users
            FROM source_events
            GROUP BY source_table, issue_id
            {having_clause}
            ORDER BY last_seen_time DESC, source_table ASC, issue_id ASC
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
        try:
            dataset = self.client.get_dataset(self._dataset_ref())
            query_kwargs = {'job_config': job_config}
            dataset_location = getattr(dataset, 'location', None)
            if dataset_location:
                query_kwargs['location'] = dataset_location
            query_job = self.client.query(query, **query_kwargs)
            result = query_job.result(
                page_size=page_size, timeout=self.query_timeout
            )
        except Exception as exc:
            self._translate_error(exc)

        bytes_processed = getattr(query_job, 'total_bytes_processed', None)
        if bytes_processed is not None:
            logger.info(
                'Firebase Crashlytics BigQuery query processed %s bytes for %s.%s',
                bytes_processed,
                self.project_id,
                self.dataset_id,
            )
        return [dict(row.items()) for row in result]
