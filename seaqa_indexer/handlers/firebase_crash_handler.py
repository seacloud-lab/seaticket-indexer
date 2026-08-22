import datetime
import json
from urllib.parse import quote

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.index.index_store.firebase_crash_index import FirebaseCrashIndex
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.seadb_models.firebase_crash_seadb_api import (
    FirebaseCrashSeaDBAPI,
    normalize_crash_content,
)
from seaqa_indexer.seadb_models.models import SchemaTables
from seaqa_indexer.utils import need_update_connection_last_sync_count, time_to_utc_time
from seaqa_indexer.config import FIREBASE_CRASH_CLIENT_ID, FIREBASE_CRASH_CLIENT_SECRET
from seaqa_indexer.utils.constants import ConnectionType, IndexStatus
from seaqa_indexer.utils.firebase_crash_api import (
    FirebaseCrashAPI,
    FirebaseCrashAppMappingError,
    FirebaseCrashTokenRevokedError,
    GOOGLE_TOKEN_URL,
    refresh_google_access_token,
)


class FirebaseCrashHandler(ConnectionHandler):
    connection_type = ConnectionType.FIREBASE_CRASH.value
    BATCH_SIZE = 50
    # Crashlytics supports manually scheduled BigQuery backfills for the
    # previous 30 days. Keep history in SeaQA, but reconsider issues whose
    # latest exported event falls within that backfill window.
    INCREMENTAL_LOOKBACK = datetime.timedelta(days=30)

    @staticmethod
    def _normalize_datetime(value):
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            parsed = value
        else:
            try:
                parsed = datetime.datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            except (TypeError, ValueError):
                return str(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed.astimezone(datetime.timezone.utc).isoformat()

    @staticmethod
    def _normalize_bool(value):
        if isinstance(value, str):
            return value.strip().lower() in ('1', 'true', 'yes', 'y')
        return bool(value)

    @classmethod
    def _issue_changed(cls, existed, issue):
        for field in (
            'app_id',
            'title',
            'subtitle',
            'error_type',
            'platform',
            'bundle_identifier',
            'app_version',
        ):
            if (existed.get(field) or '') != (issue.get(field) or ''):
                return True

        if cls._normalize_bool(existed.get('is_fatal')) != cls._normalize_bool(issue.get('is_fatal')):
            return True

        for field in ('occurrence_count', 'affected_users'):
            try:
                existed_value = int(existed.get(field) or 0)
            except (TypeError, ValueError):
                existed_value = 0
            try:
                issue_value = int(issue.get(field) or 0)
            except (TypeError, ValueError):
                issue_value = 0
            if existed_value != issue_value:
                return True

        for field in ('first_seen_time', 'last_seen_time'):
            if cls._normalize_datetime(existed.get(field)) != cls._normalize_datetime(issue.get(field)):
                return True

        return (existed.get('content') or '') != normalize_crash_content(issue.get('exceptions_json'))

    def gen_table_name(self, connection_id):
        return SchemaTables.FIREBASE_CRASH_ISSUES.table_name(connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        return FirebaseCrashIndex(seasearch_api).get_search_param(
            index_name, keyword, start, size, time_from, time_to
        )

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        FirebaseCrashIndex(seasearch_api).delete_index_by_index_name(
            gen_index_name_by_type(connection_id, self.connection_type)
        )
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

    @staticmethod
    def _issue_url(project_id, app_id, issue_id):
        if not project_id or not app_id or not issue_id:
            return ''
        return (
            'https://console.firebase.google.com/project/'
            f'{quote(str(project_id), safe="")}/crashlytics/app/'
            f'{quote(str(app_id), safe="")}/issues/{quote(str(issue_id), safe="")}'
        )

    @staticmethod
    def _map_issue_app_ids(issues, app_ids_by_table):
        for issue in issues:
            source_table = issue.get('source_table')
            issue['app_id'] = app_ids_by_table.get(source_table, '')
        return issues

    @staticmethod
    def _split_tables_for_sync(table_names, old_last_sync_time, connection_status):
        current_tables = list(dict.fromkeys(table_names))
        known_tables = connection_status.get('firebase_crash_known_tables')
        if not old_last_sync_time or not isinstance(known_tables, list):
            return current_tables, []

        known_tables = {
            table_name for table_name in known_tables if isinstance(table_name, str)
        }
        return (
            [table_name for table_name in current_tables if table_name not in known_tables],
            [table_name for table_name in current_tables if table_name in known_tables],
        )

    def format_vector_search_result(self, record, connection, result):
        config = json.loads(connection.config)
        return {
            'title': record.get('title', ''),
            'error_type': record.get('error_type', ''),
            'platform': record.get('platform', ''),
            'url': self._issue_url(
                config.get('project_id'), record.get('app_id'), record.get('issue_id')
            ),
        }

    def update_index(self, project_connection, seasearch_api, seaqa_db, since):
        connection_status = json.loads(project_connection.status)
        connection_status['last_index_status'] = IndexStatus.INDEXING
        seaqa_db.update_connection_status(project_connection.id, json.dumps(connection_status))

        index_name = gen_index_name_by_type(project_connection.id, project_connection.type)
        index = FirebaseCrashIndex(seasearch_api)
        index.create_index_if_missing(index_name)
        count = index.update(
            index_name,
            project_connection.id,
            connection_status,
            seaqa_db,
            FirebaseCrashSeaDBAPI(project_connection.project_uuid),
            since,
        )
        connection_status['last_indexed_count'] = count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        seaqa_db.update_connection(
            project_connection.id,
            datetime.datetime.now(datetime.timezone.utc),
            json.dumps(connection_status),
        )

    def sync(self, connection_id, project_uuid, config, old_last_sync_time,
             connection_status, seaqa_db, sync_logger):
        project_id = config.get('project_id')
        dataset_id = config.get('dataset_id')
        if not project_id or not dataset_id:
            sync_logger.log('Firebase Crashlytics configuration is invalid')
            return None, None, None, None

        oauth = seaqa_db.get_project_connection_oauth(project_uuid, ConnectionType.FIREBASE_CRASH.value)
        if not oauth:
            sync_logger.log('Firebase Crashlytics Google OAuth authorization not found')
            return None, None, None, None

        access_token = oauth.access_token
        refresh_token = oauth.refresh_token
        expires_at = oauth.expires_at
        if expires_at:
            expires_at = time_to_utc_time(str(expires_at))
        if not expires_at or expires_at <= datetime.datetime.now(datetime.timezone.utc):
            try:
                new_tokens = refresh_google_access_token(refresh_token)
            except FirebaseCrashTokenRevokedError:
                sync_logger.log(
                    'Firebase Crashlytics Google authorization expired. Please reconnect Google.'
                )
                return None, None, None, None
            except Exception:
                sync_logger.log('Failed to refresh Firebase Crashlytics Google authorization')
                return None, None, None, None
            access_token = new_tokens['access_token']
            refresh_token = new_tokens['refresh_token']
            expires_at = new_tokens['expires_at']
            seaqa_db.update_project_connection_oauth(
                project_uuid,
                ConnectionType.FIREBASE_CRASH.value,
                access_token,
                expires_at,
                refresh_token,
            )

        try:
            from google.oauth2.credentials import Credentials
        except ImportError:
            sync_logger.log('google-cloud-bigquery is required for Firebase Crashlytics connections')
            return None, None, None, None

        def refresh_credentials():
            nonlocal access_token, refresh_token, expires_at
            new_tokens = refresh_google_access_token(refresh_token)
            access_token = new_tokens['access_token']
            refresh_token = new_tokens['refresh_token']
            expires_at = new_tokens['expires_at']
            seaqa_db.update_project_connection_oauth(
                project_uuid,
                ConnectionType.FIREBASE_CRASH.value,
                access_token,
                expires_at,
                refresh_token,
            )
            return new_tokens

        class PersistentFirebaseCrashCredentials(Credentials):
            def __init__(self, *args, refresh_callback=None, **kwargs):
                super().__init__(*args, **kwargs)
                self._refresh_callback = refresh_callback

            def refresh(self, request):
                new_tokens = self._refresh_callback()
                self.token = new_tokens['access_token']
                self.refresh_token = new_tokens['refresh_token']
                self.expiry = new_tokens['expires_at']

        credentials = PersistentFirebaseCrashCredentials(
            token=access_token,
            refresh_token=refresh_token,
            token_uri=GOOGLE_TOKEN_URL,
            client_id=FIREBASE_CRASH_CLIENT_ID,
            client_secret=FIREBASE_CRASH_CLIENT_SECRET,
            expiry=expires_at,
            refresh_callback=refresh_credentials,
        )

        try:
            firebase_api = FirebaseCrashAPI(
                project_id,
                credentials,
                dataset_id,
                refresh_access_token=refresh_credentials,
            )
            table_names = firebase_api.list_app_tables()
        except Exception as exc:
            sync_logger.log(f'Failed to load Firebase Crashlytics BigQuery tables: {exc}')
            return None, None, None, None

        if not table_names:
            sync_logger.log(
                'The selected BigQuery dataset does not contain Firebase Crashlytics export tables.'
            )
            return None, None, None, None
        try:
            app_ids_by_table = firebase_api.get_app_ids_by_table()
        except FirebaseCrashAppMappingError as exc:
            # Table exports remain usable when Firebase Management API access
            # is unavailable. Only console deep links require this mapping.
            sync_logger.log(f'{exc} Crashlytics console links will be unavailable.')
            app_ids_by_table = {}
        unmapped_tables = sorted(set(table_names) - set(app_ids_by_table))
        if unmapped_tables:
            sync_logger.log(
                'Crashlytics tables without a current Firebase application ID '
                'will sync without console links: '
                + ', '.join(unmapped_tables)
            )

        full_sync_tables, incremental_sync_tables = self._split_tables_for_sync(
            table_names, old_last_sync_time, connection_status
        )
        since = None
        if old_last_sync_time:
            since_time = time_to_utc_time(str(old_last_sync_time)) - self.INCREMENTAL_LOOKBACK
            since = since_time.isoformat()
        try:
            issues = []
            if full_sync_tables:
                issues.extend(
                    firebase_api.list_crash_issues(
                        since=None, table_names=full_sync_tables
                    )
                )
            if incremental_sync_tables:
                issues.extend(
                    firebase_api.list_crash_issues(
                        since=since, table_names=incremental_sync_tables
                    )
                )
        except Exception as exc:
            sync_logger.log(f'Failed to query Firebase Crashlytics BigQuery export: {exc}')
            return None, None, None, None
        connection_status['firebase_crash_known_tables'] = sorted(table_names)
        seadb_api = FirebaseCrashSeaDBAPI(project_uuid)

        sync_count = 0
        added_count = 0
        updated_count = 0
        status_time = datetime.datetime.now()
        status_count = 0
        for issue in self._map_issue_app_ids(issues, app_ids_by_table):
            existed = seadb_api.get_issue_by_identity(
                issue.get('issue_id'), issue.get('source_table'), connection_id
            )
            if existed and not self._issue_changed(existed, issue):
                continue
            seadb_api.save_or_update_issue_to_db(connection_id, issue, existed)
            sync_count += 1
            if existed:
                updated_count += 1
            else:
                added_count += 1

            should, status_time, status_count = need_update_connection_last_sync_count(
                sync_count, status_time, status_count
            )
            if should:
                connection_status['last_sync_count'] = sync_count
                seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

        connection_status['last_sync_changes'] = {
            'added': added_count,
            'updated': updated_count,
            'deleted': 0,
        }
        return sync_count, seadb_api.get_crash_issues_count(connection_id), False, config

    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        api = FirebaseCrashSeaDBAPI(project_uuid)
        table_name = self.gen_table_name(connection_id)
        updates = []
        processed = 0
        while True:
            rows = api.get_need_ai_issues_by_page(connection_id, 100)
            if not rows:
                break
            for row in rows:
                content = f"Title: {row.get('title', '')}\n\nStack trace: {row.get('content', '')}"
                summary = self.seaqa_ai_api.generate_summary(
                    content, self.connection_type, project_uuid
                )
                updates.append({
                    'pk': row['_pk'],
                    'row': {
                        'ai_summary': summary,
                        'ai_summary_vector': self.seaqa_ai_api.generate_embedding(summary, project_uuid),
                        'ai_processed_time': datetime.datetime.now(
                            datetime.timezone.utc
                        ).isoformat(),
                    },
                })
                processed += 1
                if len(updates) >= self.BATCH_SIZE:
                    seadb_api.update_rows(project_uuid, table_name, updates)
                    updates = []
            if updates:
                seadb_api.update_rows(project_uuid, table_name, updates)
                updates = []
        return processed
