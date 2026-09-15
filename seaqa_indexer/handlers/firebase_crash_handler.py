import datetime
import json
from urllib.parse import quote

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.index.index_store.firebase_crash_index import FirebaseCrashIndex
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.seadb_models.firebase_crash_seadb_api import (
    FirebaseCrashSeaDBAPI,
)
from seaqa_indexer.seadb_models.models import SchemaTables
from seaqa_indexer.utils import need_update_connection_last_sync_count, time_to_utc_iso, time_to_utc_time
from seaqa_indexer.utils.constants import ConnectionType, IndexStatus
from seaqa_indexer.utils.firebase_crash_api import (
    FIREBASE_CRASHLYTICS_DATASET_ID,
    FirebaseCrashAPI,
    FirebaseCrashTokenRevokedError,
    refresh_google_access_token,
)


class FirebaseCrashHandler(ConnectionHandler):
    connection_type = ConnectionType.FIREBASE_CRASH.value
    BATCH_SIZE = 50
    # Crashlytics supports manually scheduled BigQuery backfills for the
    # previous 30 days. Keep history in SeaQA, but reconsider issues whose
    # latest exported event falls within that backfill window.
    INCREMENTAL_LOOKBACK = datetime.timedelta(days=30)

    @classmethod
    def _issue_changed(cls, existed, issue):
        if time_to_utc_iso(issue.get('last_seen_time')) != time_to_utc_iso(existed.get('last_seen_time')):
            return True

        issue_first = time_to_utc_iso(issue.get('first_seen_time'))
        existed_first = time_to_utc_iso(existed.get('first_seen_time'))
        if issue_first and existed_first and issue_first < existed_first:
            return True

        return False

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

    def format_vector_search_result(self, record, connection, result):
        config = json.loads(connection.config)
        return {
            'title': record.get('title', ''),
            'error_type': record.get('error_type', ''),
            'platform': config.get('platform', ''),
            'url': self._issue_url(
                config.get('project_id'), config.get('app_id'), record.get('issue_id')
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
        app_id = config.get('app_id')
        bundle_identifier = config.get('bundle_identifier')
        platform = config.get('platform')
        if not project_id or not app_id or not bundle_identifier or not platform:
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

        # google-auth 的 Credentials.expired 用 offset-naive 的 utcnow() 和 expiry
        # 比较，aware expiry 会触发 "can't compare offset-naive and offset-aware
        # datetimes"。这里把 expiry 转成 naive UTC 以匹配。
        credentials = Credentials(
            token=access_token,
            expiry=expires_at.replace(tzinfo=None),
        )

        try:
            firebase_api = FirebaseCrashAPI(
                project_id,
                credentials,
                FIREBASE_CRASHLYTICS_DATASET_ID,
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
        table_id = FirebaseCrashAPI._table_id_for_app(bundle_identifier, platform)
        if table_id and table_id in table_names:
            table_names = [table_id]
        else:
            sync_logger.log('The selected Firebase application has no Crashlytics export table.')
            return None, None, None, None

        known_tables = connection_status.get('firebase_crash_known_tables')
        is_first_sync = (
            not old_last_sync_time
            or not isinstance(known_tables, list)
            or table_id not in known_tables
        )
        since = None
        if not is_first_sync:
            since_time = time_to_utc_time(str(old_last_sync_time)) - self.INCREMENTAL_LOOKBACK
            since = since_time.isoformat()
        try:
            issues = firebase_api.list_crash_issues(since=since, table_names=table_names)
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
        for issue in issues:
            existed = seadb_api.get_issue_by_identity(
                issue.get('issue_id'), connection_id
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
