import datetime
import json

from seaqa_indexer.seadb_models.models import SchemaTables
from seaqa_indexer.utils.seadb_api import SeaDBAPI


MAX_CONTENT_LENGTH = 10000


def normalize_crash_content(exceptions_json):
    """Turn the latest Crashlytics exception payload into readable stack text."""
    if not exceptions_json:
        return ''
    try:
        exceptions = json.loads(exceptions_json) if isinstance(exceptions_json, str) else exceptions_json
    except (TypeError, ValueError):
        return str(exceptions_json)[:MAX_CONTENT_LENGTH]

    lines = []

    def append_frames(frames):
        for frame in frames or []:
            if not isinstance(frame, dict):
                continue
            symbol = frame.get('symbol') or ''
            file_name = frame.get('file') or ''
            line_number = frame.get('line')
            location = file_name
            if line_number is not None:
                location = f'{location}:{line_number}' if location else str(line_number)
            if symbol or location:
                if symbol and location:
                    lines.append(f'  at {symbol} ({location})')
                else:
                    lines.append(f'  at {symbol or location}')

    def append_android_exception(exception):
        if not isinstance(exception, dict):
            return
        exception_type = exception.get('type') or 'Exception'
        message = exception.get('exception_message') or ''
        lines.append(f'{exception_type}: {message}'.rstrip(': '))
        append_frames(exception.get('frames'))

    def append_apple_error(error):
        if not isinstance(error, dict):
            return
        title = error.get('title') or 'Error'
        subtitle = error.get('subtitle') or ''
        lines.append(f'{title}: {subtitle}'.rstrip(': '))
        append_frames(error.get('frames'))

    def append_thread(thread):
        if not isinstance(thread, dict):
            return
        thread_name = thread.get('queue_name') or 'Thread'
        if thread.get('crashed') is True:
            thread_name = f'{thread_name} (crashed)'
        lines.append(thread_name)
        append_frames(thread.get('frames'))

    if isinstance(exceptions, dict) and any(
        key in exceptions for key in ('exceptions', 'error', 'threads')
    ):
        exception_values = exceptions.get('exceptions') or []
        error = exceptions.get('error')
        if isinstance(error, dict):
            append_apple_error(error)
        elif isinstance(error, list):
            for error_item in error:
                append_apple_error(error_item)
        threads = exceptions.get('threads') or []
        crashed_threads = [
            thread for thread in threads
            if isinstance(thread, dict) and thread.get('crashed') is True
        ]
        for thread in crashed_threads or threads:
            append_thread(thread)
    else:
        exception_values = exceptions

    if isinstance(exception_values, dict):
        exception_values = [exception_values]
    for exception in exception_values or []:
        append_android_exception(exception)

    return '\n'.join(lines)[:MAX_CONTENT_LENGTH]


def _isoformat(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return value.astimezone(datetime.timezone.utc).isoformat()
    return str(value)


class FirebaseCrashSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def get_crash_issues_count(self, connection_id):
        table_name = SchemaTables.FIREBASE_CRASH_ISSUES.table_name(connection_id)
        response = self.seadb_api.query_rows(
            self.base_id, f"SELECT COUNT(*) AS count FROM `{table_name}`"
        )
        rows = response.get('results', []) if response else []
        return rows[0].get('count', 0) if rows else 0

    def get_issue_by_identity(self, issue_id, source_table, connection_id):
        table_name = SchemaTables.FIREBASE_CRASH_ISSUES.table_name(connection_id)
        escaped_issue_id = str(issue_id).replace("'", "''")
        escaped_source_table = str(source_table).replace("'", "''")
        sql = f"""
            SELECT `_pk`, `issue_id`, `source_table`, `app_id`, `title`, `subtitle`, `content`, `error_type`, `platform`,
                   `bundle_identifier`, `app_version`, `is_fatal`,
                   `occurrence_count`, `affected_users`, `first_seen_time`, `last_seen_time`,
                   `created_time`
            FROM `{table_name}`
            WHERE `issue_id` = '{escaped_issue_id}' AND `source_table` = '{escaped_source_table}'
            LIMIT 1
        """
        rows = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        return rows[0] if rows else {}

    def save_or_update_issue_to_db(self, connection_id, issue, existed_issue):
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        table_name = SchemaTables.FIREBASE_CRASH_ISSUES.table_name(connection_id)
        last_seen = _isoformat(issue.get('last_seen_time'))
        first_seen = _isoformat(issue.get('first_seen_time'))
        sync_row = {
            SchemaTables.FIREBASE_CRASH_ISSUES.column.issue_id.name: str(issue.get('issue_id') or ''),
            SchemaTables.FIREBASE_CRASH_ISSUES.column.source_table.name: str(issue.get('source_table') or ''),
            SchemaTables.FIREBASE_CRASH_ISSUES.column.app_id.name: str(issue.get('app_id') or ''),
            SchemaTables.FIREBASE_CRASH_ISSUES.column.title.name: issue.get('title') or '',
            SchemaTables.FIREBASE_CRASH_ISSUES.column.subtitle.name: issue.get('subtitle') or '',
            SchemaTables.FIREBASE_CRASH_ISSUES.column.content.name: normalize_crash_content(
                issue.get('exceptions_json')
            ),
            SchemaTables.FIREBASE_CRASH_ISSUES.column.error_type.name: issue.get('error_type'),
            SchemaTables.FIREBASE_CRASH_ISSUES.column.platform.name: issue.get('platform'),
            SchemaTables.FIREBASE_CRASH_ISSUES.column.bundle_identifier.name: issue.get('bundle_identifier'),
            SchemaTables.FIREBASE_CRASH_ISSUES.column.app_version.name: issue.get('app_version'),
            SchemaTables.FIREBASE_CRASH_ISSUES.column.is_fatal.name: issue.get('is_fatal'),
            SchemaTables.FIREBASE_CRASH_ISSUES.column.occurrence_count.name: int(
                issue.get('occurrence_count') or 0
            ),
            SchemaTables.FIREBASE_CRASH_ISSUES.column.affected_users.name: int(
                issue.get('affected_users') or 0
            ),
            SchemaTables.FIREBASE_CRASH_ISSUES.column.first_seen_time.name: first_seen,
            SchemaTables.FIREBASE_CRASH_ISSUES.column.last_seen_time.name: last_seen,
            SchemaTables.FIREBASE_CRASH_ISSUES.column.modified_time.name: last_seen,
            SchemaTables.FIREBASE_CRASH_ISSUES.column.sync_time.name: now,
            SchemaTables.FIREBASE_CRASH_ISSUES.column.record_modified_time.name: now,
            SchemaTables.FIREBASE_CRASH_ISSUES.column.deleted.name: False,
        }
        if existed_issue:
            self.seadb_api.update_rows(
                self.base_id, table_name, [{'pk': existed_issue['_pk'], 'row': sync_row}]
            )
        else:
            sync_row[SchemaTables.FIREBASE_CRASH_ISSUES.column.created_time.name] = first_seen
            self.seadb_api.insert_rows(self.base_id, table_name, [sync_row])

    def get_issues_by_page(self, connection_id, last_indexed_at, page_size, offset):
        table_name = SchemaTables.FIREBASE_CRASH_ISSUES.table_name(connection_id)
        where = ''
        if last_indexed_at:
            where = f"WHERE `record_modified_time` >= '{last_indexed_at}'"
        sql = f"""
            SELECT `_pk`, `deleted`, `outdated`, `content`, `title`, `modified_time`,
                   `issue_id`, `source_table`, `app_id`, `subtitle`, `error_type`, `platform`
            FROM `{table_name}` {where}
            ORDER BY `_pk` ASC
            LIMIT {page_size} OFFSET {offset}
        """
        return self.seadb_api.query_rows(self.base_id, sql).get('results', [])

    def get_need_ai_issues_by_page(self, connection_id, page_size):
        table_name = SchemaTables.FIREBASE_CRASH_ISSUES.table_name(connection_id)
        sql = f"""
            SELECT `_pk`, `title`, `content` FROM `{table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < record_modified_time)
              AND (deleted IS NULL OR deleted = false)
              AND (outdated IS NULL OR outdated = false)
            LIMIT {page_size}
        """
        return self.seadb_api.query_rows(self.base_id, sql).get('results', [])
