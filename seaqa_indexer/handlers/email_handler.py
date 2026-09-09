import json
import logging
import datetime

from dateutil.relativedelta import relativedelta

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.syncer.utils import upload_email_attachment, insert_and_update_thread_rows, EMAIL_SYNC_INTERVAL_DAYS
from seaqa_indexer.utils.mail_viewer import get_mail_viewer_from_config, EmailConfigError, EmailViewError, EmailAuthProviderError
from seaqa_indexer.seadb_models.email_seadb_api import EmailSeaDBAPI
from seaqa_indexer.crawler.storage import get_storage
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.utils.constants import IndexStatus, DataEventType
from seaqa_indexer.utils import need_update_connection_last_sync_count, time_to_utc_time
from seaqa_indexer import config as indexer_config
from seaqa_indexer.index.index_store.email_index import EmailIndex

from seaqa_indexer.seadb_models.models import SchemaTables


logger = logging.getLogger(__name__)

EMAIL_OAUTH_CONFIGS = {
    'Gmail': {
        'client_id': indexer_config.GOOGLE_EMAIL_CLIENT_ID,
        'client_secret': indexer_config.GOOGLE_EMAIL_CLIENT_SECRET,
        'token_url': 'https://oauth2.googleapis.com/token',
        'scopes': {
            'personal': [
                'https://www.googleapis.com/auth/gmail.modify',
                'https://www.googleapis.com/auth/gmail.send',
                'https://www.googleapis.com/auth/gmail.settings.basic',
            ],
            'shared': [
                'https://www.googleapis.com/auth/gmail.modify',
                'https://www.googleapis.com/auth/gmail.send',
            ],
        },
    },
    'Microsoft': {
        'client_id': indexer_config.MICROSOFT_EMAIL_CLIENT_ID,
        'client_secret': indexer_config.MICROSOFT_EMAIL_CLIENT_SECRET,
        'token_url': 'https://login.microsoftonline.com/common/oauth2/v2.0/token',
        'scopes': {
            'personal': ['openid', 'profile', 'email', 'offline_access', 'User.Read', 'Mail.ReadWrite', 'Mail.Send'],
            'shared': ['offline_access', 'Mail.ReadWrite.Shared', 'Mail.Send.Shared'],
        },
    },
}


class EmailHandler(ConnectionHandler):
    """Handler for EMAIL connection type."""

    connection_type = ConnectionType.EMAIL.value
    BATCH_SIZE = 50

    def gen_table_name(self, connection_id) -> str:
        return SchemaTables.THREAD.table_name(connection_id=connection_id)

    def gen_email_table_name(self, connection_id) -> str:
        return SchemaTables.EMAIL.table_name(connection_id=connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        email_index = EmailIndex(seasearch_api)
        return email_index.get_search_param(index_name, keyword, start, size, time_from, time_to)

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        email_index = EmailIndex(seasearch_api)
        email_index.delete_index_by_index_name(index_name)

        # summary vector index
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

    def format_vector_search_result(self, record, connection, result):
        return {
            'title': record.get('title', ''),
        }

    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = self.gen_table_name(connection_id)
        email_seadb_api = EmailSeaDBAPI(project_uuid)

        ai_processing_count = 0
        while True:
            rows = email_seadb_api.get_need_ai_emails_by_page(connection_id, page_size)
            if not rows:
                break
            for row in rows:
                pk = row.get('_pk')
                title = row.get('title', '')
                result = email_seadb_api.get_first_email(connection_id, pk)
                content = result.get('content') or ''

                full_content = f"Title: {title}\n\nBody: {content}\n"
                summary = self.seaqa_ai_api.generate_summary(full_content, ConnectionType.EMAIL.value, project_uuid)
                vector = self.seaqa_ai_api.generate_embedding(summary, project_uuid)
                updates.append({
                    "pk": pk,
                    "row": {
                        "ai_summary": summary,
                        "ai_summary_vector": vector,
                        "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                    }
                })
                ai_processing_count += 1
                if len(updates) >= self.BATCH_SIZE:
                    seadb_api.update_rows(project_uuid, table_name, updates)
                    updates = []
            if updates:
                seadb_api.update_rows(project_uuid, table_name, updates)
        return ai_processing_count

    def update_index(self, project_connection, seasearch_api, seaqa_db, since):
        connection_id = project_connection.id
        connection_type = project_connection.type
        project_uuid = project_connection.project_uuid
        connection_status = json.loads(project_connection.status)

        email_seadb_api = EmailSeaDBAPI(project_uuid)
        email_index = EmailIndex(seasearch_api)

        connection_status['last_index_status'] = IndexStatus.INDEXING
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
        index_name = gen_index_name_by_type(connection_id, connection_type)
        email_index.create_index_if_missing(index_name)

        index_count = email_index.update(index_name, connection_id, connection_status, seaqa_db, email_seadb_api, since)
        indexed_at = datetime.datetime.now(datetime.UTC)
        connection_status['last_indexed_count'] = index_count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))

    def sync(self, connection_id, project_uuid, config, old_last_sync_time, connection_status, seaqa_db, sync_logger):
        sync_years = config.get('sync_years', 5)
        since = old_last_sync_time
        is_first_update = False

        if not since:
            since = datetime.datetime.now(datetime.UTC) - relativedelta(years=sync_years)
            is_first_update = True
        since = time_to_utc_time(str(since)).isoformat()

        email_seadb_api = EmailSeaDBAPI(project_uuid)
        storage = get_storage()

        try:
            oauth_token = None
            oauth_config = None
            if config.get('server_provider') in ('Gmail', 'Microsoft'):
                oauth = seaqa_db.get_connection_oauth(project_uuid, connection_id)
                if not oauth:
                    raise EmailConfigError('Email OAuth authorization is required')
                oauth_token = {
                    'access_token': oauth.access_token,
                    'refresh_token': oauth.refresh_token,
                    'expires_at': oauth.expires_at.timestamp(),
                }
                provider = config.get('server_provider')
                account_type = config.get('account_type', 'personal')
                provider_config = EMAIL_OAUTH_CONFIGS.get(provider)
                if not provider_config or account_type not in provider_config['scopes']:
                    raise EmailConfigError('Email OAuth provider configuration is invalid')
                oauth_config = {
                    'client_id': provider_config['client_id'],
                    'client_secret': provider_config['client_secret'],
                    'token_url': provider_config['token_url'],
                    'scopes': provider_config['scopes'][account_type],
                }
                if account_type == 'shared':
                    oauth_config['client_id'] = config.get('client_id')
                    oauth_config['client_secret'] = config.get('client_secret')
                    if provider == 'Microsoft' and config.get('token_url'):
                        oauth_config['token_url'] = config['token_url']
            mail_viewer = get_mail_viewer_from_config(config, oauth_token, oauth_config)
        except (EmailConfigError, EmailAuthProviderError, EmailViewError) as e:
            logger.error('connection_id %s mail viewer initialization failed: %s', connection_id, e)
            sync_logger.log('email account is invalid')
            return None, None, None, None

        try:
            sync_count, total_count = self._sync_email(connection_id, project_uuid, connection_status, since,
                        mail_viewer, seaqa_db, email_seadb_api, storage, is_first_update, is_oauth=(oauth_token is not None))
        except (EmailConfigError, EmailAuthProviderError, EmailViewError) as e:
            logger.error('connection_id %s email sync failed: %s', connection_id, e)
            sync_logger.log(str(e))
            return None, None, None, None
        finally:
            mail_viewer.close()

        return sync_count, total_count, False, config

    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, cutoff_time):
        """Clean deleted email thread records and their emails."""
        page_size = 1000
        table_name = self.gen_table_name(connection_id)
        email_table_name = self.gen_email_table_name(connection_id)
        total_deleted = 0

        while True:
            sql = f"""
                SELECT _pk FROM `{table_name}` 
                WHERE deleted = true AND record_modified_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            deleted_pks = [row.get('_pk') for row in rows]

            # Delete from thread table
            seadb_api.delete_rows(project_uuid, table_name, deleted_pks)

            # Delete related emails
            for thread_id in deleted_pks:
                delete_sql = f"DELETE FROM `{email_table_name}` WHERE thread_id = {thread_id}"
                seadb_api.query_rows(project_uuid, delete_sql)

            total_deleted += len(deleted_pks)

            if len(rows) < page_size:
                break

        return total_deleted

    def _sync_email(
            self, connection_id, project_uuid, connection_status, since, mail_viewer,
            seaqa_db, email_seadb_api, storage, is_first_sync, is_oauth):
        sync_count = 0
        added_count = 0
        updated_count = 0
        _status_last_update_time = datetime.datetime.now()
        _status_last_update_count = 0

        since = time_to_utc_time(str(since))
        today = datetime.datetime.now(datetime.timezone.utc).date()

        # Due to time zone differences, emails sent on the same day may not be available for retrieval within that time frame.
        # Therefore, the search scope has been expanded to include a full day.
        before_date = since.date() - datetime.timedelta(days=1)

        while before_date <= today:
            since_date = before_date
            before_date += datetime.timedelta(days=EMAIL_SYNC_INTERVAL_DAYS)

            if is_oauth:
                refreshed_oauth_token = mail_viewer.refresh_access_token_if_needed()
                if refreshed_oauth_token:
                    expires_at = datetime.datetime.fromtimestamp(
                        refreshed_oauth_token['expires_at'], tz=datetime.timezone.utc
                    )
                    seaqa_db.update_connection_oauth(
                        project_uuid, connection_id, refreshed_oauth_token['access_token'],
                        expires_at, refreshed_oauth_token['refresh_token']
                    )
            email_list = mail_viewer.search_emails(since_date, before_date)
            changed_email_list = []
            for email in email_list:
                if not email:
                    continue
                existed_email = {}
                if not is_first_sync:
                    message_id = email.get('message_id')
                    modified_time = email.get('email_date')
                    existed_email = email_seadb_api.get_email_by_message_id(connection_id, message_id)
                    if existed_email and existed_email.get('modified_time'):
                        existed_date = time_to_utc_time(str(existed_email.get('modified_time')))
                        email_thread_id = existed_email.get('thread_id')
                        if time_to_utc_time(modified_time) <= existed_date and email_thread_id:
                            logger.debug(f'connection_id %s message_id %s is up to date, skipping sync.', connection_id, message_id)
                            continue
                email_info = {
                    'message_id': email.get('message_id'),
                    'reply_to_message_id': email.get('reply_to_message_id'),
                    'email_date': email.get('email_date'),
                    'subject': email.get('subject'),
                    'is_sender': email.get('is_sender'),
                    'email_from': email.get('email_from'),
                    '_is_new_email': not bool(existed_email),
                }
                changed_email_list.append(email_info)

                pk = email_seadb_api.save_or_update_email_to_db(connection_id, email, existed_email, is_first_sync)
                upload_email_attachment(connection_id, project_uuid, storage, email, pk)
                email_seadb_api.update_html_content_and_attachments(connection_id, pk, email)
                if existed_email:
                    updated_count += 1
                else:
                    added_count += 1

                should, new_time, new_count = need_update_connection_last_sync_count(
                    sync_count, _status_last_update_time, _status_last_update_count
                )

                if should:
                    connection_status['last_sync_count'] = sync_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                    _status_last_update_time = new_time
                    _status_last_update_count = new_count
                sync_count += 1
            if changed_email_list:
                old_thread_rows_by_id = self._get_old_thread_rows_by_id(
                    email_seadb_api, connection_id, changed_email_list
                )
                insert_and_update_thread_rows(
                    email_seadb_api, connection_id, since, changed_email_list, is_first_sync,
                )
                self._publish_thread_events(email_seadb_api, connection_id, changed_email_list, old_thread_rows_by_id)

        connection_status['last_sync_changes'] = {
            'added': added_count,
            'deleted': 0,
            'updated': updated_count,
        }
        total_count = email_seadb_api.get_email_total_count(connection_id)
        return sync_count, total_count

    def _get_old_thread_rows_by_id(self, email_seadb_api, connection_id, email_list):
        old_thread_rows_by_id = {}
        for email in email_list:
            reply_to_message_id = email.get('reply_to_message_id')
            if not reply_to_message_id:
                continue

            replied_email = email_seadb_api.get_email_by_message_id(connection_id, reply_to_message_id)
            thread_id = replied_email.get('thread_id')
            if not thread_id or thread_id in old_thread_rows_by_id:
                continue

            old_thread = email_seadb_api.get_thread_by_pk(connection_id, thread_id)
            if old_thread:
                old_thread_rows_by_id[thread_id] = old_thread
        return old_thread_rows_by_id

    def _is_new_outbound_email(self, email):
        return bool(email.get('_is_new_email') and email.get('is_sender'))

    def _publish_thread_events(self, email_seadb_api, connection_id, email_list, old_thread_rows_by_id):
        published_thread_ids = set()
        new_emails_by_thread_id = {}

        for email in email_list:
            if self._is_new_outbound_email(email):
                logger.info('Skip agent event for new outbound email (message_id=%s)', email.get('message_id'))
                continue

            message_id = email.get('message_id')
            if not message_id:
                continue

            latest_email = email_seadb_api.get_email_by_message_id(connection_id, message_id)
            thread_id = latest_email.get('thread_id')
            if not thread_id:
                continue
            if email.get('_is_new_email'):
                new_emails_by_thread_id.setdefault(thread_id, []).append({
                    key: value for key, value in email.items() if not key.startswith('_')
                })

        for email in email_list:
            if self._is_new_outbound_email(email):
                continue

            message_id = email.get('message_id')
            if not message_id:
                continue

            latest_email = email_seadb_api.get_email_by_message_id(connection_id, message_id)
            thread_id = latest_email.get('thread_id')
            if not thread_id or thread_id in published_thread_ids:
                continue

            old_thread = old_thread_rows_by_id.get(thread_id)
            if old_thread is None:
                state_event_type = DataEventType.EMAIL_THREAD_ADDED.value
            else:
                state_event_type = DataEventType.EMAIL_MESSAGE_ADDED.value

            self.publish_synced_record_events(
                get_latest_row=lambda tid=thread_id: email_seadb_api.get_thread_by_pk(connection_id, tid),
                project_uuid=email_seadb_api.base_id,
                connection_id=connection_id,
                item_id=thread_id,
                old_row=old_thread,
                state_event_type=state_event_type,
                additional_items=new_emails_by_thread_id.get(thread_id),
                additional_event_type=DataEventType.EMAIL_MESSAGE_ADDED.value,
                state_event_ignored_fields=('modified_time', 'unread', 'record_modified_time', 'sync_time'),
                log_object_name='email thread',
            )
            published_thread_ids.add(thread_id)
