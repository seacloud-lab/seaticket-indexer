import datetime
import json

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.index.index_store.confluence_index import ConfluenceIndex
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.seadb_models.confluence_seadb_api import ConfluenceSeaDBAPI
from seaqa_indexer.seadb_models.models import SchemaTables
from seaqa_indexer.utils import need_update_connection_last_sync_count, time_to_utc_time
from seaqa_indexer.utils.constants import ConnectionType, DataEventType, IndexStatus, \
    OAUTH_TOKEN_REFRESH_THRESHOLD_SECONDS
from seaqa_indexer.utils.confluence_api import ConfluenceAPI, ConfluenceUserNotFoundException


class ConfluenceHandler(ConnectionHandler):
    connection_type = ConnectionType.CONFLUENCE.value
    BATCH_SIZE = 50

    def gen_table_name(self, connection_id):
        return SchemaTables.CONFLUENCE.table_name(connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        confluence_index = ConfluenceIndex(seasearch_api)
        return confluence_index.get_search_param(index_name, keyword, start, size, time_from, time_to)

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        confluence_index = ConfluenceIndex(seasearch_api)
        confluence_index.delete_index_by_index_name(index_name)
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

    def format_vector_search_result(self, record, connection, result):
        connection_config = json.loads(connection.config)
        workspace_url = connection_config.get('workspace_url', '')
        page_id = record.get('page_id') or ''
        url = f'{workspace_url}/wiki/pages/viewpage.action?pageId={page_id}'
        return {
            'title': record.get('title', ''),
            'modified_time': record.get('modified_time', ''),
            'page_id': page_id,
            'url': url,
        }

    def process_ai(self, seadb_api, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = SchemaTables.CONFLUENCE.table_name(connection_id)
        confluence_seadb_api = ConfluenceSeaDBAPI(project_uuid)

        ai_processing_count = 0
        while True:
            rows = confluence_seadb_api.get_need_ai_confluence_pages_by_page(connection_id, page_size)
            if not rows:
                break
            for row in rows:
                pk = row.get('_pk')
                title = row.get('title', '')
                content = row.get('content', '')
                full_content = f"Title: {title}\n\nBody: {content}\n"
                summary = self.seaqa_ai_api.generate_summary(full_content, ConnectionType.CONFLUENCE.value, project_uuid)
                vector = self.seaqa_ai_api.generate_embedding(summary, project_uuid)
                updates.append({
                    'pk': pk,
                    'row': {
                        'ai_summary': summary,
                        'ai_summary_vector': vector,
                        'ai_processed_time': datetime.datetime.now(datetime.UTC).isoformat()
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
        connection_status = json.loads(project_connection.status)
        project_uuid = project_connection.project_uuid
        confluence_seadb_api = ConfluenceSeaDBAPI(project_uuid)
        confluence_index = ConfluenceIndex(seasearch_api)

        connection_status['last_index_status'] = IndexStatus.INDEXING
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

        index_name = gen_index_name_by_type(connection_id, project_connection.type)
        confluence_index.create_index_if_missing(index_name)

        index_count = confluence_index.update(
            index_name,
            connection_id,
            connection_status,
            seaqa_db,
            confluence_seadb_api,
            since,
        )

        indexed_at = datetime.datetime.now(datetime.UTC)
        connection_status['last_indexed_count'] = index_count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))

    def sync(self, connection_id, project_uuid, config, old_last_sync_time,
             connection_status, seaqa_db, sync_logger):
        workspace_id = config.get('workspace_id')
        if not workspace_id:
            sync_logger.log('Confluence workspace_id is required.')
            raise RuntimeError('Confluence workspace_id is required.')

        oauth = seaqa_db.get_project_connection_oauth(project_uuid, ConnectionType.CONFLUENCE.value)
        if not oauth:
            sync_logger.log('Confluence OAuth not found.')
            raise RuntimeError('Confluence OAuth not found.')

        access_token = oauth.access_token
        refresh_token = oauth.refresh_token
        expires_at = oauth.expires_at

        # Refresh proactively if the token has expired
        if not expires_at or time_to_utc_time(str(expires_at)) <= datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=OAUTH_TOKEN_REFRESH_THRESHOLD_SECONDS):
            api = ConfluenceAPI(access_token, refresh_token)
            new_tokens = api.refresh_access_token()
            access_token = new_tokens['access_token']
            refresh_token = new_tokens['refresh_token']
            expires_at = new_tokens['expires_at']
            seaqa_db.update_project_connection_oauth(project_uuid, ConnectionType.CONFLUENCE.value, access_token, expires_at, refresh_token)

        confluence_api = ConfluenceAPI(access_token, refresh_token)
        confluence_seadb_api = ConfluenceSeaDBAPI(project_uuid)
        space_keys = config.get('space_keys') or None
        sync_count, total_count = self._sync_confluence(
            connection_id,
            project_uuid,
            connection_status,
            confluence_api,
            seaqa_db,
            confluence_seadb_api,
            workspace_id,
            old_last_sync_time,
            space_keys,
        )

        return sync_count, total_count, False, config

    def _sync_confluence(self, connection_id, project_uuid, connection_status, confluence_api, seaqa_db,
                         confluence_seadb_api, workspace_id, old_last_sync_time, space_keys=None):
        sync_count = 0
        page_size = 100
        status_last_update_time = datetime.datetime.now()
        status_last_update_count = 0
        user_cache = {}

        if old_last_sync_time:
            since = time_to_utc_time(str(old_last_sync_time))
        else:
            # CQL server-side filter: only fetch pages modified since the latest local record. 
            # Since_time use latest modified time because the expiration time for the access_token is one hour.
            since = confluence_seadb_api.get_latest_modified_time(connection_id)

        def _upsert_user(account_id):
            """Resolve account ID → display name and store in user table.
            Does NOT modify the page dict — account IDs stay as-is."""
            if not account_id or account_id in user_cache:
                return
            user_cache[account_id] = True  # mark as processed
            try:
                name = confluence_api.get_user_display_name(account_id, workspace_id)
            except ConfluenceUserNotFoundException:
                return
            if name:
                confluence_seadb_api.upsert_user(connection_id, account_id, name)

        space_contexts = [None] if space_keys is None else space_keys
        for space_key in space_contexts:
            cursor = None
            while True:
                pages, next_cursor, has_more = confluence_api.list_pages(
                    workspace_id,
                    cursor=cursor,
                    limit=page_size,
                    space_keys=[space_key] if space_key else None,
                    since=since,
                )
                if not pages and not has_more:
                    break
                for page in pages:
                    page_id = page.get('page_id')
                    if not page_id:
                        continue

                    # Upsert user display names into user table (account IDs stay on page dict)
                    _upsert_user(page.get('creator_id'))
                    _upsert_user(page.get('last_modifier_id'))

                    page_modified_time = time_to_utc_time(page.get('modified_time'))
                    existed_page = confluence_seadb_api.get_page_by_page_id(page_id, connection_id)
                    if existed_page and existed_page.get('modified_time'):
                        local_modified_time = time_to_utc_time(str(existed_page.get('modified_time')))
                        if page_modified_time <= local_modified_time:
                            continue

                    confluence_seadb_api.save_or_update_page_to_db(connection_id, page, existed_page)
                    sync_count += 1
                    state_event_type = DataEventType.CONFLUENCE_PAGE_UPDATED.value if existed_page else DataEventType.CONFLUENCE_PAGE_ADDED.value

                    self.publish_synced_record_events(
                        get_latest_row=lambda pf=page_id: confluence_seadb_api.get_page_by_page_id(pf, connection_id),
                        project_uuid=project_uuid,
                        connection_id=connection_id,
                        item_id=page_id,
                        old_row=existed_page or None,
                        state_event_type=state_event_type,
                        log_object_name='confluence page',
                    )

                    should, new_time, new_count = need_update_connection_last_sync_count(
                        sync_count, status_last_update_time, status_last_update_count
                    )
                    if should:
                        connection_status['last_sync_count'] = sync_count
                        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                        status_last_update_time = new_time
                        status_last_update_count = new_count

                if not has_more or not next_cursor:
                    break
                cursor = next_cursor

        total_count = confluence_seadb_api.get_confluence_pages_count(connection_id)
        return sync_count, total_count
