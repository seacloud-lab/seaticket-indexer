import json
import datetime
import logging
import time

from dateutil.relativedelta import relativedelta

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.seadb_models.models import DiscourseTopicsTable, DiscourseRepliesTable
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.utils.discourse_forum_api import DiscourseForumAPI
from seaqa_indexer.seadb_models.discourse_seadb_api import DiscourseSeaDBAPI
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.utils.constants import IndexStatus
from seaqa_indexer.utils import need_update_connection_last_sync_count, time_to_utc_time
from seaqa_indexer.index.index_store.discourse_index import DiscourseIndex
from seaqa_indexer.ai_processor.utils import generate_summary
from seaqa_indexer.utils.constants import DataEventType

logger = logging.getLogger(__name__)


class DiscourseHandler(ConnectionHandler):
    """Handler for DISCOURSE_FORUM connection type."""

    connection_type = ConnectionType.DISCOURSE_FORUM.value
    BATCH_SIZE = 50

    def gen_table_name(self, connection_id):
        return DiscourseTopicsTable.gen_table_name(connection_id)

    def gen_replies_table_name(self, connection_id):
        return DiscourseRepliesTable.gen_table_name(connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        discourse_index = DiscourseIndex(seasearch_api)
        return discourse_index.get_search_param(index_name, keyword, start, size, time_from, time_to)

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        discourse_index = DiscourseIndex(seasearch_api)
        discourse_index.delete_index_by_index_name(index_name)

        # summary vector index
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

    def format_vector_search_result(self, record, connection, result):
        connection_config = json.loads(connection.config)
        discourse_forum_url = connection_config.get('url')
        slug = record.get('slug')
        topic_id = record.get('topic_id')
        topic_url = discourse_forum_url.rstrip('/') + '/t/' + slug + '/' + str(topic_id)

        return {
            'title': record.get('title', ''),
            'slug': record.get('slug', ''),
            'topic_id': record.get('topic_id', ''),
            'url': topic_url,
        }

    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = self.gen_table_name(connection_id)
        discourse_seadb_api = DiscourseSeaDBAPI(project_uuid)

        ai_processing_count = 0
        while True:
            rows = discourse_seadb_api.get_need_ai_topics(connection_id, page_size)
            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                topic_id = row.get('topic_id')
                title = row.get('title', '')
                result = discourse_seadb_api.get_topic_first_reply(connection_id, topic_id)
                first_content = result.get('content', '')
                full_content = f"Title: {title}\n\nBody: {first_content}\n"
                summary, vector = generate_summary(full_content, ConnectionType.DISCOURSE_FORUM.value, project_uuid)
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

        discourse_seadb_api = DiscourseSeaDBAPI(project_uuid)
        discourse_forum_index = DiscourseIndex(seasearch_api)

        connection_status['last_index_status'] = IndexStatus.INDEXING
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
        discourse_index_name = gen_index_name_by_type(connection_id, connection_type)
        discourse_forum_index.create_index_if_missing(discourse_index_name)

        index_count = discourse_forum_index.update(discourse_index_name, connection_id, connection_status, seaqa_db, discourse_seadb_api, since)
        indexed_at = datetime.datetime.now(datetime.UTC)
        connection_status['last_indexed_count'] = index_count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))

    def sync(self, connection_id, project_uuid, config, old_last_sync_time,
             connection_status, seaqa_db, sync_logger):
        forum_url = config['url']
        discourse_api_key = config['api_key']
        discourse_api_username = config['api_username']
        sync_years = config.get('sync_years', 5)

        project_connection = seaqa_db.get_project_connection_by_id(connection_id)
        project_created_at = project_connection.created_at

        discourse_forum_api = DiscourseForumAPI(forum_url, discourse_api_key, discourse_api_username)
        discourse_seadb_api = DiscourseSeaDBAPI(project_uuid)

        # Calculate sync_earliest_time by subtracting sync_years from project_created_time
        project_created_time_utc = time_to_utc_time(str(project_created_at))
        sync_earliest_time = project_created_time_utc - relativedelta(years=sync_years)

        sync_count, total_count = self._sync_discourse(connection_id, sync_earliest_time, old_last_sync_time,
                             connection_status, discourse_forum_api, discourse_seadb_api, seaqa_db)
        return sync_count, total_count, False, config

    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, cutoff_time):
        """Clean deleted discourse topic records and their replies."""
        page_size = 1000
        table_name = self.gen_table_name(connection_id)
        replies_table_name = self.gen_replies_table_name(connection_id)
        total_deleted = 0

        while True:
            sql = f"""
                SELECT _pk, topic_id FROM `{table_name}` 
                WHERE deleted = true AND sync_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            deleted_pks = [row.get('_pk') for row in rows]
            deleted_topic_ids = [row.get('topic_id') for row in rows]

            seadb_api.delete_rows(project_uuid, table_name, deleted_pks)
            for topic_id in deleted_topic_ids:
                delete_sql = f"DELETE FROM `{replies_table_name}` WHERE topic_id = {topic_id}"
                seadb_api.query_rows(project_uuid, delete_sql)

            total_deleted += len(deleted_pks)

            if len(rows) < page_size:
                break

        return total_deleted

    def clean_connection_data(self, storage, seadb_api, project_uuid,
                              connection_id, tables):
        """Clean discourse connection data including topics and replies tables."""
        if tables:
            topics_table_name = self.gen_table_name(connection_id)
            replies_table_name = self.gen_replies_table_name(connection_id)
            for table in tables:
                if table['name'] in (topics_table_name, replies_table_name):
                    seadb_api.delete_table(project_uuid, table['id'])

    def _sync_discourse(self, connection_id, sync_earliest_time, last_sync_time, connection_status, discourse_forum_api, discourse_seadb_api, seaqa_db):
        page = 0
        should_stop = False
        sync_count = 0
        added_count = 0
        updated_count = 0
        _status_last_update_time = datetime.datetime.now()
        _status_last_update_count = 0

        if last_sync_time:
            last_sync_time = time_to_utc_time(str(last_sync_time))

        while True:
            topics = discourse_forum_api.fetch_latest_topics_page(page)

            if not topics:
                break

            for topic_data in topics:
                topic_modified_time = time_to_utc_time(topic_data.get('bumped_at'))

                if last_sync_time and topic_modified_time <= last_sync_time and not topic_data.get('pinned'):
                    should_stop = True
                    break

                if topic_modified_time <= sync_earliest_time:
                    if topic_data.get('pinned'):
                        continue
                    should_stop = True
                    break

                topic_id = topic_data.get('id')

                # Check if topic already exists in DB
                existing_topic = discourse_seadb_api.get_topic_by_topic_id(connection_id, topic_id)
                if existing_topic:
                    existing_modified_time = time_to_utc_time(str(existing_topic.get('modified_time')))
                    if topic_modified_time <= existing_modified_time:
                        logger.debug(f'Topic {topic_id} is up to date, skipping sync.')
                        continue

                topic_state_event_type = (
                    DataEventType.DISCOURSE_TOPIC_UPDATED.value
                    if existing_topic
                    else DataEventType.DISCOURSE_TOPIC_ADDED.value
                )
                primary_key = existing_topic.get('_pk')
                if primary_key:
                    discourse_seadb_api.update_topic(connection_id, primary_key, topic_data)
                    updated_count += 1
                else:
                    discourse_seadb_api.insert_topic(connection_id, topic_id, topic_data)
                    added_count += 1

                # sync topic replies
                topic_replies = discourse_forum_api.fetch_topic_replies(topic_id)
                added_replies = discourse_seadb_api.update_topic_replies(connection_id, topic_id, topic_replies)

                sync_count += 1
                should, new_time, new_count = need_update_connection_last_sync_count(
                    sync_count, _status_last_update_time, _status_last_update_count
                )
                if should:
                    connection_status['last_sync_count'] = sync_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                    _status_last_update_time = new_time
                    _status_last_update_count = new_count
                time.sleep(1)

                self.publish_synced_record_events(
                    get_latest_row=lambda: discourse_seadb_api.get_topic_by_topic_id(connection_id, topic_id),
                    project_uuid=discourse_seadb_api.base_id,
                    connection_id=connection_id,
                    item_id=topic_id,
                    old_row=existing_topic,
                    state_event_type=topic_state_event_type,
                    additional_items=added_replies,
                    additional_event_type=DataEventType.DISCOURSE_TOPIC_REPLY_ADDED.value,
                    additional_items_field='replies',
                    log_object_name='topic',
                )

            page += 1
            if should_stop:
                break

        connection_status['last_sync_changes'] = {
            'added': added_count,
            'deleted': 0,
            'updated': updated_count,
        }
        total_count = discourse_seadb_api.get_topic_count(connection_id)
        return sync_count, total_count
