import logging
import datetime

from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.utils import get_seadb_column_name, get_seadb_table_name
from seaqa_indexer.utils import time_to_utc_time


logger = logging.getLogger(__name__)

MAX_CONTENT_LENGTH = 10000


class DiscourseSeaDBAPI:
    def __init__(self, base_id, timeout=30):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI(timeout=timeout)

    # Topics operations
    def insert_topic(self, connection_id, topic_id, topic_data):
        table_name = get_seadb_table_name('DiscourseTopicsTable', connection_id=connection_id)

        title = topic_data.get('title')
        slug = topic_data.get('slug')
        views = topic_data.get('views', 0)
        category_id = topic_data.get('category_id')
        resolved = True if topic_data.get('has_accepted_answer', False) else False # Avoid writing a None value to the resolved field when has_accepted_answer is present but evaluates to None.
        modified_time = topic_data.get('bumped_at')
        topic_created_time = topic_data.get('created_at')

        rows = [{
            get_seadb_column_name('DiscourseTopicsTable', 'topic_id'): topic_id,
            get_seadb_column_name('DiscourseTopicsTable', 'title'): title,
            get_seadb_column_name('DiscourseTopicsTable', 'slug'): slug,
            get_seadb_column_name('DiscourseTopicsTable', 'views'): views,
            get_seadb_column_name('DiscourseTopicsTable', 'category_id'): category_id,
            get_seadb_column_name('DiscourseTopicsTable', 'modified_time'): time_to_utc_time(modified_time).isoformat(),
            get_seadb_column_name('DiscourseTopicsTable', 'deleted'): False,
            get_seadb_column_name('DiscourseTopicsTable', 'sync_time'): datetime.datetime.now(datetime.UTC).isoformat(),
            get_seadb_column_name('DiscourseTopicsTable', 'record_modified_time'): datetime.datetime.now(datetime.UTC).isoformat(),
            get_seadb_column_name('DiscourseTopicsTable', 'created_time'): time_to_utc_time(topic_created_time).isoformat(),
            get_seadb_column_name('DiscourseTopicsTable', 'resolved'): resolved,
        }]
        return self.seadb_api.insert_rows(self.base_id, table_name, rows)

    def get_topics_by_page(self, connection_id, since, page_size, offset):
        table_name = get_seadb_table_name('DiscourseTopicsTable', connection_id=connection_id)
        sql = f"""SELECT `_pk`, `topic_id`, `deleted`, `outdated`, `title`, `modified_time` FROM `{table_name}` 
                WHERE record_modified_time >= '{since}' 
                LIMIT {page_size} OFFSET {offset}"""
        response = self.seadb_api.query_rows(self.base_id, sql)
        results = response.get('results', [])
        return results

    def get_topic_by_topic_id(self, connection_id, topic_id):
        table_name = get_seadb_table_name('DiscourseTopicsTable', connection_id=connection_id)
        sql = f"""SELECT `_pk`, `topic_id`, `title`, `slug`, `views`, `category_id`, `modified_time`, `resolved`,
                `deleted`, `outdated`, `record_modified_time`, `sync_time`, `created_time`, `ai_processed_time`
                FROM `{table_name}` WHERE `topic_id` = {topic_id}"""
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0]
        return {}

    def get_reply_by_id(self, connection_id, topic_id, post_number):
        table_name = get_seadb_table_name('DiscourseRepliesTable', connection_id=connection_id)
        sql = f"""SELECT `_pk`, `topic_id`, `post_number`, `content`, `author`, `modified_time`, `accepted_answer`
                FROM `{table_name}` WHERE `topic_id` = {topic_id} AND `post_number` = {post_number}"""
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0]
        return {}

    def update_topic(self, connection_id, primary_key, topic_data):
        table_name = get_seadb_table_name('DiscourseTopicsTable', connection_id=connection_id)

        title = topic_data.get('title')
        slug = topic_data.get('slug')
        views = topic_data.get('views', 0)
        category_id = topic_data.get('category_id')
        resolved = True if topic_data.get('has_accepted_answer', False) else False # Avoid writing a None value to the resolved field when has_accepted_answer is present but evaluates to None.
        bumped_at = topic_data.get('bumped_at')
        last_posted_at = topic_data.get('last_posted_at')
        # When using the webhook, in TOPIC_CREATED mode,
        # the received data does not have bumped_at, so last_posted_at is used
        modified_time = bumped_at if bumped_at else last_posted_at

        updates = [{
            "pk": primary_key,
            "row": {
                get_seadb_column_name('DiscourseTopicsTable', 'title'): title,
                get_seadb_column_name('DiscourseTopicsTable', 'slug'): slug,
                get_seadb_column_name('DiscourseTopicsTable', 'views'): views,
                get_seadb_column_name('DiscourseTopicsTable', 'category_id'): category_id,
                get_seadb_column_name('DiscourseTopicsTable', 'modified_time'): time_to_utc_time(modified_time).isoformat(),
                get_seadb_column_name('DiscourseTopicsTable', 'sync_time'): datetime.datetime.now(datetime.UTC).isoformat(),
                get_seadb_column_name('DiscourseTopicsTable', 'record_modified_time'): datetime.datetime.now(datetime.UTC).isoformat(),
                get_seadb_column_name('DiscourseTopicsTable', 'deleted'): False,
                get_seadb_column_name('DiscourseTopicsTable', 'resolved'): resolved,
            }
        }]
        return self.seadb_api.update_rows(self.base_id, table_name, updates)

    def delete_topic(self, connection_id, topic_id):
        """Logically delete topic record"""
        table_name = get_seadb_table_name('DiscourseTopicsTable', connection_id=connection_id)
        topic = self.get_topic_by_topic_id(connection_id, topic_id)
        if not topic:
            logger.debug(f"Topic {topic_id} not found in connection {connection_id}, skipping deletion")
            return None
        primary_key = topic.get('_pk')
        updates = [{
            "pk": primary_key,
            "row": {
                "deleted": True,
                "sync_time": datetime.datetime.now(datetime.UTC).isoformat(),
                "record_modified_time": datetime.datetime.now(datetime.UTC).isoformat()
            }
        }]
        return self.seadb_api.update_rows(self.base_id, table_name, updates)

    def insert_reply(self, connection_id, topic_id, reply):
        table_name = get_seadb_table_name('DiscourseRepliesTable', connection_id=connection_id)
        post_number = reply.get('post_number')
        content = reply.get('cooked')
        author = reply.get('username')
        modified_time = reply.get('updated_at')
        accepted_answer = reply.get('accepted_answer', False)

        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]
        rows = [{
            get_seadb_column_name('DiscourseRepliesTable', 'topic_id'): topic_id,
            get_seadb_column_name('DiscourseRepliesTable', 'post_number'): post_number,
            get_seadb_column_name('DiscourseRepliesTable', 'content'): content,
            get_seadb_column_name('DiscourseRepliesTable', 'author'): author,
            get_seadb_column_name('DiscourseRepliesTable', 'accepted_answer'): accepted_answer,
            get_seadb_column_name('DiscourseRepliesTable', 'modified_time'): time_to_utc_time(modified_time).isoformat(),
        }]
        return self.seadb_api.insert_rows(self.base_id, table_name, rows)

    def update_reply(self, connection_id, reply_pk, topic_id, reply):
        table_name = get_seadb_table_name('DiscourseRepliesTable', connection_id=connection_id)
        post_number = reply.get('post_number')
        content = reply.get('cooked', '')
        author = reply.get('username')
        modified_time = reply.get('updated_at')
        accepted_answer = reply.get('accepted_answer', False)

        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]
        updates = [{
            "pk": reply_pk,
            "row": {
                get_seadb_column_name('DiscourseRepliesTable', 'content'): content,
                get_seadb_column_name('DiscourseRepliesTable', 'author'): author,
                get_seadb_column_name('DiscourseRepliesTable', 'modified_time'): time_to_utc_time(modified_time).isoformat(),
                get_seadb_column_name('DiscourseRepliesTable', 'topic_id'): topic_id,
                get_seadb_column_name('DiscourseRepliesTable', 'post_number'): post_number,
                get_seadb_column_name('DiscourseRepliesTable', 'accepted_answer'): accepted_answer,
            }
        }]
        return self.seadb_api.update_rows(
            base_id=self.base_id,
            table_name=table_name,
            rows=updates
        )

    def update_topic_replies(self, connection_id, topic_id, topic_replies):
        offset = 0
        page_size = 1000
        old_reply_ids = set()
        new_reply_ids = set()
        post_number_to_pk = {}
        added_replies = []
        while True:
            replies = self.get_topic_replies_by_page(connection_id, topic_id, page_size, offset)
            for reply in replies:
                post_number = reply.get('post_number')
                post_number_to_pk[post_number] = reply.get('_pk')
                old_reply_ids.add(post_number)
            if not replies or len(replies) < page_size:
                break
            offset += page_size

        for reply in topic_replies:
            if reply.get('post_type') != 1:
                continue
            post_number = reply.get('post_number')
            new_reply_ids.add(post_number)

            pk = post_number_to_pk.get(post_number)

            if pk:
                self.update_reply(connection_id, pk, topic_id, reply)
            else:
                self.insert_reply(connection_id, topic_id, reply)
                added_replies.append({
                    'post_number': post_number,
                    'author': reply.get('username'),
                    'content': reply.get('cooked'),
                    'modified_time': time_to_utc_time(reply.get('updated_at')).isoformat(),
                })

        need_deleted_reply_ids = old_reply_ids - new_reply_ids
        for reply_id in need_deleted_reply_ids:
            self.delete_reply_from_db(connection_id, topic_id, reply_id)

        return added_replies

    def delete_reply_from_db(self, connection_id, topic_id, post_number):
        table_name = get_seadb_table_name('DiscourseRepliesTable', connection_id=connection_id)
        sql = f"DELETE FROM `{table_name}` WHERE topic_id = {topic_id} AND post_number = {post_number}"
        return self.seadb_api.query_rows(self.base_id, sql)

    def update_topic_sync_and_record_time(self, connection_id, topic_id):
        table_name = get_seadb_table_name('DiscourseTopicsTable', connection_id=connection_id)
        topic = self.get_topic_by_topic_id(connection_id, topic_id)
        if not topic:
            return None
        primary_key = topic.get('_pk')
        updates = [{
            "pk": primary_key,
            "row": {
                "sync_time": datetime.datetime.now(datetime.UTC).isoformat(),
                "record_modified_time": datetime.datetime.now(datetime.UTC).isoformat()
            }
        }]
        return self.seadb_api.update_rows(self.base_id, table_name, updates)

    def get_topic_replies_by_page(self, connection_id, topic_id, page_size, offset):
        table_name = get_seadb_table_name('DiscourseRepliesTable', connection_id=connection_id)
        sql = f"""SELECT `_pk`, `post_number`, `content` FROM `{table_name}`
                WHERE topic_id = {topic_id} LIMIT {page_size} OFFSET {offset}"""
        replies = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        return replies

    def get_topic_first_reply(self, connection_id, topic_id):
        table_name = get_seadb_table_name('DiscourseRepliesTable', connection_id=connection_id)
        if not topic_id:
            return {}

        sql = f"SELECT topic_id, content FROM `{table_name}` WHERE topic_id = {topic_id} ORDER BY post_number ASC LIMIT 1"
        result = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if result:
            return result[0]
        return {}

    def get_topic_count(self, connection_id):
        topics_table_name = get_seadb_table_name('DiscourseTopicsTable', connection_id=connection_id)

        topics_sql = f"SELECT COUNT(*) as count FROM `{topics_table_name}`"
        topics_result = self.seadb_api.query_rows(self.base_id, topics_sql)
        topic_count = 0
        if topics_result and 'results' in topics_result and topics_result['results']:
            topic_count = topics_result['results'][0].get('count', 0)

        return topic_count

    def get_need_ai_topics(self, connection_id, page_size):
        table_name = get_seadb_table_name('DiscourseTopicsTable', connection_id=connection_id)
        sql = f"""SELECT _pk, topic_id, title FROM `{table_name}`
        WHERE (ai_processed_time IS NULL OR ai_processed_time < sync_time) AND (deleted IS NULL OR deleted = false)
        LIMIT {page_size}"""
        result = self.seadb_api.query_rows(self.base_id, sql)
        return result.get('results', [])
