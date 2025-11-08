import logging

from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import DiscourseTopicsTable, DiscourseRepliesTable


logger = logging.getLogger(__name__)

class DiscourseSeaDBAPI:
    def __init__(self, base_id, username='', timeout=30):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI(username=username, timeout=timeout)

    # Topics operations
    def insert_topic(self, connection_id, topic_data):
        """Insert a single topic record"""
        table_name = DiscourseTopicsTable.gen_table_name(connection_id)
        rows = [topic_data]
        return self.seadb_api.insert_rows(self.base_id, table_name, rows)

    def get_topics_by_page(self, connection_id, since, page_size, offset):
        table_name = DiscourseTopicsTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE sync_time >= '{since}' LIMIT {page_size} OFFSET {offset}"
        res = self.seadb_api.query_rows(self.base_id, sql)
        results = res.get('results', [])
        return results

    def get_topic_by_topic_id(self, connection_id, topic_id):
        table_name = DiscourseTopicsTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE `topic_id` = {topic_id}"
        result = self.seadb_api.query_rows(self.base_id, sql)
        if result and 'results' in result:
            return result['results'][0]
        return None

    def get_reply_by_id(self, connection_id, topic_id, post_number):
        """Get discourse reply by topic_id and post_number"""
        table_name = DiscourseRepliesTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE `topic_id` = {topic_id} AND `post_number` = {post_number}"
        result = self.seadb_api.query_rows(self.base_id, sql)
        if result and 'results' in result and result['results']:
            return result['results'][0]
        return {}

    def update_topic(self, connection_id, topic_data, primary_key):
        table_name = DiscourseTopicsTable.gen_table_name(connection_id)
        updates = [{
            "pk": primary_key,
            "row": topic_data
        }]
        return self.seadb_api.update_rows(
            base_id=self.base_id,
            table_name=table_name,
            rows=updates
        )

    def delete_topic(self, connection_id, topic_id):
        """Delete topic record"""
        table_name = DiscourseTopicsTable.gen_table_name(connection_id)
        topic = self.get_topic_by_topic_id(connection_id, topic_id)
        if not topic:
            logger.debug(f"Topic {topic_id} not found in connection {connection_id}, skipping deletion")
            return None
        primary_key = topic.get('_pk')
        return self.seadb_api.delete_rows(self.base_id, table_name, [primary_key])

    def delete_topics_from_db(self, connection_id, need_delete_topic_pks, need_deleted_reply_topic_ids):
        table_name = DiscourseTopicsTable.gen_table_name(connection_id)
        self.seadb_api.delete_rows(self.base_id, table_name, need_delete_topic_pks)

        comments_table_name = DiscourseRepliesTable.gen_table_name(connection_id)
        for topic_id in need_deleted_reply_topic_ids:
            delete_sql = f"DELETE FROM `{comments_table_name}` WHERE topic_id = {topic_id}"
            self.seadb_api.query_rows(self.base_id, delete_sql)

    # Replies operations
    def insert_reply(self, connection_id, reply_data):
        """Insert a single reply record"""
        table_name = DiscourseRepliesTable.gen_table_name(connection_id)
        rows = [reply_data]
        return self.seadb_api.insert_rows(self.base_id, table_name, rows)

    def update_reply(self, connection_id, reply_pk, reply_data):
        """Update reply records"""
        table_name = DiscourseRepliesTable.gen_table_name(connection_id)
        rows = [reply_data]
        updates = [{
            "pk": reply_pk,
            "row": rows
        }]
        return self.seadb_api.update_rows(
            base_id=self.base_id,
            table_name=table_name,
            rows=updates
        )

    def delete_reply(self, connection_id, topic_id, post_number):
        """Delete reply record"""
        table_name = DiscourseRepliesTable.gen_table_name(connection_id)
        reply = self.get_reply_by_id(connection_id, topic_id, post_number)
        primary_key = reply.get('_pk')
        return self.seadb_api.delete_rows(self.base_id, table_name, [primary_key])

    def delete_replies_by_topic(self, connection_id, topic_id):
        """Delete all replies for the specified topic."""
        table_name = DiscourseRepliesTable.gen_table_name(connection_id)
        sql = f"DELETE FROM `{table_name}` WHERE topic_id = {topic_id}"
        results = self.seadb_api.query_rows(self.base_id, sql)
        return results

    def get_topic_replies_by_page(self, connection_id, topic_id, page_size, offset):
        table_name = DiscourseRepliesTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE topic_id = {topic_id} LIMIT {page_size} OFFSET {offset}"
        replies = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        return replies

    def delete_topic_from_db(self, topic_id, connection_id):
        """Delete topic from database"""
        table_name = DiscourseTopicsTable.gen_table_name(connection_id)
        sql = f"DELETE FROM `{table_name}` WHERE topic_id = '{topic_id}'"
        return self.seadb_api.query_rows(self.base_id, sql)

    def get_topic_count(self, connection_id):
        """Retrieve topic count for the specified connection_id."""
        topics_table_name = DiscourseTopicsTable.gen_table_name(connection_id)

        # Obtain the number of topics
        topics_sql = f"SELECT COUNT(*) as count FROM `{topics_table_name}`"
        topics_result = self.seadb_api.query_rows(self.base_id, topics_sql)
        topic_count = 0
        if topics_result and 'results' in topics_result and topics_result['results']:
            topic_count = topics_result['results'][0].get('count', 0)

        return topic_count
