import logging
import datetime

from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.constants import ConnectionType
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

    def get_topic_by_id(self, connection_id, topic_id):
        """Retrieve topic record based on topic_id"""
        table_name = DiscourseTopicsTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE `topic_id` = {topic_id}"
        result = self.seadb_api.query_rows(self.base_id, sql)
        if result and 'results' in result and result['results']:
            return result['results'][0]
        return None

    def get_topics_by_connection_id(self, connection_id, update_at_gt=None, topic_id=None):
        """Retrieve all topics for the specified connection_id."""
        table_name = DiscourseTopicsTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}`"
        if update_at_gt is not None:
            if isinstance(update_at_gt, datetime.datetime):
                update_at_gt = update_at_gt.isoformat()
            sql += f" WHERE `update_at` > {update_at_gt}"
        if topic_id is not None:
            if 'WHERE' in sql:
                sql += f" AND `topic_id` = {topic_id}"
            else:
                sql += f" WHERE `topic_id` = {topic_id}"
        result = self.seadb_api.query_rows(self.base_id, sql)
        if result and 'results' in result:
            return result['results']
        return []

    def get_reply_by_id(self, connection_id, topic_id, post_number):
        """Get discourse reply by topic_id and post_number"""
        table_name = DiscourseRepliesTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE `topic_id` = {topic_id} AND `post_number` = {post_number}"
        result = self.seadb_api.query_rows(self.base_id, sql)
        if result and 'results' in result and result['results']:
            return result['results'][0]
        return None

    def update_topic(self, connection_id, topic_data):
        table_name = DiscourseTopicsTable.gen_table_name(connection_id)
        pk = topic_data.get('_pk')
        row_data = {k: v for k, v in topic_data.items() if k != '_pk'}
        updates = [{
            "pk": pk,
            "row": row_data
        }]
        return self.seadb_api.update_rows(
            base_id=self.base_id,
            table_name=table_name,
            rows=updates
        )

    def delete_topic(self, connection_id, topic_id):
        """Delete topic record"""
        table_name = DiscourseTopicsTable.gen_table_name(connection_id)
        topics = self.get_topics_by_connection_id(connection_id, topic_id=topic_id)
        primary_key = topics[0].get('_pk')
        return self.seadb_api.delete_rows(self.base_id, table_name, [primary_key])

    # Replies operations
    def insert_reply(self, connection_id, reply_data):
        """Insert a single reply record"""
        table_name = DiscourseRepliesTable.gen_table_name(connection_id)
        rows = [reply_data]
        return self.seadb_api.insert_rows(self.base_id, table_name, rows)

    def update_reply(self, connection_id, topic_id, post_number, reply_data):
        """Update reply records"""
        reply = self.get_reply_by_id(connection_id, topic_id, post_number)
        pk = reply.get('_pk')

        table_name = DiscourseRepliesTable.gen_table_name(connection_id)

        reply_data['topic_id'] = topic_id
        reply_data['post_number'] = post_number
        rows = [reply_data]
        updates = [{
            "pk": pk,
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
        result = self.seadb_api.query_rows(self.base_id, sql)
        return result

    def get_replies_by_topic(self, connection_id, topic_id):
        """Retrieve all replies for the specified topic."""
        table_name = DiscourseRepliesTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE `topic_id` = {topic_id}"
        result = self.seadb_api.query_rows(self.base_id, sql)
        if result and 'results' in result:
            return result['results']
        return []

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
