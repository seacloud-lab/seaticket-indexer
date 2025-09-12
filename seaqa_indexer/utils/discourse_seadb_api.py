import logging
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.constants import DISCOURSE_TOPICS_COLUMNS, DISCOURSE_REPLIES_COLUMNS

logger = logging.getLogger(__name__)

class DiscourseSeaDBAPI:
    def __init__(self, base_id, username='', timeout=30):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI(username=username, timeout=timeout)
    
    def _get_topics_table_name(self, connection_id):
        """Retrieve the table name of topics"""
        return f"{connection_id}_topics"
    
    def _get_replies_table_name(self, connection_id):
        """Retrieve table name for replies"""
        return f"{connection_id}_replies"
    
    def init_discourse_forum_seadb_table(self, connection_id):
        """Initialize the SeaDB table of the discourse forum"""
        # Create topics table
        topics_table_name = self._get_topics_table_name(connection_id)
        res = self.seadb_api.create_table(self.base_id, topics_table_name)
        topics_table_id = res['table_id']
        for column in DISCOURSE_TOPICS_COLUMNS:
            mapped_column = {
                'column_name': column['name'],
                'column_type': column['type'],
            }
            self.seadb_api.add_column(self.base_id, topics_table_id, mapped_column)
        
        # Create replies table
        replies_table_name = self._get_replies_table_name(connection_id)
        res = self.seadb_api.create_table(self.base_id, replies_table_name)
        replies_table_id = res['table_id']
        for column in DISCOURSE_REPLIES_COLUMNS:
            mapped_column = {
                'column_name': column['name'],
                'column_type': column['type'],
            }
            self.seadb_api.add_column(self.base_id, replies_table_id, mapped_column)
    
    # Topics operations
    def insert_topic(self, connection_id, topic_data):
        """Insert a single topic record"""
        table_name = self._get_topics_table_name(connection_id)
        rows = [topic_data]
        return self.seadb_api.insert_rows(self.base_id, table_name, rows)
    
    def get_topic_by_id(self, connection_id, topic_id):
        """Retrieve topic record based on topic_id"""
        table_name = self._get_topics_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE `topic_id` = {topic_id}"
        result = self.seadb_api.query_rows(self.base_id, sql)
        if result and 'results' in result and result['results']:
            return result['results'][0]
        return None
    
    def get_topics_by_connection_id(self, connection_id, need_index=None, topic_id=None):
        """Retrieve all topics for the specified connection_id."""
        table_name = self._get_topics_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}`"
        if need_index is not None:
            sql += f" WHERE `need_index` = {True if need_index else False}"
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
        table_name = self._get_replies_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE `topic_id` = {topic_id} AND `connection_id` = {connection_id} AND `post_number` = {post_number}"
        result = self.seadb_api.query_rows(self.base_id, sql)
        if result and 'results' in result and result['results']:
            return result['results'][0]
        return None

    def update_topic_index_status(self, connection_id, topic_id, need_index=True):
        table_name = self._get_topics_table_name(connection_id)
        topics = self.get_topics_by_connection_id(connection_id, topic_id=topic_id)
        
        topic = topics[0]
        pk = topic.get('_pk')
        updates = [{
            "pk": pk,
            "row": {
                "need_index": need_index
            }
        }]
        
        return self.seadb_api.update_rows(
            base_id=self.base_id,
            table_name=table_name,
            rows=updates
        )

    def update_topic(self, connection_id, topic_data):
        table_name = self._get_topics_table_name(connection_id)
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
        table_name = self._get_topics_table_name(connection_id)
        topics = self.get_topics_by_connection_id(connection_id, topic_id=topic_id)
        primary_key = topics[0].get('_pk')
        return self.seadb_api.delete_rows(self.base_id, table_name, [primary_key])
    
    def get_topic_by_id(self, connection_id, topic_id):
        """Retrieve topic record based on topic_id"""
        return self.get_topic_by_id(connection_id, topic_id)
    
    # Replies operations
    def insert_reply(self, connection_id, reply_data):
        """Insert a single reply record"""
        table_name = self._get_replies_table_name(connection_id)
        rows = [reply_data]
        return self.seadb_api.insert_rows(self.base_id, table_name, rows)
    
    def update_reply(self, connection_id, topic_id, post_number, reply_data):
        """Update reply records"""
        reply = self.get_reply_by_id(connection_id, topic_id, post_number)
        pk = reply.get('_pk')

        table_name = self._get_replies_table_name(connection_id)

        reply_data['topic_id'] = topic_id
        reply_data['connection_id'] = connection_id
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
        table_name = self._get_replies_table_name(connection_id)
        reply = self.get_reply_by_id(connection_id, topic_id, post_number)
        primary_key = reply.get('_pk')
        return self.seadb_api.delete_rows(self.base_id, table_name, [primary_key])

    def delete_replies_by_topic(self, connection_id, topic_id):
        """Delete all replies for the specified topic."""
        table_name = self._get_replies_table_name(connection_id)
        sql = f"DELETE FROM `{table_name}` WHERE topic_id = {topic_id}"
        result = self.seadb_api.query_rows(self.base_id, sql)
        return result
    
    def get_replies_by_topic(self, connection_id, topic_id):
        """Retrieve all replies for the specified topic."""
        table_name = self._get_replies_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE `topic_id` = {topic_id}"
        result = self.seadb_api.query_rows(self.base_id, sql)
        if result and 'results' in result:
            return result['results']
        return []
    
    def get_reply_by_id(self, connection_id, topic_id, post_number):
        """Get discourse reply by topic_id and post_number"""
        table_name = self._get_replies_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE `topic_id` = {topic_id} AND `connection_id` = {connection_id} AND `post_number` = {post_number}"
        result = self.seadb_api.query_rows(self.base_id, sql)
        if result and 'results' in result and result['results']:
            return result['results'][0]
        return None
    
    # Combined operations
    def delete_all_data_by_connection_id(self, connection_id):
        """Delete all data for the specified connection_id."""
        deleted_count = 0
        
        # Delete the topics 0-
        topics_table = self._get_topics_table_name(connection_id)
        try:
            topics_sql = f"DELETE FROM `{topics_table}`"
            self.seadb_api.query_rows(self.base_id, topics_sql)
            logger.info(f"Deleted all topics from {topics_table}")
        except Exception as e:
            logger.error(f"Error deleting topics from {topics_table}: {e}")
        
        # Delete the replies table
        replies_table = self._get_replies_table_name(connection_id)
        try:
            replies_sql = f"DELETE FROM `{replies_table}`"
            self.seadb_api.query_rows(self.base_id, replies_sql)
            logger.info(f"Deleted all replies from {replies_table}")
        except Exception as e:
            logger.error(f"Error deleting replies from {replies_table}: {e}")
        
        return deleted_count
    
    def delete_topic_from_db(self, topic_id, connection_id):
        """Delete topic from database"""
        table_name = self._get_topics_table_name(connection_id)
        sql = f"DELETE FROM `{table_name}` WHERE topic_id = '{topic_id}'"
        return self.seadb_api.query_rows(self.base_id, sql)
    
    def get_connection_data_count(self, connection_id):
        """Retrieve data statistics for the specified connection_id."""
        topics_table = self._get_topics_table_name(connection_id)
        replies_table = self._get_replies_table_name(connection_id)
        
        # Obtain the number of topics
        topics_sql = f"SELECT COUNT(*) as count FROM `{topics_table}`"
        topics_result = self.seadb_api.query_rows(self.base_id, topics_sql)
        topic_count = 0
        if topics_result and 'results' in topics_result and topics_result['results']:
            topic_count = topics_result['results'][0].get('count', 0)
        
        # Obtain the number of replies
        replies_sql = f"SELECT COUNT(*) as count FROM `{replies_table}`"
        replies_result = self.seadb_api.query_rows(self.base_id, replies_sql)
        reply_count = 0
        if replies_result and 'results' in replies_result and replies_result['results']:
            reply_count = replies_result['results'][0].get('count', 0)
        
        return {
            'topics': topic_count,
            'replies': reply_count
        }
