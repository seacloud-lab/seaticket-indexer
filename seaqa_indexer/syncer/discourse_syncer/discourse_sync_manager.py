import logging
import requests
import time
import json
import datetime

from sqlalchemy import text

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.constants import CrawlStatus
from seaqa_indexer.utils import decrypt_config, time_str_to_utc_time
from seaqa_indexer.mq import get_mq
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.seasearch.index_manager import IndexManager

logger = logging.getLogger(__name__)


class ForumNotFoundException(Exception):
    pass


class DiscourseSyncManager:
    def __init__(self):
        self.db_session = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def _handle_topic_update(self, forum, topic_id, data):
        topic_data = data.get('topic', {})
        existing_topic = self.check_existing_topic(topic_id, forum.id)

        title = topic_data.get('title')
        slug = topic_data.get('slug')
        views = topic_data.get('views', 0)
        category_id = topic_data.get('category_id')
        if existing_topic:
            self.update_topic(title, slug, views, category_id, topic_id, forum.id)
            logger.debug(f"Updated existing topic {topic_id}")
        else:
            bumped_at = time_str_to_utc_time(topic_data.get('bumped_at')) or datetime.datetime.now(datetime.UTC)

            self.insert_topic(topic_id, title, slug, views, category_id, forum.id, bumped_at)
            logger.debug(f"Created new topic {topic_id}")

    def _handle_reply_update(self, forum, topic_id, data):
        reply_data = data.get('post', {})
        post_number = reply_data.get('post_number')

        existing_reply = self.check_existing_reply(topic_id, post_number, forum.id)
        content = reply_data.get('cooked', '')
        author = reply_data.get('username', '')
        if existing_reply:
            self.update_reply(content, author, topic_id, post_number, forum.id)
            logger.debug(f"Updated existing reply {post_number} in topic {topic_id}")
        else:
            self.insert_reply(topic_id, post_number, content, author, forum.id)
            logger.debug(f"Created new reply {post_number} in topic {topic_id}")

    def _handle_reply_delete(self, forum, topic_id, data):
        reply_data = data.get('post', {})
        post_number = reply_data.get('post_number')
        if post_number:
            self.delete_reply(topic_id, post_number, forum.id)
            logger.info(f"Deleted reply {post_number} in topic {topic_id}")

    def get_project_connection_by_id(self, pc_id, conn_type='discourse_forum'):
        with self.db_session() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid
            FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.type=:conn_type AND pc.id=:pc_id"""
            connection = session.execute(text(sql), {'pc_id': pc_id, 'conn_type': conn_type}).fetchone()
            return connection

    def get_forum_by_connection_id(self, forum_id):
        with self.db_session() as session:
            sql = "SELECT * FROM project_connection WHERE id = :forum_id AND type = 'discourse_forum'"
            forum = session.execute(text(sql), {'forum_id': forum_id}).fetchone()
            return forum

    def update_status(self, connection_id, status):
        with self.db_session() as session:
            cmd = "UPDATE project_connection SET status = :status WHERE id = :connection_id"
            session.execute(text(cmd), {'connection_id': connection_id, 'status': status})
            session.commit()

    def update_connection_updated_at(self, connection_id, updated_at):
        """Update connection's updated_at timestamp"""
        with self.db_session() as session:
            sql = """UPDATE project_connection
                       SET updated_at = :updated_at
                       WHERE id = :connection_id"""
            session.execute(text(sql), {
                'updated_at': updated_at,
                'connection_id': connection_id
            })
            session.commit()

    def get_project_connections_by_page(self, start, limit):
        """Get discourse forums by page"""
        with self.db_session() as session:
            sql = """SELECT pc.id as connection_id, pc.config, pc.project_id, pc.status, p.uuid
            FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.type='discourse_forum' LIMIT :start, :limit"""
            forums = session.execute(text(sql), {'start': start, 'limit': limit}).fetchall()
            return forums

    def get_project_connection_updated_at(self, connection_id):
        """Get the latest updated_at timestamp for a connection"""
        with self.db_session() as session:
            sql = """SELECT updated_at FROM project_connection
                     WHERE id = :connection_id"""
            result = session.execute(text(sql), {'connection_id': connection_id}).fetchone()
            if result and result[0]:
                dt = result[0]
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                return dt
            return None

    def check_existing_topic(self, topic_id, connection_id):
        """Check if a topic already exists"""
        with self.db_session() as session:
            sql = """SELECT id FROM discourse_topics 
                      WHERE topic_id = :topic_id AND connection_id = :connection_id"""
            return session.execute(text(sql), {'topic_id': topic_id, 'connection_id': connection_id}).fetchone()

    def update_topic_bumped_at(self, topic_id, connection_id, bumped_at):
        """Update topic's bumped_at timestamp"""
        with self.db_session() as session:
            sql = """UPDATE discourse_topics 
                       SET bumped_at = :bumped_at 
                       WHERE topic_id = :topic_id AND connection_id = :connection_id"""

            session.execute(text(sql), {
                'bumped_at': bumped_at or datetime.datetime.now(datetime.UTC),
                'topic_id': topic_id,
                'connection_id': connection_id
            })
            session.commit()

    def insert_topic(self, topic_id, title, slug, views, category_id, connection_id, bumped_at):
        """Insert a new topic"""
        with self.db_session() as session:
            sql = """INSERT INTO discourse_topics 
                     (topic_id, title, slug, views, category_id, connection_id, bumped_at, need_index) 
                     VALUES (:topic_id, :title, :slug, :views, :category_id, :connection_id, :bumped_at, :need_index)
                     ON DUPLICATE KEY UPDATE
                         title = VALUES(title),
                         slug = VALUES(slug),
                         views = VALUES(views),
                         category_id = VALUES(category_id),
                         bumped_at = VALUES(bumped_at),
                         need_index = 1"""
            session.execute(text(sql), {
                'topic_id': topic_id,
                'title': title,
                'slug': slug,
                'views': views,
                'category_id': category_id,
                'connection_id': connection_id,
                'bumped_at': bumped_at,
                'need_index': 1
            })
            session.commit()

    def insert_reply(self, topic_id, post_number, content, author, connection_id):
        """Insert a new reply"""
        with self.db_session() as session:
            sql = """INSERT INTO discourse_replies 
                     (topic_id, post_number, content, author, connection_id, need_index) 
                     VALUES (:topic_id, :post_number, :content, :author, :connection_id, :need_index)
                     ON DUPLICATE KEY UPDATE
                         content = VALUES(content),
                         author = VALUES(author),
                         need_index = 1"""
            session.execute(text(sql), {
                'topic_id': topic_id,
                'post_number': post_number,
                'content': content,
                'author': author,
                'connection_id': connection_id,
                'need_index': 1
            })
            session.commit()

    def update_topic(self, title, slug, views, category_id, topic_id, connection_id):
        """Update an existing topic"""
        with self.db_session() as session:
            sql = """UPDATE discourse_topics 
                       SET title = :title, slug = :slug, views = :views, 
                           category_id = :category_id 
                       WHERE topic_id = :topic_id AND connection_id = :connection_id"""
            session.execute(text(sql), {
                'title': title,
                'slug': slug,
                'views': views,
                'category_id': category_id,
                'topic_id': topic_id,
                'connection_id': connection_id
            })
            session.commit()

    def check_existing_reply(self, topic_id, post_number, connection_id):
        """Check if a reply already exists"""
        with self.db_session() as session:
            sql = """SELECT id FROM discourse_replies 
                      WHERE topic_id = :topic_id AND post_number = :post_number AND connection_id = :connection_id"""
            return session.execute(text(sql), {
                'topic_id': topic_id,
                'post_number': post_number,
                'connection_id': connection_id
            }).fetchone()

    def update_reply(self, content, author, topic_id, post_number, connection_id):
        """Update an existing reply"""
        with self.db_session() as session:
            sql = """UPDATE discourse_replies 
                       SET content = :content, author = :author 
                       WHERE topic_id = :topic_id AND post_number = :post_number AND connection_id = :connection_id"""
            session.execute(text(sql), {
                'content': content,
                'author': author,
                'topic_id': topic_id,
                'post_number': post_number,
                'connection_id': connection_id
            })
            session.commit()

    def delete_topic_and_replies(self, topic_id, connection_id):
        """Delete a topic and all its replies"""
        with self.db_session() as session:
            # Delete replies first
            delete_replies_sql = """DELETE FROM discourse_replies 
            WHERE topic_id = :topic_id AND connection_id = :connection_id"""
            session.execute(text(delete_replies_sql), {
                'topic_id': topic_id,
                'connection_id': connection_id
            })

            # Delete topic
            delete_topic_sql = """DELETE FROM discourse_topics 
                     WHERE topic_id = :topic_id AND connection_id = :connection_id"""
            session.execute(text(delete_topic_sql), {
                'topic_id': topic_id,
                'connection_id': connection_id
            })
            session.commit()

    def delete_reply(self, topic_id, post_number, connection_id):
        """Delete a specific reply"""
        with self.db_session() as session:
            sql = """DELETE FROM discourse_replies 
            WHERE topic_id = :topic_id AND post_number = :post_number AND connection_id = :connection_id"""
            session.execute(text(sql), {
                'topic_id': topic_id,
                'post_number': post_number,
                'connection_id': connection_id
            })
            session.commit()

    def sync_discourse(self, connection_id):
        """Start syncing Discourse forum"""
        discourse_connection = self.get_project_connection_by_id(connection_id, conn_type='discourse_forum')
        if not discourse_connection:
            logger.warning('discourse forum: %s not found', connection_id)
            return
        if discourse_connection.status == CrawlStatus.CRAWLING:
            logger.warning('discourse forum: %s is syncing', connection_id)
            return
        self.update_status(connection_id, CrawlStatus.CRAWLING)
        logger.info(f"Start syncing Discourse forum {connection_id}")
        config = json.loads(discourse_connection.config)
        config = decrypt_config(config)
        df_url = config['url']
        df_api_key = config['api_key']
        df_api_username = config['api_username']

        latest_bumped_at = self.sync_discourse_latest_topics(df_url, df_api_key, df_api_username, connection_id)
        if latest_bumped_at:
            self.update_connection_updated_at(connection_id, latest_bumped_at)
        self.add_discourse_index_task(connection_id)

    def update_discourse_forums(self):
        """Check if all discourse forums need to be updated"""
        start, limit = 0, 1000
        while True:
            project_connections = self.get_project_connections_by_page(start, limit)
            start += 1000
            if len(project_connections) == 0:
                break
            for project_connection in project_connections:
                self.add_discourse_sync_task(project_connection.connection_id)

    def add_discourse_sync_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('discourse_sync_task', json.dumps(msg_content))

    def fetch_latest_topics_page(self, forum_url, headers, page):
        url = f"{forum_url}/latest.json?page={page}"
        logger.debug(f"Fetching page {page} from {url}")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        data = response.json()
        topics = data.get("topic_list", {}).get("topics", [])
        return topics

    def sync_discourse_latest_topics(self, forum_url, api_key, api_username, connection_id):
        """Sync latest topics from Discourse forum"""
        headers = self._build_api_headers(api_key, api_username)
        last_updated_at = self.get_project_connection_updated_at(connection_id)

        page = 0
        latest_bumped_at = None
        should_stop = False
        while True:
            topics = self.fetch_latest_topics_page(forum_url, headers, page)

            if not topics:
                break
            for topic_data in topics:
                topic_bumped_at = time_str_to_utc_time(topic_data.get('bumped_at'))
                # Check if we should stop syncing based on timestamp
                if topic_bumped_at <= last_updated_at and not topic_data.get('pinned'):
                    should_stop = True
                    break
                if topic_bumped_at <= last_updated_at and topic_data.get('pinned'):
                    # if pinned, not need update latest_bumped_at
                    pass
                else:
                    if not latest_bumped_at:
                        # latest_bumped_at update one time is the last time
                        latest_bumped_at = topic_bumped_at

                topic_id = topic_data.get('id')

                # Handle different sync scenarios
                if not last_updated_at:
                    # Create new topic with replies
                    self._create_topic_with_replies(topic_data, topic_id, forum_url, headers, connection_id)
                else:
                    self._update_existing_topic(topic_data, topic_id, connection_id, forum_url, headers)

            if should_stop:
                break

            page += 1

        return latest_bumped_at if latest_bumped_at else last_updated_at

    def fetch_discourse_topic(self, forum_url, topic_id, headers):
        url = f"{forum_url}/t/{topic_id}.json"
        logger.debug(f"Fetching topic detail from {url}")
        response = requests.get(url, headers=headers, timeout=30)
        return response

    def fetch_discourse_topic_replies(self, forum_url, topic_id, headers):
        """Fetch detailed topic content including all replies"""
        response = self.fetch_discourse_topic(forum_url, topic_id, headers)
        response.raise_for_status()

        topic_data = response.json()
        replies = []
        reply_stream = topic_data.get('post_stream', {})

        for post in reply_stream.get('posts', []):
            # Only process regular replies (post_type=1), ignore system replies (post_type=4)
            if post.get('post_type') != 1:
                continue
            reply_info = {
                'id': post.get('id'),
                'post_number': post.get('post_number'),
                'username': post.get('username'),
                'created_at': post.get('created_at'),
                'updated_at': post.get('updated_at'),
                'cooked': post.get('cooked', ''),
                'raw': post.get('raw', ''),
                'reply_count': post.get('reply_count', 0),
                'like_count': post.get('like_count', 0),
                'reads': post.get('reads', 0),
                'topic_id': post.get('topic_id'),
                'user_id': post.get('user_id')
            }
            replies.append(reply_info)

        return {
            'replies': replies,
        }

    def handle_webhook(self, connection_id, request_data, event_type):
        """Handle complete webhook request processing with connection validation"""
        forum = self.get_forum_by_connection_id(connection_id)
        if not forum:
            raise ForumNotFoundException()

        topic_id = request_data.get('topic', {}).get('id')
        if event_type in ['topic_created', 'topic_recovered', 'topic_edited']:
            self._handle_topic_update(forum, topic_id, request_data)
        elif event_type in ['post_created', 'post_edited', 'post_recovered']:
            self._handle_reply_update(forum, topic_id, request_data)
        elif event_type == 'topic_destroyed':
            self.delete_topic_and_replies(topic_id, forum.id)
        elif event_type == 'post_destroyed':
            self._handle_reply_delete(forum, topic_id, request_data)
        else:
            logger.warning(f"Unknown event type: {event_type}")

        self.update_connection_updated_at(connection_id, time_str_to_utc_time(request_data.get('updated_at')))
        self.add_discourse_index_task(connection_id)

    def _build_api_headers(self, api_key, api_username):
        """Build API headers for Discourse requests"""
        return {
            "Api-Key": api_key,
            "Api-Username": api_username
        }

    def _update_existing_topic(self, topic_data, topic_id, connection_id, forum_url, headers):
        """Deletes and recreates an existing topic and its replies to ensure content is up-to-date."""
        # Delete existing topic and its replies
        self.delete_topic_and_replies(topic_id, connection_id)
        logger.debug(f"Successfully deleted topic {topic_id} and its replies for updating.")

        # Create new topic with replies
        self._create_topic_with_replies(topic_data, topic_id, forum_url, headers, connection_id)
        logger.debug(f"Successfully recreated topic {topic_id} with replies.")

    def _create_topic_with_replies(self, topic_data, topic_id, forum_url, headers, connection_id):
        """Create new topic and its replies"""
        # Fetch topic posts
        topic_replies_data = self.fetch_discourse_topic_replies(forum_url, topic_id, headers)
        if not topic_replies_data:
            logger.warning(f"Failed to fetch replies for topic {topic_id}")
            return

        # Create topic
        self._insert_topic_data(topic_data, topic_id, connection_id)

        # Create replies
        self._insert_topic_replies(topic_replies_data, topic_id, connection_id)

    def _insert_topic_data(self, topic_data, topic_id, connection_id):
        """Insert topic data into database"""
        title = topic_data.get('title')
        slug = topic_data.get('slug')
        views = topic_data.get('views', 0)
        category_id = topic_data.get('category_id')
        bumped_at = time_str_to_utc_time(topic_data.get('bumped_at')) or datetime.datetime.now(datetime.UTC)

        self.insert_topic(topic_id, title, slug, views, category_id, connection_id, bumped_at)
        logger.debug(f"Successfully saved topic {topic_id}")

    def _insert_topic_replies(self, topic_replies_data, topic_id, connection_id):
        """Insert topic replies into database"""
        for reply in topic_replies_data.get('replies', []):
            post_number = reply.get('post_number')
            content = reply.get('cooked')
            author = reply.get('username')

            self.insert_reply(topic_id, post_number, content, author, connection_id)
        logger.debug(f"Successfully processed and saved topic {topic_id} with replies")

    def add_discourse_index_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('index_task', json.dumps(msg_content))
