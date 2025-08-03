import logging
import requests
import time
import json
import datetime

from sqlalchemy import text

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.constants import SyncStatus
from seaqa_indexer.utils import decrypt_config
from seaqa_indexer.mq import get_mq
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

logger = logging.getLogger(__name__)


def parse_discourse_date(date_str):
    if not date_str:
        return None
    try:
        # Handle UTC time format
        if date_str.endswith('Z'):
            date_str = date_str[:-1] + '+00:00'
        dt = datetime.datetime.fromisoformat(date_str)
        if dt.tzinfo is not None:
            dt = dt.utctimetuple()
            dt = datetime.datetime(*dt[:6])
        return dt
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to parse date string '{date_str}': {e}")
        return None

class DiscourseSyncManager:
    def __init__(self):
        self.db_session = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def process_webhook_event(self, forum, topic_id, event_type, data):
        try:

            success = False
            if event_type in ['topic_created', 'topic_recovered', 'topic_edited', 'post_created', 'post_edited', 'post_recovered']:
                success = self._handle_database_update(forum, topic_id, data, event_type)
            elif event_type in ['topic_destroyed', 'post_destroyed']:
                success = self._handle_database_delete(forum, topic_id, data, event_type)
            else:
                logger.warning(f"Unknown event type: {event_type}")
                success = True

            if success:
                logger.info(f"Successfully processed {event_type} for topic {topic_id}")
            else:
                logger.error(f"Failed to process {event_type} for topic {topic_id}")

            return success

        except Exception as e:
            logger.exception(f"Error processing webhook event: {str(e)}")
            return False

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

    def get_project_discourse_forums_by_page(self, start, limit):
        """Get discourse forums by page"""
        with self.db_session() as session:
            sql = """SELECT pc.id as connection_id, pc.config, pc.project_id, pc.status, p.uuid
            FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.type='discourse_forum' LIMIT :start, :limit"""
            forums = session.execute(text(sql), {'start': start, 'limit': limit}).fetchall()
            return forums

    def get_latest_updated_at(self, connection_id):
        """Get the latest updated_at timestamp for a connection"""
        with self.db_session() as session:
            sql = """SELECT updated_at FROM discourse_topics 
                     WHERE connection_id = :connection_id 
                     ORDER BY updated_at DESC LIMIT 1"""
            result = session.execute(text(sql), {'connection_id': connection_id}).fetchone()
            return result[0] if result else None

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

    def insert_topic(self, topic_data, connection_id):
        """Insert a new topic"""
        with self.db_session() as session:
            sql = """INSERT INTO discourse_topics 
                     (topic_id, title, slug, views, category_id, connection_id, created_at, updated_at, bumped_at) 
                     VALUES (:topic_id, :title, :slug, :views, :category_id, :connection_id, :created_at, :updated_at, :bumped_at)"""
            session.execute(text(sql), {
                'topic_id': topic_data['topic_id'],
                'title': topic_data.get('title'),
                'slug': topic_data.get('slug'),
                'views': topic_data.get('views', 0),
                'category_id': topic_data.get('category_id'),
                'connection_id': connection_id,
                'created_at': topic_data.get('created_at', datetime.datetime.now(datetime.UTC)),
                'updated_at': topic_data.get('updated_at', datetime.datetime.now(datetime.UTC)),
                'bumped_at': topic_data.get('bumped_at', datetime.datetime.now(datetime.UTC))
            })
            session.commit()

    def insert_reply(self, reply_data, connection_id):
        """Insert a new reply"""
        with self.db_session() as session:
            sql = """INSERT INTO discourse_replies 
                     (topic_id, post_number, content, author, connection_id, created_at, updated_at) 
                     VALUES (:topic_id, :post_number, :content, :author, :connection_id, :created_at, :updated_at)"""
            session.execute(text(sql), {
                'topic_id': reply_data['topic_id'],
                'post_number': reply_data.get('post_number'),
                'content': reply_data.get('content'),
                'author': reply_data.get('author'),
                'connection_id': connection_id,
                'created_at': reply_data.get('created_at', datetime.datetime.now(datetime.UTC)),
                'updated_at': reply_data.get('updated_at', datetime.datetime.now(datetime.UTC))
            })
            session.commit()

    def update_topic(self, topic_data, topic_id, connection_id):
        """Update an existing topic"""
        with self.db_session() as session:
            sql = """UPDATE discourse_topics 
                       SET title = :title, slug = :slug, views = :views, 
                           category_id = :category_id, updated_at = :updated_at 
                       WHERE topic_id = :topic_id AND connection_id = :connection_id"""
            session.execute(text(sql), {
                'title': topic_data.get('title'),
                'slug': topic_data.get('slug'),
                'views': topic_data.get('views', 0),
                'category_id': topic_data.get('category_id'),
                'updated_at': topic_data.get('updated_at', datetime.datetime.now(datetime.UTC)),
                'topic_id': topic_id,
                'connection_id': connection_id
            })
            session.commit()

    def check_existing_post(self, topic_id, post_number, connection_id):
        """Check if a post already exists"""
        with self.db_session() as session:
            sql =  """SELECT id FROM discourse_replies 
                      WHERE topic_id = :topic_id AND post_number = :post_number AND connection_id = :connection_id"""
            return session.execute(text(sql), {
                'topic_id': topic_id,
                'post_number': post_number,
                'connection_id': connection_id
            }).fetchone()

    def update_post(self, post_data, topic_id, post_number, connection_id):
        """Update an existing post"""
        with self.db_session() as session:
            sql = """UPDATE discourse_replies 
                       SET content = :content, author = :author, updated_at = :updated_at 
                       WHERE topic_id = :topic_id AND post_number = :post_number AND connection_id = :connection_id"""
            session.execute(text(sql), {
                'content': post_data.get('content', ''),
                'author': post_data.get('author', ''),
                'updated_at': post_data.get('updated_at', datetime.datetime.now(datetime.UTC)),
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

    def delete_post(self, topic_id, post_number, connection_id):
        """Delete a specific post"""
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
        if discourse_connection.status == SyncStatus.SYNCING:
            logger.warning('discourse forum: %s is syncing', connection_id)
            return
        self.update_status(connection_id, SyncStatus.SYNCING)
        logger.info(f"Start syncing Discourse forum {connection_id}")
        config = json.loads(discourse_connection.config)
        config = decrypt_config(config)
        df_url = config['url']
        df_api_key = config['api_key']
        df_api_username = config['api_username']

        self.sync_discourse_latest_topics(df_url, df_api_key, df_api_username, connection_id)

    def update_discourse_forums(self):
        """Check if all discourse forums need to be updated"""

        start, limit = 0, 1000
        while True:
            try:
                project_forums = self.get_project_discourse_forums_by_page(start, limit)
            except Exception as e:
                logger.error("Error: %s" % e)
                return
            start += 1000
            if len(project_forums) == 0:
                break
            for forum in project_forums:
                connection_id = forum.connection_id

                try:
                    self.add_discourse_sync_task(connection_id)
                    logger.debug('Successfully queued discourse forum sync task: %s', connection_id)
                except Exception as e:
                    logger.warning('Queue discourse forum sync task failed, connection_id: %s, error: %s', connection_id, e)
                    self.update_status(connection_id, SyncStatus.FAILED)

    def add_discourse_sync_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('discourse_sync_task', json.dumps(msg_content))

    def fetch_latest_topics_page(self, forum_url, headers, page):
        try:
            url = f"{forum_url}/latest.json?page={page}"
            logger.debug(f"Fetching page {page} from {url}")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            topics = data.get("topic_list", {}).get("topics", [])
            return topics
        except Exception as e:
            logger.error(f"Failed to fetch topics from page {page}: {e}")
            return []
    
    def sync_discourse_latest_topics(self, forum_url, api_key, api_username, connection_id):
        """Sync latest topics from Discourse forum"""
        headers = self._build_api_headers(api_key, api_username)
        
        sync_context = self._prepare_sync_context(connection_id)
        
        page = 0
        while True:
            topics = self.fetch_latest_topics_page(forum_url, headers, page)
            
            if not topics:
                logger.debug(f"No more topics found on page {page}")
                break
                
            should_stop = self._process_topics_batch(topics, sync_context, forum_url, headers, connection_id)
            if should_stop:
                break
                
            page += 1
            
        return True

    def fetch_discourse_topic_posts(self, forum_url, topic_id, headers):
        """Fetch detailed topic content including all posts"""
        try:
            url = f"{forum_url}/t/{topic_id}.json"
            logger.debug(f"Fetching topic detail from {url}")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()

            topic_data = response.json()
            posts = []
            post_stream = topic_data.get('post_stream', {})

            for post in post_stream.get('posts', []):
                # Only process regular posts (post_type=1), ignore system posts (post_type=4)
                if post.get('post_type') != 1:
                    continue
                post_info = {
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
                posts.append(post_info)

            return {
                'posts': posts,
            }

        except requests.RequestException as e:
            logger.error(f"HTTP error fetching topic {topic_id}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error parsing topic {topic_id}: {str(e)}")
            return None

    def _handle_database_update(self, forum, topic_id, data, event_type):
        try:
            if event_type in ['topic_created', 'topic_recovered', 'topic_edited']:
                topic_data = data.get('topic', {})
                if not topic_data:
                    logger.warning(f"No topic data found for event {event_type}")
                    return False

                existing_topic = self.check_existing_topic(topic_id, forum.id)

                if existing_topic:
                    update_data = {
                        'title': topic_data.get('title'),
                        'slug': topic_data.get('slug'),
                        'views': topic_data.get('views', 0),
                        'category_id': topic_data.get('category_id'),
                        'updated_at': parse_discourse_date(topic_data.get('updated_at')) or datetime.datetime.utcnow()
                    }
                    self.update_topic(update_data, topic_id, forum.id)
                    logger.info(f"Updated existing topic {topic_id}")
                else:
                    insert_data = {
                        'topic_id': topic_id,
                        'title': topic_data.get('title', ''),
                        'slug': topic_data.get('slug', ''),
                        'views': topic_data.get('views', 0),
                        'category_id': topic_data.get('category_id'),
                        'connection_id': forum.id,
                        'created_at': parse_discourse_date(topic_data.get('created_at')) or datetime.datetime.utcnow(),
                        'updated_at': parse_discourse_date(topic_data.get('updated_at')) or datetime.datetime.utcnow(),
                        'bumped_at': parse_discourse_date(topic_data.get('bumped_at')) or datetime.datetime.utcnow()
                    }
                    self.insert_topic(insert_data, forum.id)
                    logger.info(f"Created new topic {topic_id}")

            elif event_type in ['post_created', 'post_edited', 'post_recovered']:
                post_data = data.get('post', {})
                if not post_data:
                    logger.warning(f"No post data found for event {event_type}")
                    return False

                post_number = post_data.get('post_number')
                if not post_number:
                    logger.warning(f"No post_number found for event {event_type}")
                    return False

                existing_post = self.check_existing_post(topic_id, post_number, forum.id)

                if existing_post:
                    update_data = {
                        'content': post_data.get('cooked', ''),
                        'author': post_data.get('username', ''),
                        'updated_at': parse_discourse_date(post_data.get('updated_at')) or datetime.datetime.utcnow()
                    }
                    self.update_post(update_data, topic_id, post_number, forum.id)
                    logger.info(f"Updated existing post {post_number} in topic {topic_id}")
                else:
                    insert_data = {
                        'topic_id': topic_id,
                        'post_number': post_number,
                        'content': post_data.get('cooked', ''),
                        'author': post_data.get('username', ''),
                        'created_at': parse_discourse_date(post_data.get('created_at')) or datetime.datetime.utcnow(),
                        'updated_at': parse_discourse_date(post_data.get('updated_at')) or datetime.datetime.utcnow()
                    }
                    self.insert_reply(insert_data, forum.id)
                    logger.info(f"Created new post {post_number} in topic {topic_id}")

            return True

        except Exception as e:
            logger.exception(f"Error updating database for event {event_type}: {str(e)}")
            return False

    def _handle_database_delete(self, forum, topic_id, data, event_type):
        try:
            if event_type == 'topic_destroyed':
                self.delete_topic_and_replies(topic_id, forum.id)
                logger.info(f"Deleted topic {topic_id} and all its replies")

            elif event_type == 'post_destroyed':
                post_data = data.get('post', {})
                post_number = post_data.get('post_number')
                if post_number:
                    self.delete_post(topic_id, post_number, forum.id)
                    logger.info(f"Deleted post {post_number} in topic {topic_id}")
            return True
        except Exception as e:
            logger.exception(f"Error deleting from database for event {event_type}: {str(e)}")
            return False

    def handle_webhook_request(self, connection_id, request_data, event_type):
        """Handle complete webhook request processing with connection validation"""
        try:
            # Find forum by connection_id
            forum = self.get_forum_by_connection_id(connection_id)
            if not forum:
                return {
                    'success': False,
                    'error': f'Forum not found for connection_id {connection_id}',
                    'status_code': 404
                }
            
            # Validate request data
            if not request_data:
                return {
                    'success': False,
                    'error': 'No data received',
                    'status_code': 400
                }
            
            if not event_type:
                return {
                    'success': False,
                    'error': 'Event type not specified',
                    'status_code': 400
                }
            
            # Extract topic_id
            topic_id = request_data.get('topic', {}).get('id')
            if not topic_id:
                return {
                    'success': False,
                    'error': 'Topic ID not found in request data',
                    'status_code': 400
                }
            
            # Process the webhook event
            success = self.process_webhook_event(forum, topic_id, event_type, request_data)
            
            if success:
                return {
                    'success': True,
                    'message': f'Successfully processed {event_type} for topic {topic_id}',
                    'status_code': 200
                }
            else:
                return {
                    'success': False,
                    'error': f'Failed to process {event_type} for topic {topic_id}',
                    'status_code': 500
                }
                
        except Exception as e:
            logger.exception(f"Error handling webhook request: {str(e)}")
            return {
                'success': False,
                'error': f'Internal server error: {str(e)}',
                'status_code': 500
            }

    def _build_api_headers(self, api_key, api_username):
        """Build API headers for Discourse requests"""
        return {
            "Api-Key": api_key,
            "Api-Username": api_username
        }

    def _prepare_sync_context(self, connection_id, db_session):
        """Prepare synchronization context"""
        latest_local_updated_at = self.get_latest_updated_at(connection_id, db_session)
        is_initial_sync = latest_local_updated_at is None
        
        logger.debug(f"Starting sync (initial_sync: {is_initial_sync})")
        
        return {
            'latest_local_updated_at': latest_local_updated_at,
            'is_initial_sync': is_initial_sync
        }

    def _process_topics_batch(self, topics, sync_context, forum_url, headers, connection_id, db_session):
        """Process a batch of topics, returns True if sync should stop"""
        for topic_data in topics:
            try:
                should_stop = self._process_single_topic(
                    topic_data, sync_context, forum_url, headers, connection_id, db_session
                )
                if should_stop:
                    return True
                    
                time.sleep(0.05)
            except Exception as e:
                db_session.rollback()
                logger.error(f"Error processing topic {topic_data.get('id', 'unknown')}: {str(e)}")
                continue
                
            return False

    def _process_single_topic(self, topic_data, sync_context, forum_url, headers, connection_id, db_session):
        """Process a single topic, returns True if sync should stop"""
        topic_id = topic_data.get('id')
        if not topic_id:
            return False
            
        # Check if we should stop syncing based on timestamp
        if self._should_stop_sync(topic_data, sync_context):
            return True
            
        existing_topic = self.check_existing_topic(topic_id, connection_id, db_session)
        
        # Handle different sync scenarios
        if sync_context['is_initial_sync']:
            if existing_topic:
                logger.debug(f"Topic {topic_id} already exists, skipping.")
                return False
        else:
            if existing_topic:
                self._update_existing_topic(topic_data, topic_id, connection_id, db_session)
                return False
                
        # Create new topic with posts
        self._create_topic_with_posts(topic_data, topic_id, forum_url, headers, connection_id, db_session)
        return False

    def _should_stop_sync(self, topic_data, sync_context):
        """Check if sync should stop based on topic timestamp"""
        if sync_context['is_initial_sync'] or not sync_context['latest_local_updated_at']:
            return False
            
        topic_bumped_at = parse_discourse_date(topic_data.get('bumped_at'))
        if topic_bumped_at and topic_bumped_at <= sync_context['latest_local_updated_at']:
            # Skip pinned topics
            if topic_data.get('pinned'):
                logger.debug(f"Topic {topic_data.get('id')} is pinned, continuing sync.")
                return False
                
            logger.debug(f"Found topic with bumped_at ({topic_bumped_at}) <= latest local updated_at ({sync_context['latest_local_updated_at']}), stopping sync")
            return True
            
        return False

    def _update_existing_topic(self, topic_data, topic_id, connection_id, db_session):
        """Update existing topic's bumped_at timestamp"""
        try:
            topic_bumped_at = parse_discourse_date(topic_data.get('bumped_at'))
            self.update_topic_bumped_at(topic_id, connection_id, topic_bumped_at, db_session)
            db_session.commit()
            logger.debug(f"Successfully updated topic {topic_id}")
        except Exception as e:
            db_session.rollback()
            logger.error(f"Error updating topic {topic_id}: {str(e)}")

    def _create_topic_with_posts(self, topic_data, topic_id, forum_url, headers, connection_id, db_session):
        """Create new topic and its posts"""
        # Fetch topic posts
        topic_posts_data = self.fetch_discourse_topic_posts(forum_url, topic_id, headers)
        if not topic_posts_data:
            logger.warning(f"Failed to fetch posts for topic {topic_id}")
            return
            
        # Create topic
        self._insert_topic_data(topic_data, topic_id, connection_id, db_session)
        
        # Create posts
        self._insert_topic_posts(topic_posts_data, topic_id, connection_id, db_session)

    def _insert_topic_data(self, topic_data, topic_id, connection_id, db_session):
        """Insert topic data into database"""
        try:
            topic_insert_data = {
                'topic_id': topic_id,
                'title': topic_data.get('title'),
                'slug': topic_data.get('slug'),
                'views': topic_data.get('views', 0),
                'category_id': topic_data.get('category_id'),
                'created_at': datetime.datetime.now(datetime.UTC),
                'updated_at': datetime.datetime.now(datetime.UTC),
                'bumped_at': parse_discourse_date(topic_data.get('bumped_at')) or datetime.datetime.now(datetime.UTC)
            }
            self.insert_topic(topic_insert_data, connection_id, db_session)
            db_session.commit()
            logger.debug(f"Successfully saved topic {topic_id}")
        except Exception as e:
            db_session.rollback()
            logger.error(f"Error saving topic {topic_id}: {str(e)}")
            raise

    def _insert_topic_posts(self, topic_posts_data, topic_id, connection_id, db_session):
        """Insert topic posts into database"""
        try:
            for post in topic_posts_data.get('posts', []):
                reply_data = {
                    'topic_id': topic_id,
                    'post_number': post.get('post_number'),
                    'content': post.get('cooked'),
                    'author': post.get('username'),
                    'created_at': datetime.datetime.now(datetime.UTC),
                    'updated_at': datetime.datetime.now(datetime.UTC)
                }
                self.insert_reply(reply_data, connection_id, db_session)
                
            db_session.commit()
            logger.debug(f"Successfully processed and saved topic {topic_id} with replies")
        except Exception as e:
            db_session.rollback()
            logger.error(f"Error saving replies for topic {topic_id}: {str(e)}")
            raise
