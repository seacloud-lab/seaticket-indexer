import requests
import time
import logging
import queue
import uuid

from datetime import datetime
from threading import Thread, Lock
from sqlalchemy.sql import text

from web_crawl_index.discourse.dtable_schema import DTableManager
from web_crawl_index.db import init_db_session_class
from web_crawl_index.utils.dtable_db_api import DTableDBAPI
from web_crawl_index.utils.dtable_server_api import DTableServerAPI
from web_crawl_index.utils.dtable_web_api import DTableWebAPI
from web_crawl_index.utils.utils import get_access_token_by_api_token
from config import INNER_DTABLE_DB_URL, INNER_DTABLE_SERVER_URL, \
    DTABLE_WEB_SERVICE_URL


logger = logging.getLogger(__name__)


class DiscourseManager:
    def __init__(self, app):
        self.app = app
        self.lock = Lock()
        self.running_syncs = set()
        self.db_session = init_db_session_class()
        
        self.event_queue = queue.Queue()
        self.queue_worker_running = False
    
    def start(self):
        """Start the queue worker"""
        logger.info("Discourse Manager started with APScheduler")
        self._start_queue_worker()
    
    def stop(self):
        """Stop the scheduler and queue worker"""
        logger.info("Discourse Manager stopping")
        self.scheduler.shutdown()
        self.queue_worker_running = False
    
    def _start_queue_worker(self):
        if not self.queue_worker_running:
            self.queue_worker_running = True
            worker_thread = Thread(target=self._queue_worker, daemon=True)
            worker_thread.start()
            logger.info("Queue worker thread started")
    
    def _queue_worker(self):
        while self.queue_worker_running:
            try:
                event_data = self.event_queue.get(timeout=1)
                
                self._process_single_event(event_data)
                
                self.event_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.exception(f"Error in queue worker: {str(e)}")
    
    def process_webhook_event(self, forum_id, topic_id, event_type, data):
        try:
            event_data = {
                'forum_id': forum_id,
                'topic_id': topic_id,
                'event_type': event_type,
                'data': data,
                'timestamp': datetime.now().isoformat()
            }
            
            self.event_queue.put(event_data)
            logger.info(f"Added event {event_type} for topic {topic_id} to processing queue")
            return True
            
        except Exception as e:
            logger.exception(f"Error adding event to queue: {str(e)}")
            return False
    
    def get_forum_by_id(self, forum_id):
        with self.db_session() as session:
            sql = "SELECT * FROM discourse_forum WHERE id=:forum_id"
            forum = session.execute(text(sql), {'forum_id': forum_id}).fetchone()
            return forum
    
    def update_forum_status(self, forum_id, status):
        with self.db_session() as session:
            cmd = """UPDATE discourse_forum SET status = :status WHERE id = :forum_id"""
            session.execute(text(cmd), {'forum_id': forum_id, 'status': status})
            session.commit()
    
    def update_forum_table_name(self, forum_id, table_name):
        with self.db_session() as session:
            cmd = """UPDATE discourse_forum SET table_name = :table_name WHERE id = :forum_id"""
            session.execute(text(cmd), {'forum_id': forum_id, 'table_name': table_name})
            session.commit()
    
    def update_forum_last_synced(self, forum_id, status):
        last_synced_at = datetime.now()
        with self.db_session() as session:
            cmd = """UPDATE discourse_forum SET status = :status, last_synced_at = :last_synced_at WHERE id = :forum_id"""
            session.execute(text(cmd), {'forum_id': forum_id, 'status': status, 'last_synced_at': last_synced_at})
            session.commit()
    
    def _process_single_event(self, event_data):
        try:
            forum_id = event_data['forum_id']
            topic_id = event_data['topic_id']
            event_type = event_data['event_type']
            data = event_data['data']
            
            forum = self.get_forum_by_id(forum_id)
            if not forum:
                logger.error(f"Forum {forum_id} not found for event {event_type}")
                return False
            
            if not forum.dtable_api_token or not forum.dtable_uuid or not forum.table_name:
                logger.warning(f"Forum {forum.name} not properly configured for dtable operations")
                return False
            
            access_token = get_access_token_by_api_token(forum.dtable_api_token)
            if not access_token:
                logger.error(f"Failed to get dtable access token for forum {forum.name}")
                return False
            
            dtable_db_api = DTableDBAPI(INNER_DTABLE_DB_URL, forum.dtable_uuid, access_token)
            
            success = False
            if event_type in ['topic_created', 'topic_recovered', 'topic_edited', 'post_created', 'post_edited', 'post_recovered']:
                success = self._handle_dtable_update(forum, topic_id, data, dtable_db_api)
            elif event_type in ['topic_destroyed', 'post_destroyed']:
                success = self._handle_dtable_delete(forum, topic_id, data, dtable_db_api)
            else:
                logger.warning(f"Unknown event type: {event_type}")
                success = True
            
            if success:
                logger.info(f"Successfully processed {event_type} for topic {topic_id}")
            else:
                logger.error(f"Failed to process {event_type} for topic {topic_id}")
            
            return success
            
        except Exception as e:
            logger.exception(f"Error processing event: {str(e)}")
            return False
    
    def _handle_dtable_update(self, forum, topic_id, data, dtable_db_api):
        try:
            headers = {
                "Api-Key": forum.api_key,
                "Api-Username": forum.api_username
            }
            
            topic_detail = self._fetch_topic_posts(forum, topic_id, headers)
            if not topic_detail:
                logger.error(f"Failed to fetch topic detail for topic {topic_id}")
                return False
            
            topic_data = {
                'id': topic_id,
                'title': data.get('topic', {}).get('title', ''),
                'slug': data.get('topic', {}).get('slug', ''),
                'views': data.get('topic', {}).get('views', 0),
                'category_id': data.get('topic', {}).get('category_id'),
                'posts': topic_detail
            }
            
            try:
                DTableManager.delete_big_data_table_row(dtable_db_api, forum.table_name, topic_id)
            except Exception:
                pass
            
            DTableManager.batch_insert_topics(dtable_db_api, forum.table_name, [topic_data])
            
            return True
            
        except Exception as e:
            logger.exception(f"Error in _handle_dtable_update for topic {topic_id}: {str(e)}")
            return False
    
    def _handle_dtable_delete(self, forum, topic_id, data, dtable_db_api):
        try:
            event_type = data.get('event_type', '')
            
            if event_type == 'topic_destroyed':
                DTableManager.delete_big_data_table_row(dtable_db_api, forum.table_name, topic_id)
                logger.info(f"Deleted topic {topic_id} from dtable")
                
            elif event_type == 'post_destroyed':
                post_data = data.get('post', {})
                post_number = post_data.get('post_number', 1)
                
                if post_number == 1:
                    DTableManager.delete_big_data_table_row(dtable_db_api, forum.table_name, topic_id)
                    logger.info(f"Deleted topic {topic_id} from dtable (first post deleted)")
                else:
                    return self._handle_dtable_update(forum, topic_id, data, dtable_db_api)
            
            return True
            
        except Exception as e:
            logger.exception(f"Error in _handle_dtable_delete for topic {topic_id}: {str(e)}")
            return False

    def sync_forum(self, forum_id):
        """Synchronize forum content asynchronously"""
        with self.lock:
            if forum_id in self.running_syncs:
                logger.warning(f"Forum {forum_id} is already being synced")
                return {'status': 'already_running', 'message': 'Forum is already being synced'}
            self.running_syncs.add(forum_id)
        
        try:
            forum = self.get_forum_by_id(forum_id)
            if not forum:
                logger.error(f"Forum {forum_id} not found")
                return {'status': 'error', 'message': 'Forum not found'}
            
            self.update_forum_status(forum_id, 'pending')
            
            sync_thread = Thread(target=self._execute_sync, args=(forum_id,))
            sync_thread.daemon = True
            sync_thread.start()
            
            return {'status': 'pending', 'message': 'Sync started in background'}
            
        except Exception as e:
            with self.lock:
                if forum_id in self.running_syncs:
                    self.running_syncs.remove(forum_id)
                logger.exception(f"Error starting sync for forum {forum_id}: {str(e)}")
                return {'status': 'error', 'message': str(e)}

    def _execute_sync(self, forum_id):
        """Execute actual synchronization work in background thread"""
        try:
            forum = self.get_forum_by_id(forum_id)
            if not forum:
                logger.error(f"Forum {forum_id} not found")
                return
            
            self.update_forum_status(forum_id, 'syncing')

            access_token = get_access_token_by_api_token(forum.dtable_api_token)
            if not access_token:
                logger.error("Failed to get dtable access token.")
                self.update_forum_status(forum_id, 'failed')
                return
                
            dtable_db_api = DTableDBAPI(INNER_DTABLE_DB_URL, forum.dtable_uuid, access_token)
            dtable_server_api = DTableServerAPI(INNER_DTABLE_SERVER_URL, forum.dtable_uuid, access_token)
            dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
            try:
                dtable_web_api.open_big_data_storage(dtable_uuid=forum.dtable_uuid, access_token=access_token)
            except Exception as e:
                logger.error(f"Failed to open big data storage for forum {forum.name}: {str(e)}")
                self.update_forum_status(forum_id, 'failed')
                return

            try:
                DTableManager.create_discourse_table(dtable_server_api, forum.table_name)
            except Exception as e:
                logger.error(f"Failed to create/verify table in dtable for forum {forum.name}. Aborting sync.")
                self.update_forum_status(forum_id, 'failed')
                return
        
            sync_success = self._sync_latest_topics(forum, dtable_db_api)
            
            if sync_success:
                self.update_forum_last_synced(forum_id, 'completed')
                logger.info(f"Forum {forum.name} synced successfully")
            else:
                self.update_forum_status(forum_id, 'failed')
                logger.error(f"Forum {forum.name} sync failed")
            
        except Exception as e:
            forum = self.get_forum_by_id(forum_id)
            if forum:
                self.update_forum_status(forum_id, 'failed')
                logger.exception(f"Error syncing forum {forum.name}: {str(e)}")
            else:
                logger.exception(f"Error syncing forum {forum_id}: {str(e)}")
                
        finally:
            with self.lock:
                if forum_id in self.running_syncs:
                    self.running_syncs.remove(forum_id)

    def _fetch_topic_posts(self, forum, topic_id, headers):
        """Fetch detailed topic content including all posts"""
        try:
            url = f"{forum.base_url}/t/{topic_id}.json"
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

    def _sync_latest_topics(self, forum, dtable_db_api):
        """Sync all forum topics with pagination and detailed content"""
        headers = {
            "Api-Key": forum.api_key,
            "Api-Username": forum.api_username
        }
        page = 0

        logger.info(f"Starting to sync topics for forum {forum.name}")
        try:
            while True:
                try:
                    url = f"{forum.base_url}/latest.json?page={page}"
                    logger.info(f"Fetching page {page} from {url}")
                    response = requests.get(url, headers=headers, timeout=30)
                    response.raise_for_status()
                    
                    data = response.json()
                    topics_batch = []
                    topics = data.get("topic_list", {}).get("topics", [])

                    if not topics:
                        logger.info(f"No more topics found on page {page}")
                        break
                    for topic_data in topics:
                        try:
                            topic_id = topic_data.get('id')
                            if not topic_id:
                                continue
                            
                            topic_posts = self._fetch_topic_posts(forum, topic_id, headers)
                            if not topic_posts:
                                logger.warning(f"Failed to fetch posts for topic {topic_id}")
                                continue
                            topics_batch.append({**topic_data, 'posts':topic_posts})
                            time.sleep(0.05)
                        except Exception as e:
                            logger.error(f"Error processing topic {topic_data.get('id', 'unknown')}: {str(e)}")
                            continue
                            
                    DTableManager.batch_insert_topics(
                        dtable_db_api=dtable_db_api,
                        table_name=forum.table_name,
                        topics_data=topics_batch,
                    )
                    
                    page += 1
                    
                except Exception as e:
                    logger.error(f"Error fetching page {page}: {str(e)}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error in _sync_latest_topics for forum {forum.name}: {str(e)}")
            return False
