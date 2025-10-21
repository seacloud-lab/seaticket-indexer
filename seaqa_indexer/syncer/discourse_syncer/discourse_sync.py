import logging
import requests
import time
import json
import datetime
import random

from sqlalchemy import text
from urllib.parse import urljoin
from functools import wraps
from dateutil.relativedelta import relativedelta

from seaqa_indexer.utils import time_str_to_utc_time, need_update_connection_last_sync_count
from seaqa_indexer.seadb_models.discourse_seadb_api import DiscourseSeaDBAPI
from seaqa_indexer.seadb_models.models import DiscourseTopicsTable, DiscourseRepliesTable

logger = logging.getLogger(__name__)


def retry_with_exponential_backoff(max_retries=5, base_delay=3, max_delay=60):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                        logger.warning(f"Request failed: {str(e)}, retrying in {delay:.2f} seconds (attempt {attempt + 1}/{max_retries + 1})")
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(f"Request failed after {max_retries} retries: {str(e)}")
                        raise
        return wrapper
    return decorator


class DiscourseSync:
    def __init__(self, base_id, seaqa_db, sync_logger, username=''):
        self.discourse_seadb_api = DiscourseSeaDBAPI(base_id=base_id, username=username)
        self.topics_table = DiscourseTopicsTable
        self.replies_table = DiscourseRepliesTable
        self.seaqa_db = seaqa_db
        self.sync_logger = sync_logger

    def check_existing_topic(self, topic_id, connection_id):
        """Check if a topic already exists"""
        return self.discourse_seadb_api.get_topic_by_id(connection_id, topic_id)

    def insert_topic(self, topic_id, title, slug, views, category_id, connection_id, bumped_at, topic_created_at):
        """Insert a new topic"""
        topic_data = {
            self.topics_table.topic_id.name: topic_id,
            self.topics_table.title.name: title,
            self.topics_table.slug.name: slug,
            self.topics_table.views.name: views,
            self.topics_table.category_id.name: category_id,
            self.topics_table.bumped_at.name: bumped_at.isoformat(),
            self.topics_table.deleted.name: False,
            self.topics_table.updated_at.name: datetime.datetime.now(datetime.UTC).isoformat(),
            self.topics_table.created_at.name: topic_created_at.isoformat(),}
        self.discourse_seadb_api.insert_topic(connection_id, topic_data)

    def delete_topic_and_replies(self, topic_id, connection_id):
        """Delete a topic and all its replies"""
        self.discourse_seadb_api.delete_replies_by_topic(connection_id, topic_id)

        self.discourse_seadb_api.delete_topic(connection_id, topic_id)

    def get_discourse_forum_topic_count(self, connection_id):
        """Retrieve the number of discourse topics for the specified connection"""
        return self.discourse_seadb_api.get_topic_count(connection_id)

    def handle_topic_update(self, connection_id, topic_id, data):
        topic_data = data.get('topic', {})
        existing_topic = self.check_existing_topic(topic_id, connection_id)

        title = topic_data.get('title')
        slug = topic_data.get('slug')
        views = topic_data.get('views', 0)
        category_id = topic_data.get('category_id')
        if existing_topic:
            bumped_at = time_str_to_utc_time(topic_data.get('last_posted_at'))
            self.update_topic(title, slug, views, category_id, topic_id, connection_id, bumped_at)
            self.sync_logger._log_debug(f"Updated existing topic {topic_id}")
        else:
            # When using webhook, the returned value does not have a bumped_at.
            topic_created_at = time_str_to_utc_time(topic_data.get('created_at'))
            if topic_data.get('bumped_at'):
                bumped_at = time_str_to_utc_time(topic_data.get('bumped_at'))
            else:
                bumped_at = time_str_to_utc_time(topic_data.get('last_posted_at'))
            self.insert_topic(topic_id, title, slug, views, category_id, connection_id, bumped_at, topic_created_at)
            self.sync_logger._log_debug(f"Created new topic {topic_id}")

    def handle_reply_update(self, connection_id, topic_id, data):
        reply_data = data.get('post', {})
        post_number = reply_data.get('post_number')

        existing_reply = self.check_existing_reply(topic_id, post_number, connection_id)
        content = reply_data.get('cooked', '')
        author = reply_data.get('username', '')
        updated_at = time_str_to_utc_time(reply_data.get('updated_at'))
        if existing_reply:
            self.update_reply(content, author, topic_id, post_number, updated_at, connection_id)
            self.sync_logger._log_debug(f"Updated existing reply {post_number} in topic {topic_id}")
        else:
            self.insert_reply(topic_id, post_number, content, author, updated_at, connection_id)
            self.sync_logger._log_debug(f"Created new reply {post_number} in topic {topic_id}")

    def handle_reply_delete(self, connection_id, topic_id, data):
        reply_data = data.get('post', {})
        post_number = reply_data.get('post_number')
        if post_number:
            self.discourse_seadb_api.delete_reply(connection_id, topic_id, post_number)
            self.sync_logger._log_info(f"Deleted reply {post_number} in topic {topic_id}")

    def handle_topic_deleted(self, topic_id, connection_id):
        """Mark a topic as deleted instead of physically deleting it"""
        try:
            self.discourse_seadb_api.delete_topic(connection_id, topic_id)
            self.sync_logger._log_info(f"Deleted topic {topic_id} for connection {connection_id}")
        except Exception as e:
            self.sync_logger._log_error(f"Failed to delete topic {topic_id}: {e}")

    def insert_reply(self, topic_id, post_number, content, author, updated_at, connection_id):
        """Insert a new reply"""
        reply_data = {
            self.replies_table.topic_id.name: topic_id,
            self.replies_table.post_number.name: post_number,
            self.replies_table.content.name: content,
            self.replies_table.author.name: author,
            self.replies_table.updated_at.name: updated_at.isoformat(),
        }
        self.discourse_seadb_api.insert_reply(connection_id, reply_data)

    def update_topic(self, title, slug, views, category_id, topic_id, connection_id, bumped_at):
        """Update an existing topic"""
        topic_data = {
            self.topics_table.title.name: title,
            self.topics_table.slug.name: slug,
            self.topics_table.views.name: views,
            self.topics_table.category_id.name: category_id,
            self.topics_table.bumped_at.name: bumped_at.isoformat(),
            self.topics_table.updated_at.name: datetime.datetime.now(datetime.UTC).isoformat(),
        }
        self.discourse_seadb_api.update_topic(connection_id, topic_id, topic_data)

    def check_existing_reply(self, topic_id, post_number, connection_id):
        """Check if a reply already exists"""
        return self.discourse_seadb_api.get_reply_by_id(topic_id, connection_id, post_number)

    def update_reply(self, content, author, topic_id, post_number, updated_at, connection_id):
        """Update an existing reply"""
        reply_data = {
            self.replies_table.content.name: content,
            self.replies_table.author.name: author,
            self.replies_table.updated_at.name: updated_at.isoformat()
        }
        self.discourse_seadb_api.update_reply(connection_id, topic_id, post_number, reply_data)

    def _build_api_headers(self, api_key, api_username):
        """Build API headers for Discourse requests"""
        return {
            "Api-Key": api_key,
            "Api-Username": api_username
        }

    def add_or_update_topic_and_replies(self, topic_data, topic_id, connection_id, forum_url, headers):
        """Deletes and recreates an existing topic and its replies to ensure content is up-to-date."""
        # Delete existing topic and its replies
        self.delete_topic_and_replies(topic_id, connection_id)

        # Create new topic with replies
        self._create_topic_and_replies(topic_data, topic_id, forum_url, headers, connection_id)
        self.sync_logger._log_debug(f"Successfully recreated topic {topic_id} with replies.")

    def _create_topic_and_replies(self, topic_data, topic_id, forum_url, headers, connection_id):
        """Create new topic and its replies"""
        # Fetch topic posts
        topic_replies_data = self.get_discourse_topic_replies(forum_url, topic_id, headers)
        if not topic_replies_data:
            self.sync_logger._log_info(f"fetch 0 replies for topic {topic_id}")
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
        bumped_at = time_str_to_utc_time(topic_data.get('bumped_at'))
        topic_created_at = time_str_to_utc_time(topic_data.get('created_at'))

        self.insert_topic(topic_id, title, slug, views, category_id, connection_id, bumped_at, topic_created_at)

    def _insert_topic_replies(self, topic_replies_data, topic_id, connection_id):
        """Insert topic replies into database"""
        for reply in topic_replies_data.get('replies', []):
            post_number = reply.get('post_number')
            content = reply.get('cooked')
            author = reply.get('username')
            updated_at = time_str_to_utc_time(reply.get('updated_at'))

            self.insert_reply(topic_id, post_number, content, author, updated_at, connection_id)
        self.sync_logger._log_debug(f"Successfully processed and saved topic {topic_id} with replies")

    @retry_with_exponential_backoff()
    def fetch_latest_topics_page(self, forum_url, headers, page):
        url = urljoin(forum_url, f"latest.json?page={page}")
        self.sync_logger._log_debug(f"Fetching page {page} from {url}")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        data = response.json()
        topics = data.get("topic_list", {}).get("topics", [])
        return topics

    @retry_with_exponential_backoff()
    def fetch_discourse_topic(self, forum_url, topic_id, headers):
        url = urljoin(forum_url, f"t/{topic_id}.json")
        self.sync_logger._log_info(f"Fetching topic detail from {url}")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response

    def get_discourse_topic_replies(self, forum_url, topic_id, headers):
        """Fetch detailed topic content including all replies"""
        response = self.fetch_discourse_topic(forum_url, topic_id, headers)

        topic_data = response.json()
        replies = []
        posts = topic_data.get('post_stream', {}).get('posts', [])

        for post in posts:
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

    def sync(self, connection_id, forum_url, sync_years, project_created_at, api_key, api_username, last_updated_at, connection_status):
        page = 0
        should_stop = False
        sync_count = 0
        headers = self._build_api_headers(api_key, api_username)
        _status_last_update_time = datetime.datetime.now()
        _status_last_update_count = 0

        if last_updated_at:
            last_updated_at = time_str_to_utc_time(str(last_updated_at))

        # Calculate sync_earliest_time by subtracting sync_years from project_created_at
        project_created_at_utc = time_str_to_utc_time(str(project_created_at))
        sync_earliest_time = project_created_at_utc - relativedelta(years=sync_years)

        while True:
            topics = self.fetch_latest_topics_page(forum_url, headers, page)

            if not topics:
                return sync_count

            for topic_data in topics:
                topic_bumped_at = time_str_to_utc_time(topic_data.get('bumped_at'))

                if last_updated_at and topic_bumped_at <= last_updated_at and not topic_data.get('pinned'):
                    should_stop = True
                    break

                if topic_bumped_at <= sync_earliest_time:
                    if topic_data.get('pinned'):
                        continue
                    should_stop = True
                    break

                topic_id = topic_data.get('id')

                # Check if topic already exists in DB
                existing_topic = self.check_existing_topic(topic_id, connection_id)
                if existing_topic:
                    existing_bumped_at = time_str_to_utc_time(str(existing_topic.get('bumped_at')))
                    if topic_bumped_at <= existing_bumped_at:
                        self.sync_logger._log_debug(f'Topic {topic_id} is up to date, skipping sync.')
                        continue

                # Handle different sync scenarios
                self.add_or_update_topic_and_replies(topic_data, topic_id, connection_id, forum_url, headers)

                sync_count += 1
                should, new_time, new_count = need_update_connection_last_sync_count(
                    sync_count, _status_last_update_time, _status_last_update_count
                )
                if should:
                    connection_status['last_sync_count'] = sync_count
                    self.seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                    _status_last_update_time = new_time
                    _status_last_update_count = new_count
                time.sleep(1)

            page += 1
            if should_stop:
                return sync_count
