import logging
import requests
import time
import random

from urllib.parse import urljoin
from functools import wraps


logger = logging.getLogger(__name__)

MAX_CONTENT_LENGTH = 10000


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


class DiscourseForumAPI:
    def __init__(self, forum_url, api_key, username, timeout=60):
        self.api_key = api_key
        self.username = username
        self.forum_url = forum_url
        self.timeout = timeout
        self.headers = None
        self.gen_header()

    def gen_header(self):
        self.headers = {
            "Api-Key": self.api_key,
            "Api-Username": self.username
        }

    @retry_with_exponential_backoff()
    def fetch_latest_topics_page(self, page):
        url = urljoin(self.forum_url, f"latest.json?page={page}")
        response = requests.get(url, headers=self.headers, timeout=self.timeout)
        response.raise_for_status()

        data = response.json()
        topics = data.get("topic_list", {}).get("topics", [])
        return topics

    @retry_with_exponential_backoff()
    def fetch_topic_replies(self, topic_id):
        url = urljoin(self.forum_url, f"t/{topic_id}.json")
        logger.info(f"Fetching topic detail from {url}")
        response = requests.get(url, headers=self.headers, timeout=self.timeout)
        response.raise_for_status()
        topic_data = response.json()
        posts = topic_data.get('post_stream', {}).get('posts', [])
        return posts
