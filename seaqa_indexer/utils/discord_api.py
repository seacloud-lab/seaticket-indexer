# -*- coding: utf-8 -*-
import logging
import requests
import time
import random

from functools import wraps


logger = logging.getLogger(__name__)

DISCORD_API_BASE_URL = 'https://discord.com/api/v10'
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


class DiscordAPI(object):
    """Discord API client for syncing messages from a channel."""

    def __init__(self, bot_token, timeout=60):
        self.bot_token = bot_token
        self.timeout = timeout
        self.headers = {
            'Authorization': f'Bot {bot_token}',
            'Content-Type': 'application/json',
        }

    @retry_with_exponential_backoff()
    def fetch_channel_messages(self, channel_id, before=None, limit=100):
        """Fetch messages from a channel, newest first.

        Args:
            channel_id: Discord channel ID (snowflake).
            before: Get messages before this message ID (for pagination).
            limit: Max messages per page (1-100).

        Returns:
            list[dict]: Message objects from Discord API.
        """
        url = f'{DISCORD_API_BASE_URL}/channels/{channel_id}/messages'
        params = {'limit': limit}
        if before:
            params['before'] = before

        logger.info(f"Fetching messages from channel {channel_id} with params: {params}")
        response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        response.raise_for_status()
        messages = response.json()
        logger.info(f"Fetched {len(messages)} messages from channel {channel_id}")
        return messages

    @retry_with_exponential_backoff()
    def fetch_thread_messages(self, channel_id, thread_id, before=None, limit=100):
        """Fetch messages from a thread (which is a sub-channel).

        Args:
            channel_id: The parent channel ID (unused in the API call but kept for context).
            thread_id: The thread channel ID.
            before: Get messages before this message ID.
            limit: Max messages per page.

        Returns:
            list[dict]: Message objects.
        """
        # Discord threads are accessed like regular channels
        url = f'{DISCORD_API_BASE_URL}/channels/{thread_id}/messages'
        params = {'limit': limit}
        if before:
            params['before'] = before

        logger.info(f"Fetching thread messages from thread {thread_id}")
        response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        response.raise_for_status()
        messages = response.json()
        return messages

    @retry_with_exponential_backoff()
    def fetch_forum_threads(self, guild_id, channel_id, limit=100):
        """Fetch active threads from a forum channel.

        Uses the guild-level threads endpoint and filters by parent_id
        to return only threads belonging to the specified forum channel.

        Returns:
            list[dict]: Thread objects with 'id', 'name', 'parent_id', etc.
        """
        url = f'{DISCORD_API_BASE_URL}/guilds/{guild_id}/threads/active'
        params = {'limit': limit}

        logger.info(f"Fetching active threads for guild {guild_id}, forum channel {channel_id}")
        response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        all_threads = data.get('threads', [])
        # Filter to only threads from this forum channel
        forum_threads = [t for t in all_threads if t.get('parent_id') == channel_id]
        logger.info(f"Found {len(forum_threads)} threads in forum channel {channel_id}")
        return forum_threads
