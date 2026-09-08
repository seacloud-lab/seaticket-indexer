# -*- coding: utf-8 -*-
import logging
import time
import random
import datetime
from functools import wraps

import requests


logger = logging.getLogger(__name__)

SLACK_API_BASE_URL = 'https://slack.com/api'


class SlackAPIError(Exception):
    """Non-retryable Slack API error (e.g. invalid channel, missing scope)."""


class SlackAPIRetryableError(SlackAPIError):
    """Retryable Slack API error (429 rate limit / 5xx)."""

    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


def retry_with_exponential_backoff(max_retries=5, base_delay=3, max_delay=60):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except SlackAPIRetryableError as e:
                    if attempt < max_retries:
                        delay = float(e.retry_after) if e.retry_after else min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                        logger.warning('Slack API retryable error: %s, retrying in %.2f seconds (attempt %d/%d)',
                                       str(e), delay, attempt + 1, max_retries + 1)
                        time.sleep(delay)
                        continue
                    raise
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                        logger.warning('Slack request failed: %s, retrying in %.2f seconds (attempt %d/%d)',
                                       str(e), delay, attempt + 1, max_retries + 1)
                        time.sleep(delay)
                        continue
                    logger.error('Slack request failed after %d retries: %s', max_retries, str(e))
                    raise
        return wrapper
    return decorator


class SlackAPI(object):
    """Slack Web API client for syncing channel messages and thread replies."""

    def __init__(self, bot_token, client_id='', client_secret='', timeout=60):
        self.bot_token = bot_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self.headers = {
            'Authorization': f'Bearer {bot_token}',
            'Content-Type': 'application/json',
        }

    def _get(self, method, params):
        url = f'{SLACK_API_BASE_URL}/{method}'
        return requests.get(url, headers=self.headers, params=params, timeout=self.timeout)

    @staticmethod
    def _parse(response, method):
        if response.status_code == 429:
            retry_after = response.headers.get('Retry-After')
            raise SlackAPIRetryableError('rate limited', retry_after=retry_after)
        if response.status_code >= 500:
            raise SlackAPIRetryableError(f'HTTP {response.status_code}: {response.text}')
        if response.status_code != 200:
            raise SlackAPIError(f'HTTP {response.status_code}: {response.text}')

        data = response.json()
        if not data.get('ok'):
            error = data.get('error', 'unknown_error')
            if error in ('ratelimited', 'internal_error', 'fatal_error'):
                raise SlackAPIRetryableError(error)
            raise SlackAPIError(error)
        return data

    @retry_with_exponential_backoff()
    def fetch_history(self, channel_id, oldest=None, cursor=None, limit=200):
        """Fetch top-level messages from a channel (newest first).

        Args:
            channel_id: Slack channel ID (e.g. 'C0123ABC').
            oldest: Only messages after this unix timestamp (decimal seconds).
            cursor: Pagination cursor from response_metadata.next_cursor.
            limit: Max messages per page (default 200, max < 1000).

        Returns:
            dict: raw Slack response with 'messages', 'has_more', 'response_metadata'.
        """
        params = {'channel': channel_id, 'limit': limit}
        if oldest:
            params['oldest'] = oldest
        if cursor:
            params['cursor'] = cursor
        response = self._get('conversations.history', params)
        return self._parse(response, 'conversations.history')

    @retry_with_exponential_backoff()
    def fetch_replies(self, channel_id, thread_ts, oldest=None, cursor=None, limit=200):
        """Fetch a thread's replies (including the parent message).

        Args:
            channel_id: Slack channel ID.
            thread_ts: The parent message ts (the thread root).
            oldest: Only replies after this unix timestamp (decimal seconds).
            cursor: Pagination cursor.

        Returns:
            dict: raw Slack response with 'messages', 'has_more', 'response_metadata'.
        """
        params = {'channel': channel_id, 'ts': thread_ts, 'limit': limit}
        if oldest:
            params['oldest'] = oldest
        if cursor:
            params['cursor'] = cursor
        response = self._get('conversations.replies', params)
        return self._parse(response, 'conversations.replies')

    @retry_with_exponential_backoff()
    def fetch_user_info(self, user_id):
        """Resolve a Slack user ID to a user object."""
        response = self._get('users.info', {'user': user_id})
        return self._parse(response, 'users.info')

    @retry_with_exponential_backoff()
    def fetch_team_info(self):
        """Fetch the current team info (includes 'team.domain')."""
        response = self._get('team.info', {})
        return self._parse(response, 'team.info')

    def refresh_access_token(self, refresh_token):
        """Refresh the Slack bot token via oauth.v2.access (token rotation).

        Returns a dict with 'access_token', 'refresh_token' and 'expires_at'
        (a timezone-aware datetime).
        """
        if not all([self.client_id, self.client_secret, refresh_token]):
            raise SlackAPIError('Slack OAuth settings are invalid for token refresh')

        payload = {
            'grant_type': 'refresh_token',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': refresh_token,
        }
        response = requests.post(
            f'{SLACK_API_BASE_URL}/oauth.v2.access',
            data=payload,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if not data.get('ok'):
            raise SlackAPIError(data.get('error', 'unknown_error'))

        access_token = data.get('access_token')
        new_refresh_token = data.get('refresh_token') or refresh_token
        expires_in = data.get('expires_in') or 3600
        expires_at = datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=max(int(expires_in) - 60, 0))

        return {
            'access_token': access_token,
            'refresh_token': new_refresh_token,
            'expires_at': expires_at,
        }
