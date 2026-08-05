import datetime
import re
import requests

from urllib.parse import parse_qs, urlparse

from seaqa_indexer.config import CONFLUENCE_CLIENT_ID, CONFLUENCE_CLIENT_SECRET
from seaqa_indexer.utils import time_to_utc_time


class ConfluenceUserNotFoundException(Exception):
    pass


class ConfluenceAPI:
    def __init__(self, access_token, refresh_token=None, expires_at=None, timeout=60):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.expires_at = expires_at
        self.client_id = CONFLUENCE_CLIENT_ID
        self.client_secret = CONFLUENCE_CLIENT_SECRET
        self.timeout = timeout
        self.token_url = 'https://auth.atlassian.com/oauth/token'
        self.session = requests.Session()

    def _gen_headers(self):
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json',
        }

    def refresh_access_token(self):
        """Refresh the Confluence OAuth access token.

        Uses the stored refresh_token to obtain a new access token from
        Atlassian, updates instance state, and returns the new token data.
        """
        if not all([self.client_id, self.client_secret, self.refresh_token]):
            raise RuntimeError('Confluence OAuth settings are invalid.')

        payload = {
            'grant_type': 'refresh_token',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self.refresh_token,
        }
        response = requests.post(
            self.token_url,
            json=payload,
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        self.access_token = data.get('access_token')
        new_refresh_token = data.get('refresh_token')
        if new_refresh_token:
            self.refresh_token = new_refresh_token

        expires_in = data.get('expires_in') or 3600
        self.expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=max(int(expires_in) - 60, 0))

        return {
            'access_token': self.access_token,
            'refresh_token': self.refresh_token,
            'expires_at': self.expires_at,
        }

    def _extract_next_cursor(self, data, response):
        next_link = (data.get('_links') or {}).get('next')
        if next_link:
            query = parse_qs(urlparse(next_link).query)
            cursor = query.get('cursor')
            if cursor:
                return cursor[0]

        link_header = response.headers.get('Link', '')
        match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
        if not match:
            return None

        query = parse_qs(urlparse(match.group(1)).query)
        cursor = query.get('cursor')
        return cursor[0] if cursor else None

    def _normalize_page(self, page):
        body = page.get('body') or {}
        storage_body = body.get('storage') or {}
        version = page.get('version') or {}
        history = page.get('history') or {}
        created_by = history.get('createdBy') or {}

        return {
            'page_id': str(page.get('id')),
            'title': page.get('title') or '',
            'content': storage_body.get('value') or '',
            'created_time': history.get('createdDate'),
            'modified_time': version.get('when'),
            'creator_id': created_by.get('accountId') or '',
            'last_modifier_id': (version.get('by') or {}).get('accountId') or '',
            'parent_page_id': '',
        }

    def get_user_display_name(self, account_id, workspace_id):
        """Resolve an Atlassian account ID to a user display name.
        Follows the same pattern as NotionAPI.get_user_info()."""
        if not account_id:
            return ''
        url = f'https://api.atlassian.com/ex/confluence/{workspace_id}/wiki/rest/api/user'
        params = {'accountId': account_id, 'expand': 'operations'}
        resp = self.session.get(url, headers=self._gen_headers(), params=params, timeout=self.timeout)
        if resp.status_code == 404:
            raise ConfluenceUserNotFoundException()
        resp.raise_for_status()
        data = resp.json()
        # Corrupted deleted user record — accountId is the literal string "unknown"
        if data.get('accountId') == 'unknown':
            return ''
        # Deactivated/unlicensed or GDPR-deleted user — operations will be empty
        operations = data.get('operations') or []
        if not operations:
            return ''
        display_name = data.get('displayName') or ''
        # Normal GDPR-deleted user — Atlassian returns "Former user" as display name
        if display_name == 'Former user':
            return ''
        return display_name

    def report_accounts(self, accounts):
        """Report account IDs to Atlassian privacy API.

        Returns:
            dict with keys:
                'accounts': list of dicts needing action [{'accountId': ..., 'status': 'closed|updated'}]
                'cycle_period': int days until next report (from Cycle-Period response header)
        """
        if not accounts:
            return {'accounts': [], 'cycle_period': 7}
        url = 'https://api.atlassian.com/app/report-accounts/'
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
        }
        response = requests.post(url, json={'accounts': accounts}, headers=headers, timeout=60)
        cycle_period = int(response.headers.get('Cycle-Period', 7))
        print(f'Privacy report response: status={response.status_code}, cycle_period={cycle_period}')
        if response.status_code == 204:
            return {'accounts': [], 'cycle_period': cycle_period}
        response.raise_for_status()
        data = response.json()
        return {
            'accounts': data.get('accounts') or [],
            'cycle_period': cycle_period,
        }

    def list_spaces(self, workspace_id, cursor=None, limit=100):
        url = f'https://api.atlassian.com/ex/confluence/{workspace_id}/wiki/api/v2/spaces'
        params = {'limit': limit}
        if cursor:
            params['cursor'] = cursor

        response = self.session.get(url, headers=self._gen_headers(), params=params, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        results = data.get('results') or []
        spaces = [
            {
                'id': str(space['id']),
                'key': space.get('key') or '',
                'name': space.get('name') or '',
                'type': space.get('type') or '',
                'status': space.get('status') or '',
            }
            for space in results
            if space.get('id') is not None
        ]
        next_cursor = self._extract_next_cursor(data, response)
        return spaces, next_cursor, bool(next_cursor)

    def list_pages(self, workspace_id, cursor=None, limit=100, space_keys=None, since=None):
        # v1 REST API with CQL — supports server-side time filtering via `since`.
        # `since` is an ISO-format string from SeaDB (e.g. "2025-06-30T10:00:00+00:00").
        url = f'https://api.atlassian.com/ex/confluence/{workspace_id}/wiki/rest/api/content/search'
        cql_conditions = ['type=page']
        formatted_since = None
        if since:
            formatted_since = time_to_utc_time(str(since)).strftime('%Y/%m/%d %H:%M')
        if formatted_since:
            cql_conditions.append(f'lastModified >= "{formatted_since}"')
        if space_keys:
            space_clauses = ' or '.join([f'space = "{space_key}"' for space_key in space_keys])
            cql_conditions.append(f'({space_clauses})')
        cql = ' and '.join(cql_conditions)
        cql += ' order by lastModified asc'
        params = {
            'cql': cql,
            'expand': 'body.storage,version,history',
            'limit': limit,
        }
        if cursor:
            params['cursor'] = cursor
        response = self.session.get(url, headers=self._gen_headers(), params=params, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        results = data.get('results') or []
        pages = [self._normalize_page(p) for p in results if p.get('id')]
        next_cursor = self._extract_next_cursor(data, response)
        return pages, next_cursor, bool(next_cursor)
