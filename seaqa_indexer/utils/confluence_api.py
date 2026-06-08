import logging
import re
import requests

from urllib.parse import parse_qs, urljoin, urlparse

from seaqa_indexer.config import CONFLUENCE_CLIENT_ID, CONFLUENCE_CLIENT_SECRET


logger = logging.getLogger(__name__)


def refresh_confluence_access_token(refresh_token, timeout=30):
    if not all([CONFLUENCE_CLIENT_ID, CONFLUENCE_CLIENT_SECRET, refresh_token]):
        raise RuntimeError('Confluence OAuth settings are invalid.')

    payload = {
        'grant_type': 'refresh_token',
        'client_id': CONFLUENCE_CLIENT_ID,
        'client_secret': CONFLUENCE_CLIENT_SECRET,
        'refresh_token': refresh_token,
    }
    response = requests.post(
        'https://auth.atlassian.com/oauth/token',
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


class ConfluenceAPI:
    def __init__(self, access_token, timeout=60, refresh_access_token=None):
        self.access_token = access_token
        self.timeout = timeout
        self.refresh_access_token = refresh_access_token
        self.session = requests.Session()

    def _gen_headers(self):
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json',
        }

    def _request(self, method, url, retried=False, **kwargs):
        headers = dict(kwargs.pop('headers', {}) or {})
        headers.update(self._gen_headers())
        response = self.session.request(method, url, headers=headers, timeout=self.timeout, **kwargs)

        if response.status_code == 401 and self.refresh_access_token and not retried:
            new_access_token = self.refresh_access_token()
            if new_access_token:
                self.access_token = new_access_token
                return self._request(method, url, retried=True, **kwargs)

        response.raise_for_status()
        return response

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

    def _normalize_page(self, page, base_url=''):
        page_links = page.get('_links') or {}
        version = page.get('version') or {}
        body = page.get('body') or {}
        storage_body = body.get('storage') or {}
        webui_path = page_links.get('webui') or ''

        return {
            'page_id': str(page.get('id')),
            'title': page.get('title') or '',
            'content': storage_body.get('value') or '',
            'created_time': page.get('createdAt'),
            'modified_time': version.get('createdAt') or page.get('updatedAt') or page.get('createdAt'),
            'creator': page.get('authorId') or '',
            'last_modifier': version.get('authorId') or page.get('lastOwnerId') or page.get('authorId') or '',
            'parent_page_id': str(page.get('parentId')) if page.get('parentId') is not None else '',
            'url': urljoin(base_url, webui_path) if webui_path else '',
        }

    def get_user_display_name(self, account_id, workspace_id):
        """Resolve an Atlassian account ID to a user display name.
        Follows the same pattern as NotionAPI.get_user_info()."""
        if not account_id:
            return ''
        url = f'https://api.atlassian.com/ex/confluence/{workspace_id}/wiki/rest/api/user'
        params = {'accountId': account_id}
        try:
            resp = self._request('GET', url, params=params)
            data = resp.json()
            return data.get('displayName') or account_id
        except Exception:
            return account_id

    def list_spaces(self, workspace_id, cursor=None, limit=100):
        url = f'https://api.atlassian.com/ex/confluence/{workspace_id}/wiki/api/v2/spaces'
        params = {'limit': limit}
        if cursor:
            params['cursor'] = cursor

        response = self._request('GET', url, params=params)
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

    def list_pages(self, workspace_id, cursor=None, limit=100, space_ids=None):
        url = f'https://api.atlassian.com/ex/confluence/{workspace_id}/wiki/api/v2/pages'
        params = {
            'limit': limit,
            'body-format': 'storage',
            'status': 'current',
        }
        if cursor:
            params['cursor'] = cursor
        if space_ids:
            params['space-id'] = space_ids

        response = self._request('GET', url, params=params)
        data = response.json()
        base_url = (data.get('_links') or {}).get('base') or ''
        results = data.get('results') or []
        pages = [
            self._normalize_page(page, base_url)
            for page in results
            if page.get('id') is not None
        ]
        next_cursor = self._extract_next_cursor(data, response)
        return pages, next_cursor, bool(next_cursor)
