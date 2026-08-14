import datetime
import time
import requests

from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.config import JIRA_CLIENT_ID, JIRA_CLIENT_SECRET


OAUTH_TOKEN_EXPIRY_BUFFER_SECONDS = 60
DEFAULT_CYCLE_DAYS = 7


class JiraUserOrSiteNotFoundException(Exception):
    pass


class JiraSiteUnavailableException(Exception):
    pass


class JiraTokenRevokedError(Exception):
    pass


class JiraAPI:
    def __init__(self, access_token, refresh_token, timeout=60, expires_at=None):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.timeout = timeout
        self.client_id = JIRA_CLIENT_ID
        self.client_secret = JIRA_CLIENT_SECRET
        self.token_url = 'https://auth.atlassian.com/oauth/token'
        self.expires_at = expires_at
        self.session = requests.Session()

    def _gen_headers(self):
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json',
        }

    def _api_base(self, site_id):
        return f'https://api.atlassian.com/ex/jira/{site_id}'

    def refresh_access_token(self):
        """Refresh the Jira OAuth access token."""
        if not all([self.client_id, self.client_secret, self.refresh_token]):
            raise RuntimeError('Jira OAuth settings are invalid.')

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
        if response.status_code == 401:
            raise JiraTokenRevokedError()
        response.raise_for_status()
        data = response.json()
        self.access_token = data.get('access_token')
        new_refresh_token = data.get('refresh_token')
        if new_refresh_token:
            self.refresh_token = new_refresh_token

        expires_in = data.get('expires_in', 3600) or 3600
        self.expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=max(int(expires_in) - OAUTH_TOKEN_EXPIRY_BUFFER_SECONDS, 0))

        return {
            'access_token': self.access_token,
            'refresh_token': self.refresh_token,
            'expires_at': self.expires_at,
        }

    def get_user_display_name(self, account_id, site_id):
        if not account_id or account_id == 'unknown':
            return ''

        url = f'{self._api_base(site_id)}/rest/api/3/user'
        response = self.session.get(
            url,
            headers=self._gen_headers(),
            params={'accountId': account_id},
            timeout=self.timeout,
        )
        if response.status_code == 404:
            raise JiraUserOrSiteNotFoundException()
        if response.status_code == 403:
            raise JiraSiteUnavailableException()
        response.raise_for_status()
        data = response.json()
        if data.get('accountId') == 'unknown' or not data.get('active'):
            return ''
        return data.get('displayName') or ''

    def get_accessible_resources(self):
        response = self.session.get(
            'https://api.atlassian.com/oauth/token/accessible-resources',
            headers=self._gen_headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json() or []

    def report_accounts(self, accounts):
        if not accounts:
            return {'accounts': [], 'cycle_period': DEFAULT_CYCLE_DAYS}

        while True:
            response = requests.post(
                'https://api.atlassian.com/app/report-accounts/',
                json={'accounts': accounts},
                headers={
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/json',
                },
                timeout=self.timeout,
            )
            if response.status_code != 429:
                break
            time.sleep(int(response.headers['Retry-After']))

        cycle_period = int(
            response.headers.get('Cycle-Period', DEFAULT_CYCLE_DAYS)
        )

        if response.status_code == 204:
            return {'accounts': [], 'cycle_period': cycle_period}
        response.raise_for_status()
        return {
            'accounts': response.json().get('accounts') or [],
            'cycle_period': cycle_period,
        }

    def get_createmeta(self, project_key, site_id):
        url = f'{self._api_base(site_id)}/rest/api/3/issue/createmeta'
        params = {
            'projectKeys': project_key,
            'expand': 'projects.issuetypes.fields',
        }
        response = self.session.get(url, headers=self._gen_headers(), params=params, timeout=self.timeout)
        response.raise_for_status()
        metadata = response.json()
        projects = metadata.get('projects', [])
        if not projects:
            return []
        project = projects[0]
        issue_types = project.get('issuetypes', [])
        return issue_types

    def get_priorities(self, site_id):
        url = f'{self._api_base(site_id)}/rest/api/2/priority'
        response = self.session.get(url, headers=self._gen_headers(), timeout=self.timeout)
        response.raise_for_status()
        return response.json() or []

    def get_statuses(self, project_key, site_id):
        url = f'{self._api_base(site_id)}/rest/api/2/project/{project_key}/statuses/'
        response = self.session.get(url, headers=self._gen_headers(), timeout=self.timeout)
        response.raise_for_status()
        metadata = response.json() or []
        seen_ids = set()
        unique_statuses = []
        for entry in metadata:
            for status in entry.get('statuses', []):
                s_id = status['id']
                if s_id not in seen_ids:
                    unique_statuses.append(status)
                    seen_ids.add(s_id)
        return unique_statuses

    def search_issues(self, project_key, old_last_sync_time, site_id, start_at=0, max_results=50):
        if old_last_sync_time:
            since_time = time_to_utc_time(str(old_last_sync_time)).strftime('%Y-%m-%d %H:%M')
            jql = f'project = "{project_key}" AND updated >= "{since_time}" ORDER BY updated ASC'
        else:
            jql = f'project = "{project_key}" ORDER BY created ASC'

        fields = 'summary,issuetype,status,priority,assignee,creator,duedate,description,updated,created'

        url = f'{self._api_base(site_id)}/rest/api/3/search/jql'
        params = {
            'jql': jql,
            'fields': fields,
            'startAt': start_at,
            'maxResults': max_results,
        }
        response = self.session.get(url, headers=self._gen_headers(), params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_issue_comments(self, issue_key, site_id, start_at=0, max_results=100):
        """Fetch all comments for a given issue, paginated.

        Returns a flat list of all comment dicts across all pages.
        """
        all_comments = []
        while True:
            url = f'{self._api_base(site_id)}/rest/api/3/issue/{issue_key}/comment'
            params = {
                'startAt': start_at,
                'maxResults': max_results,
            }
            response = self.session.get(url, headers=self._gen_headers(), params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            comments = data.get('comments', [])
            all_comments.extend(comments)
            total = data.get('total', 0) or len(comments)
            start_at += len(comments)
            if start_at >= total or not comments:
                break
        return all_comments
