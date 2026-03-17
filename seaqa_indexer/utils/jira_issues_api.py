import requests

from seaqa_indexer.utils import time_to_utc_time


class JiraAPIException(Exception):
    pass


class JiraAPI:
    def __init__(self, base_url, user_email, api_token, timeout=30):
        self.base_url = base_url.rstrip('/')
        self.user_email = user_email
        self.api_token = api_token
        self.timeout = timeout
        self.auth = (user_email, api_token)

    def _request(self, url, params=None):
        try:
            response = requests.get(url, params=params, auth=self.auth, timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            raise JiraAPIException(f'Jira API request failed: {e}')
        if response.status_code >= 400:
            raise JiraAPIException(f'Jira API error {response.status_code}: {response.text}')
        return response.json()

    def get_priorities(self):
        url = f'{self.base_url}/rest/api/2/priority'
        return self._request(url) or []

    def get_statuses(self):
        url = f'{self.base_url}/rest/api/2/status'
        return self._request(url) or []

    def get_issue_types(self):
        url = f'{self.base_url}/rest/api/2/issuetype'
        return self._request(url) or []

    def search_issues(self, space_key, old_last_sync_time, start_at=0, max_results=50):
        if old_last_sync_time:
            since_time = time_to_utc_time(str(old_last_sync_time)).strftime('%Y-%m-%d %H:%M')
            jql = f'project = \"{space_key}\" AND updated >= \"{since_time}\" ORDER BY updated ASC'
        else:
            jql = f'project = \"{space_key}\" ORDER BY created ASC'

        fields = 'summary,issuetype,status,priority,assignees,duedate,description,comment,updated,created'

        url = f'{self.base_url}/rest/api/3/search/jql'
        params = {
            'jql': jql,
            'fields': fields,
            'startAt': start_at,
            'maxResults': max_results,
        }
        return self._request(url, params=params)
