import requests


class GitHubAPIException(Exception):
    pass


class GitHubAPI:
    def __init__(self, token, owner, repo, issues_sync_id, timeout=60):
        self.owner = owner
        self.repo = repo
        self.token = token
        self.issues_sync_id = issues_sync_id
        self.timeout = timeout
        self.headers = self._gen_headers()
        self.base_url = f'https://api.github.com/repos/{self.owner}/{self.repo}'

    def _gen_headers(self):
        headers = {
            'Accept': 'application/vnd.github+json'
        }
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        return headers

    def _request(self, url, params=None):
        response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        if response.status_code == 404:
            raise FileNotFoundError(f'Not found: {url}')
        elif response.status_code >= 400:
            raise GitHubAPIException(f'GitHub API error {response.status_code}: {response.text}')
        return response.json()

    def list_issues(self, since, state='all', per_page=30, page=1):
        """
        state: open / closed / all
        """
        url = f'{self.base_url}/issues'
        params = {
            'state': state,
            'per_page': per_page,
            'page': page
        }
        if since:
            params['since'] = since
        return self._request(url, params=params)

    def get_issue_comments(self, issue_number, per_page=100, page=1):

        url = f'{self.base_url}/issues/{issue_number}/comments'
        params = {
            'per_page': per_page,
            'page': page
        }
        return self._request(url, params=params)

    def get_issues_labels(self, per_page=100, page=1):
        url = f'{self.base_url}/labels'
        return self._request(url)
