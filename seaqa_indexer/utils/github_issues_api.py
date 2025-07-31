import json
import logging
import requests

logger = logging.getLogger(__name__)


class GitHubAPIException(Exception):
    pass


class GitHubAPI:
    def __init__(self, token, owner, repo, timeout=60):
        self.owner = owner
        self.repo = repo
        self.token = token
        self.timeout = timeout
        self.headers = self._gen_headers()
        self.base_url = f'https://api.github.com/repos/{self.owner}/{self.repo}'

    def _gen_headers(self):
        headers = {
            'Accept': 'application/vnd.github+json'
        }
        if self.token:
            headers['Authorization'] = f'token {self.token}'
        return headers

    def _request(self, url, params=None):
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
            if response.status_code == 404:
                raise FileNotFoundError(f'Not found: {url}')
            elif response.status_code >= 400:
                raise GitHubAPIException(f'GitHub API error {response.status_code}: {response.text}')
            return response.json()
        except Exception as e:
            logger.error('GitHub API request failed: %s', str(e))
            raise

    def list_issues(self, since, state='open', per_page=30, page=1):
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

    def list_all_issues(self,since, state='open', per_page=30):
        all_issues = []
        page = 1
        while True:
            try:
                issues = self.list_issues(since=since, state=state, per_page=per_page, page=page)
            except Exception as e:
                logger.error(f"Failed to fetch page {page} of issues: {e}")
                break

            if not issues:
                break

            all_issues.extend(issues)

            if len(issues) < per_page:
                break
            page += 1

        logger.info(f"Fetched {len(all_issues)} issues from GitHub")
        return all_issues

    def get_issue(self, issue_number):

        url = f'{self.base_url}/issues/{issue_number}'
        return self._request(url)
