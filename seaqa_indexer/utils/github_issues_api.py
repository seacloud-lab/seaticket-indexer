import logging

import requests
import time
import jwt

from seaqa_indexer.config import GITHUB_APP_ID, GITHUB_PRIVATE_KEY


class GitHubAPIException(Exception):
    pass


class GitHubAppNotInstalled(Exception):
    pass


class GitHubRepoNotFound(Exception):
    pass


class GitHubAPI:
    def __init__(self, installation_id, timeout=60):
        self.installation_id = installation_id
        self.app_id = str(GITHUB_APP_ID)
        self.app_private_key = GITHUB_PRIVATE_KEY
        self.timeout = timeout
        self.base_url = "https://api.github.com"
        self.headers = self._gen_headers()

    def generate_github_app_jwt(self):
        now = int(time.time())
        payload = {
            "iat": now,  # Issued at time
            "exp": now + (5 * 60),
            "iss": self.app_id
        }
        encoded_jwt = jwt.encode(payload, self.app_private_key, algorithm="RS256")
        return encoded_jwt

    def get_installation_access_token(self, installation_id):
        jwt_token = self.generate_github_app_jwt()

        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/vnd.github.v3+json"
        }

        url = f"{self.base_url}/app/installations/{installation_id}/access_tokens"

        try:
            response = requests.post(url, headers=headers)
        except requests.exceptions.RequestException as e:
            raise GitHubAPIException(f'Failed to obtain installation token: {response.status_code}: {response.text}')
        except Exception as e:
            raise GitHubAPIException(f'An exception occurred while obtaining the installation token.: {e}')
        if response.status_code == 404:
            raise GitHubAppNotInstalled('App is deleted or installation_id is incorrect.')
        response.raise_for_status()
        access_token_data = response.json()
        return access_token_data["token"]

    def _gen_headers(self):
        access_token = self.get_installation_access_token(self.installation_id)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        return headers

    def _request(self, url, params=None):
        response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        if response.status_code == 404:
            raise GitHubRepoNotFound(f'Not found: {url}')
        elif response.status_code >= 400:
            raise GitHubAPIException(f'GitHub API error {response.status_code}: {response.text}')
        return response.json()

    def list_issues(self, repo_owner, repo_name, since, state='all', per_page=30, page=1):
        """
        state: open / closed / all
        """
        url = f'{self.base_url}/repos/{repo_owner}/{repo_name}/issues'
        params = {
            'state': state,
            'per_page': per_page,
            'page': page,
            'sort': 'updated',
            'direction': 'asc'
        }
        if since:
            params['since'] = since
        return self._request(url, params=params)

    def get_issue_comments(self, repo_owner, repo_name, issue_number, per_page=100, page=1):

        url = f'{self.base_url}/repos/{repo_owner}/{repo_name}/issues/{issue_number}/comments'
        params = {
            'per_page': per_page,
            'page': page
        }
        return self._request(url, params=params)

    def get_issues_labels(self, repo_owner, repo_name, per_page=100, page=1):
        url = f'{self.base_url}/repos/{repo_owner}/{repo_name}/labels'
        return self._request(url)
