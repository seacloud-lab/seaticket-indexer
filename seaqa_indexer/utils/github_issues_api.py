
import time
import jwt
import requests
from cryptography.hazmat.primitives import serialization
from seaqa_indexer.config import APP_ID, PRIVATE_KEY_PATH


class GitHubAPIException(Exception):
    pass


class GitHubAPI:
    def __init__(self, token, installation_id, owner, repo, connection_id, timeout=60):
        self.owner = owner
        self.repo = repo
        self.base_url = f'https://api.github.com/repos/{self.owner}/{self.repo}'
        self.token = token
        self.installation_id = installation_id
        self.connection_id = connection_id
        self.timeout = timeout
        self.jwt_token = self.generate_jwt()
        self.headers = self._gen_headers()

    def _gen_headers(self):
        headers = {
            'Accept': 'application/vnd.github+json'
        }

        if self.installation_id:
            installation_token = self.get_installation_token(self.installation_id)
            headers['Authorization'] = f'Bearer {installation_token}'
        else:
            try:
                installation_id = self.get_installation_id(self.jwt_token)
                installation_token = self.get_installation_token(installation_id)
                headers['Authorization'] = f'Bearer {installation_token}'
            except Exception as e:
                logger.error(f'Failed to get installation token: {e}')
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

    def generate_jwt(self):
        with open(PRIVATE_KEY_PATH, "rb") as f:
            private_key = serialization.load_pem_private_key(f.read(), password=None)

        now = int(time.time())
        payload = {
            "iat": now,
            "exp": now + (10 * 60),
            "iss": APP_ID
        }
        encoded_jwt = jwt.encode(payload, private_key, algorithm="RS256")
        return encoded_jwt

    def get_installation_id(self, jwt_token):
        url = f"{self.base_url}/installation"
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/vnd.github+json"
        }
        try:
            response = requests.get(url, headers=headers)
            data = response.json()
            installation_id = data['id']
            from seaqa_indexer.syncer.github_issues_syncer.github_issues_sync_manager import GithubIssuesSyncManager
            github_issues_sync_manager = GithubIssuesSyncManager()
            github_issues_sync_manager.handle_github_app_installation(self.connection_id, installation_id)
            return installation_id
        except requests.exceptions.RequestException as e:
            logger.error(e)
            return None

    def get_installation_token(self, installation_id):
        url = f"https://api.github.com/app/installations/{installation_id}/access_tokens"
        headers = {
            "Authorization": f"Bearer {self.jwt_token}",
            "Accept": "application/vnd.github+json"
        }
        response = requests.post(url, headers=headers)
        return response.json()["token"]
