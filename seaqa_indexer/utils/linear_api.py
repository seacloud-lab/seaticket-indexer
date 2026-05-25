import datetime
import requests

from seaqa_indexer.config import LINEAR_CLIENT_ID, LINEAR_CLIENT_SECRET
from seaqa_indexer.utils.seaqa_db import SeaqaDB


class LinearAPI:
    def __init__(
        self,
        access_token: str,
        refresh_token,
        project_uuid=None,
        seaqa_db: SeaqaDB = None,
        expires_at=None,
        timeout: int = 60,
        token_refresh_buffer_seconds: int = 5 * 60,
    ):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.project_uuid = project_uuid
        self.seaqa_db = seaqa_db
        self.expires_at = expires_at
        self.token_refresh_buffer_seconds = token_refresh_buffer_seconds
        self.client_id = LINEAR_CLIENT_ID
        self.client_secret = LINEAR_CLIENT_SECRET
        self.api_url = "https://api.linear.app/graphql"
        self.token_url = "https://api.linear.app/oauth/token"
        self.timeout = timeout
        self._last_token_update = None

    @property
    def last_token_update(self):
        return self._last_token_update

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def refresh_access_token(self):
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        response = requests.post(
            self.token_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json() or {}
        access_token = data.get("access_token")
        self.access_token = access_token
        self.refresh_token = data.get("refresh_token")
        self._last_token_update = data

        if self.seaqa_db and self.project_uuid:
            expires_at = self.calc_expires_at(data.get("expires_in"))
            self.seaqa_db.upsert_project_linear_oauth(
                self.project_uuid,
                self.access_token,
                expires_at=expires_at,
                refresh_token=self.refresh_token,
            )
        return data

    def _is_expired(self):
        """Check if the token is expired."""
        if not self.expires_at:
            return False

        expires_at = self.expires_at
        if isinstance(expires_at, str):
            expires_at = datetime.datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S")

        now_datetime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        refresh_threshold = expires_at - datetime.timedelta(seconds=self.token_refresh_buffer_seconds)
        return now_datetime >= refresh_threshold

    def list_issues(
        self,
        since = None,
        after = None,
        first: int = 50,
        team_id = None,
        include_archived= True,
    ):
        query = """
        query Issues($after: String, $first: Int!, $filter: IssueFilter, $includeArchived: Boolean) {
          issues(after: $after, first: $first, filter: $filter, includeArchived: $includeArchived, orderBy: updatedAt) {
            nodes {
              id
              identifier
              title
              description
              state { name }
              creator { name }
              assignee { name }
              labels { nodes { name color } }
              priority
              dueDate
              createdAt
              updatedAt
              completedAt
              archivedAt
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        filter_data = {}
        if since:
            filter_data["updatedAt"] = {"gte": since}
        if team_id:
            filter_data["team"] = {"id": {"eq": team_id}}

        variables = {
            "after": after,
            "first": first,
            "filter": filter_data or None,
            "includeArchived": include_archived,
        }
        payload = {
            "query": query,
            "variables": variables,
        }
        if self._is_expired():
            self.refresh_access_token()
        response = requests.post(self.api_url, json=payload, headers=self._headers(), timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        result = data.get("data", {})
        issues_data = result.get("issues", {}) or {}
        return issues_data.get("nodes", []) or [], issues_data.get("pageInfo", {}) or {}

    def list_issue_comments(
        self,
        issue_id: str,
        after = None,
        first: int = 50,
    ):
        query = """
        query IssueComments($issueId: String!, $after: String, $first: Int!) {
          issue(id: $issueId) {
            comments(after: $after, first: $first) {
              nodes {
                id
                body
                createdAt
                updatedAt
                user { name }
              }
              pageInfo { hasNextPage endCursor }
            }
          }
        }
        """
        variables = {
            "issueId": issue_id,
            "after": after,
            "first": first,
        }
        payload = {
            "query": query,
            "variables": variables,
        }
        if self._is_expired():
            self.refresh_access_token()
        response = requests.post(self.api_url, json=payload, headers=self._headers(), timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        result = data.get("data", {})
        issue = result.get("issue") or {}
        comments = issue.get("comments") or {}
        return comments.get("nodes", []) or [], comments.get("pageInfo", {}) or {}

    def get_workflow_states(self, team_id=None, first: int = 100):
        states = []
        if self._is_expired():
            self.refresh_access_token()

        cursor = None
        while True:
            query = """
            query WorkflowStates($after: String, $first: Int!, $filter: WorkflowStateFilter) {
              workflowStates(after: $after, first: $first, filter: $filter) {
                nodes { id name color }
                pageInfo { hasNextPage endCursor }
              }
            }
            """
            filter_data = {}
            if team_id:
                filter_data["team"] = {"id": {"eq": team_id}}
            variables = {
                "after": cursor,
                "first": first,
                "filter": filter_data or None,
            }
            payload = {
                "query": query,
                "variables": variables,
            }
            response = requests.post(self.api_url, json=payload, headers=self._headers(), timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            result = data.get("data", {})
            conn = result.get("workflowStates") or {}
            nodes = conn.get("nodes") or []
            states.extend(nodes)
            page_info = conn.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return states


    def get_issue_labels(self, first: int = 100):
        labels = []
        if self._is_expired():
            self.refresh_access_token()

        cursor = None
        while True:
            query = """
            query IssueLabels($after: String, $first: Int!, $filter: IssueLabelFilter) {
              issueLabels(after: $after, first: $first, filter: $filter) {
                nodes { id name color }
                pageInfo { hasNextPage endCursor }
              }
            }
            """
            filter_data = {}
            variables = {
                "after": cursor,
                "first": first,
                "filter": filter_data or None,
            }
            payload = {
                "query": query,
                "variables": variables,
            }
            response = requests.post(self.api_url, json=payload, headers=self._headers(), timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            result = data.get("data", {})
            conn = result.get("issueLabels") or {}
            nodes = conn.get("nodes") or []
            labels.extend(nodes)
            page_info = conn.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return labels

    @staticmethod
    def calc_expires_at(expires_in):
        """Calculate absolute expiry datetime from expires_in seconds.
        Returns an aware datetime or None."""
        if not expires_in:
            return None
        expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            seconds=int(expires_in)
        )
        return expires_at.replace(microsecond=0)
