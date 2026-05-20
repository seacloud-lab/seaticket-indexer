import datetime
import requests

from seaqa_indexer.config import LINEAR_CLIENT_ID, LINEAR_CLIENT_SECRET


class LinearAPIException(Exception):
    pass


class LinearAPIAuthException(LinearAPIException):
    pass


class LinearAPI:
    def __init__(
        self,
        access_token: str,
        refresh_token,
        timeout: int = 60,
    ):
        self.access_token = access_token
        self.refresh_token = refresh_token
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
        if not (self.refresh_token and self.client_id and self.client_secret):
            return None

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        response = requests.post(self.token_url, data=payload, timeout=self.timeout)
        if response.status_code >= 400:
            raise LinearAPIAuthException(f"Linear OAuth refresh failed {response.status_code}: {response.text}")
        data = response.json() or {}
        access_token = data.get("access_token")
        if not access_token:
            raise LinearAPIAuthException("Linear OAuth refresh response missing access_token")

        self.access_token = access_token
        if data.get("refresh_token"):
            self.refresh_token = data.get("refresh_token")

        self._last_token_update = data
        return data

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
        response = requests.post(self.api_url, json=payload, headers=self._headers(), timeout=self.timeout)
        if response.status_code == 401:
            if self.refresh_access_token():
                response = requests.post(self.api_url, json=payload, headers=self._headers(), timeout=self.timeout)
            else:
                raise LinearAPIAuthException(f"Linear API unauthorized: {response.status_code}")
        if response.status_code >= 400:
            raise LinearAPIException(f"Linear API error {response.status_code}: {response.text}")
        data = response.json() or {}
        if "errors" in data and data["errors"]:
            raise LinearAPIException(data["errors"])
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
        response = requests.post(self.api_url, json=payload, headers=self._headers(), timeout=self.timeout)
        if response.status_code == 401:
            if self.refresh_access_token():
                response = requests.post(self.api_url, json=payload, headers=self._headers(), timeout=self.timeout)
            else:
                raise LinearAPIAuthException(f"Linear API unauthorized: {response.status_code}")
        if response.status_code >= 400:
            raise LinearAPIException(f"Linear API error {response.status_code}: {response.text}")
        data = response.json() or {}
        if "errors" in data and data["errors"]:
            raise LinearAPIException(data["errors"])
        result = data.get("data", {})
        issue = result.get("issue") or {}
        comments = issue.get("comments") or {}
        return comments.get("nodes", []) or [], comments.get("pageInfo", {}) or {}

    def get_workflow_states(self, team_id=None, first: int = 100):
        states = []
        try:
            cursor = None
            while True:
                query = """
                query WorkflowStates($after: String, $first: Int!, $filter: WorkflowStateFilter) {
                  workflowStates(after: $after, first: $first, filter: $filter) {
                    nodes { name color }
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
                if response.status_code == 401:
                    if self.refresh_access_token():
                        response = requests.post(self.api_url, json=payload, headers=self._headers(), timeout=self.timeout)
                    else:
                        raise LinearAPIAuthException(f"Linear API unauthorized: {response.status_code}")
                if response.status_code >= 400:
                    raise LinearAPIException(f"Linear API error {response.status_code}: {response.text}")
                data = response.json() or {}
                if "errors" in data and data["errors"]:
                    raise LinearAPIException(data["errors"])
                result = data.get("data", {})
                conn = result.get("workflowStates") or {}
                nodes = conn.get("nodes") or []
                states.extend(nodes)
                page_info = conn.get("pageInfo") or {}
                if not page_info.get("hasNextPage"):
                    break
                cursor = page_info.get("endCursor")
            return states
        except Exception:
            pass

        return states

    def get_issue_labels(self, first: int = 100):
        labels = []
        try:
            cursor = None
            while True:
                query = """
                query IssueLabels($after: String, $first: Int!, $filter: IssueLabelFilter) {
                  issueLabels(after: $after, first: $first, filter: $filter) {
                    nodes { name color }
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
                if response.status_code == 401:
                    if self.refresh_access_token():
                        response = requests.post(self.api_url, json=payload, headers=self._headers(), timeout=self.timeout)
                    else:
                        raise LinearAPIAuthException(f"Linear API unauthorized: {response.status_code}")
                if response.status_code >= 400:
                    raise LinearAPIException(f"Linear API error {response.status_code}: {response.text}")
                data = response.json() or {}
                if "errors" in data and data["errors"]:
                    raise LinearAPIException(data["errors"])
                result = data.get("data", {})
                conn = result.get("issueLabels") or {}
                nodes = conn.get("nodes") or []
                labels.extend(nodes)
                page_info = conn.get("pageInfo") or {}
                if not page_info.get("hasNextPage"):
                    break
                cursor = page_info.get("endCursor")
            return labels
        except Exception:
            pass

        return labels

    @staticmethod
    def calc_expires_in(expires_in):
        if not expires_in:
            return None
        expires_in = datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=int(expires_in))
        return expires_in.astimezone(datetime.UTC).replace(tzinfo=None, microsecond=0)
