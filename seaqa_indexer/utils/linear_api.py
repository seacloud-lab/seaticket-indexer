import datetime
import requests

from seaqa_indexer.config import LINEAR_CLIENT_ID, LINEAR_CLIENT_SECRET


class LinearAPI:
    def __init__(
        self,
        access_token,
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
        if response.status_code == 400 and response.text:
            raise Exception(response.text)
        response.raise_for_status()
        data = response.json() or {}
        access_token = data.get("access_token")
        self.access_token = access_token
        self.refresh_token = data.get("refresh_token")

        expires_at = self.calc_expires_at(data.get("expires_in"))
        access_token = data.get("access_token")
        refresh_token = data.get("refresh_token")

        return {'access_token': access_token, 'refresh_token': refresh_token, 'expires_at': expires_at}

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
              state { name type }
              creator { id name }
              assignee { id name }
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
                user { id name }
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
        response.raise_for_status()
        data = response.json()
        result = data.get("data", {})
        issue = result.get("issue") or {}
        comments = issue.get("comments") or {}
        return comments.get("nodes", []) or [], comments.get("pageInfo", {}) or {}

    def get_workflow_states(self, team_id=None, first: int = 100):
        states = []

        cursor = None
        while True:
            query = """
            query WorkflowStates($after: String, $first: Int!, $filter: WorkflowStateFilter) {
              workflowStates(after: $after, first: $first, filter: $filter) {
                nodes { id name color type }
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
        return expires_at
