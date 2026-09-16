import time
import base64
import requests

from seaqa_indexer.config import NOTION_CLIENT_ID, NOTION_CLIENT_SECRET
from seaqa_indexer.utils import calc_oauth_expires_at

NOTION_VERSION = "2026-03-11"


class UserNotFoundException(Exception):
    pass


class NotionAPI:
    def __init__(self, access_token, refresh_token, timeout=60):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.timeout = timeout
        self.base_url = 'https://api.notion.com/v1'
        self._user_cache = {}
        self.session = requests.Session()

    def _headers(self):
        headers = {
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }
        if self.access_token:
            headers['Authorization'] = f'Bearer {self.access_token}'
        return headers

    def refresh_access_token(self):
        auth = base64.b64encode(f'{NOTION_CLIENT_ID}:{NOTION_CLIENT_SECRET}'.encode()).decode()
        headers = {
            'Authorization': f'Basic {auth}',
            'Content-Type': 'application/json',
            'Notion-Version': NOTION_VERSION,
        }
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
        }
        response = self.session.post(
            f'{self.base_url}/oauth/token',
            headers=headers,
            json=payload,
            timeout=self.timeout,
        )
        if response.status_code == 400 and response.text:
            raise Exception(response.text)
        response.raise_for_status()
        data = response.json() or {}

        self.access_token = data.get('access_token')
        new_refresh_token = data.get('refresh_token')
        if new_refresh_token:
            self.refresh_token = new_refresh_token
        self.expires_at = calc_oauth_expires_at(data.get('expires_in'))

        return {
            'access_token': self.access_token,
            'refresh_token': self.refresh_token,
            'expires_at': self.expires_at,
        }

    def search_pages(self, per_page, start_cursor=None):
        pages = []

        url = f"{self.base_url}/search"
        payload = {
            "page_size": per_page,
            "filter": {"property": "object", "value": "page"},
            "sort": {"direction": "descending", "timestamp": "last_edited_time"},
        }
        if start_cursor:
            payload["start_cursor"] = start_cursor

        resp = self.session.post(url, headers=self._headers(), json=payload, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        results = data.get('results') or []
        pages.extend(results)

        return pages, data.get('next_cursor'), bool(data.get('has_more'))

    def fetch_blocks(self, block_id):
        blocks = []
        cursor = None

        while True:
            url = f"{self.base_url}/blocks/{block_id}/children"
            params = {"page_size": 100}
            if cursor:
                params["start_cursor"] = cursor

            resp = self.session.get(url, headers=self._headers(), params=params, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()

            for block in data["results"]:
                if block.get("has_children"):
                    block["children"] = self.fetch_blocks(block["id"])
                blocks.append(block)

            cursor = data.get("next_cursor")
            if not cursor:
                break

            time.sleep(0.34)

        return blocks

    def extract_page_title(self, page):
        for prop in page["properties"].values():
            if prop["type"] == "title":
                return "".join(
                    t["plain_text"] for t in prop["title"]
                )
        return ""

    def fetch_full_page(self, page):
        page_id = page.get("id")

        blocks = self.fetch_blocks(page_id)

        created_by = page.get("created_by").get("id")
        last_edited_by = page.get("last_edited_by").get("id")
        try:
            creator = self.get_user_info(created_by)
        except UserNotFoundException:
            creator = ''
        if last_edited_by == created_by:
            last_modifier = creator
        else:
            try:
                last_modifier = self.get_user_info(last_edited_by)
            except UserNotFoundException:
                last_modifier = ''

        parent = page.get("parent") or {}
        parent_page_id = parent.get("page_id") if parent.get("type") == "page_id" else None

        return {
            "page_id": page_id,
            "parent_page_id": parent_page_id,
            "title": self.extract_page_title(page),
            "created_time": page.get("created_time"),
            "last_modified_time": page.get("last_edited_time"),
            "creator": creator,
            "last_modifier": last_modifier,
            "url": page.get("url"),
            "blocks": blocks,
        }

    def get_user_info(self, user_id):
        url = f"{self.base_url}/users/{user_id}"
        resp = self.session.get(url, headers=self._headers(), timeout=self.timeout)
        if resp.status_code == 404:
            raise UserNotFoundException()
        resp.raise_for_status()
        data = resp.json()
        name = data.get('name')
        return name
