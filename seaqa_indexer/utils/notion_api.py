import time
import requests

NOTION_VERSION = "2025-09-03"

class NotionAPI:
    def __init__(self, token, timeout=60):
        self.token = token
        self.timeout = timeout
        self.headers = self._gen_headers()
        self.base_url = 'https://api.notion.com/v1'
        self._user_cache = {}
        self.session = requests.Session()


    def _gen_headers(self):
        headers = {
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        return headers


    def fetch_page(self, page_id):
        url = f"{self.base_url}/pages/{page_id}"
        resp = self.session.get(url, headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

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

        resp = self.session.post(url, headers=self.headers, json=payload, timeout=self.timeout)
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

            resp = self.session.get(url, headers=self.headers, params=params, timeout=self.timeout)
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


    def fetch_full_page(self, page_id):
        blocks = self.fetch_blocks(page_id)

        page = self.fetch_page(page_id)
        created_by = page["created_by"]["id"]
        last_edited_by = page["last_edited_by"]["id"]
        creator = self.get_user_info(created_by)
        last_modifier = self.get_user_info(last_edited_by)

        parent = page.get("parent") or {}
        parent_page_id = parent.get("page_id") if parent.get("type") == "page_id" else None

        return {
            "page_id": page["id"],
            "parent_page_id": parent_page_id,
            "title": self.extract_page_title(page),
            'created_time': page['created_time'],
            "last_modified_time": page['last_edited_time'],
            'creator': creator,
            'last_modifier': last_modifier,
            "url": page['url'],
            "blocks": blocks,
        }


    def get_user_info(self, user_id):
        url = f"{self.base_url}/users/{user_id}"
        try:
            resp = self.session.get(url, headers=self.headers, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
            name = data.get('name')
        except Exception:
            name = None
        return name
