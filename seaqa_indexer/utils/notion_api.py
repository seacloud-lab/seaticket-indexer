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
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        return resp.json()


    def search_pages(self, since, per_page, page):
        pages = []
        cursor = None

        while True:
            url = f"{self.base_url}/search"
            payload = {
                "page_size": 100,
                "filter": {"property": "object", "value": "page"},
                "sort": {"direction": "descending", "timestamp": "last_edited_time"},
            }
            if cursor:
                payload["start_cursor"] = cursor

            resp = requests.post(url, headers=self.headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
            pages.extend(data.get('results') or [])

            if not data.get('has_more'):
                break
            cursor = data.get('next_cursor')
            if not cursor:
                break
            time.sleep(0.34)

        return pages


    def fetch_blocks(self, block_id):
        blocks = []
        cursor = None

        while True:
            url = f"{self.base_url}/blocks/{block_id}/children"
            params = {"page_size": 100}
            if cursor:
                params["start_cursor"] = cursor

            resp = requests.get(url, headers=self.headers, params=params)
            resp.raise_for_status()
            data = resp.json()

            for block in data["results"]:
                if block.get("has_children"):
                    block["children"] = self.fetch_blocks(block["id"])
                blocks.append(block)

            cursor = data.get("next_cursor")
            if not cursor:
                break

            #avoid the api rate limit psc
            time.sleep(0.34)

        return blocks


    def extract_page_title(self, page):
        for prop in page["properties"].values():
            if prop["type"] == "title":
                return "".join(
                    t["plain_text"] for t in prop["title"]
                )
        return ""


    def fetch_full_page(self, page_id, since=None, state=None, per_page=None, page=None):
        blocks = self.fetch_blocks(page_id)
        if not blocks:
            return {}

        page = self.fetch_page(page_id)
        created_by = page["created_by"]["id"]
        last_edited_by = page["last_edited_by"]["id"]
        creator = self.get_user_info(created_by)
        last_modifier = self.get_user_info(last_edited_by)

        return {
            "page_id": page["id"],
            "title": self.extract_page_title(page),
            'created_time': page['created_time'],
            "last_modified_time": page['last_edited_time'],
            'creator': creator,
            'last_modifier': last_modifier,
            "url": page['url'],
            "blocks": blocks,
        }


    def get_user_info(self, user_id):
        if not user_id:
            return None

        if user_id in self._user_cache:
            return self._user_cache[user_id]

        url = f"{self.base_url}/users/{user_id}"
        try:
            resp = requests.get(url, headers=self.headers)
            if resp.status_code == 404:
                # Notion system/bot users can appear here; treat as non-fatal.
                self._user_cache[user_id] = None
                return None
            resp.raise_for_status()
            data = resp.json()
            name = data.get('name')
            self._user_cache[user_id] = name
            return name
        except Exception:
            self._user_cache[user_id] = None
            return None
