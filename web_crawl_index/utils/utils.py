import requests

from web_crawl_index.config import DTABLE_WEB_SERVICE_URL


def get_dtable_uuid_by_api_token(api_token, dtable_web_url=DTABLE_WEB_SERVICE_URL):
    url = f"{dtable_web_url}/api/v2.1/dtable/app-access-token/"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "accept": "application/json",
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('dtable_uuid')
    return None


def get_access_token_by_api_token(api_token, dtable_web_url=DTABLE_WEB_SERVICE_URL):
    url = f"{dtable_web_url}/api/v2.1/dtable/app-access-token/"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "accept": "application/json",
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('access_token')
    return None
