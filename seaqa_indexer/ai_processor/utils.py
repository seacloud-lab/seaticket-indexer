import jwt
import time
import requests

from urllib.parse import urljoin

from seaqa_indexer.config import JWT_PRIVATE_KEY, SEAQA_AI_INNER_SERVER_URL
from seaqa_indexer.utils.constants import LLM_INPUT_CHARACTERS_LIMIT


def generate_summary(content, connection_type, username='seaqa-indexer', include_vector=True):
    params = {
        'content': content[:LLM_INPUT_CHARACTERS_LIMIT],
        'username': username,
        'connection_type': connection_type,
        'include_vector': include_vector,
    }
    payload = {'exp': int(time.time()) + 300, }
    token = jwt.encode(payload, JWT_PRIVATE_KEY, algorithm='HS256')
    headers = {"Authorization": "Token %s" % token}
    url = urljoin(SEAQA_AI_INNER_SERVER_URL, '/generate-summary')
    resp = requests.post(url, json=params, headers=headers)
    if resp.status_code != 200:
        raise Exception('generate summary error status: %s body: %s', resp.status_code, resp.text)
    resp_json = resp.json()
    summary = resp_json.get('summary', '')
    vector = resp_json.get('embedding', [])
    return summary, vector
