import jwt
import time
import requests

from urllib.parse import urljoin

from seaqa_indexer.config import JWT_PRIVATE_KEY, SEAQA_AI_SERVER_URL
from seaqa_indexer.utils.constants import LLM_INPUT_CHARACTERS_LIMIT


def generate_summary(content, connection_type, project_uuid, username='seaqa-indexer', org_id=-1, include_vector=True):
    params = {
        'content': content[:LLM_INPUT_CHARACTERS_LIMIT],
        'username': username,
        'connection_type': connection_type,
        'project_uuid': project_uuid,
        'org_id': org_id,
        'include_vector': include_vector,
    }
    payload = {'exp': int(time.time()) + 300, }
    token = jwt.encode(payload, JWT_PRIVATE_KEY, algorithm='HS256')
    headers = {"Authorization": "Token %s" % token}
    url = urljoin(SEAQA_AI_SERVER_URL, '/generate-summary')
    resp = requests.post(url, json=params, headers=headers)
    if resp.status_code != 200:
        raise Exception('generate summary error status: %s body: %s', resp.status_code, resp.text)
    resp_json = resp.json()
    summary = resp_json.get('summary', '')
    vector = resp_json.get('embedding', [])
    return summary, vector


def generate_embedding(query_text, org_id=-1, username='seaqa-indexer'):
    params = {
        'content': query_text,
        'username': username,
        'org_id': org_id,
    }
    payload = {'exp': int(time.time()) + 300}
    token = jwt.encode(payload, JWT_PRIVATE_KEY, algorithm='HS256')
    headers = {"Authorization": "Token %s" % token}
    url = urljoin(SEAQA_AI_SERVER_URL, '/generate-embedding')
    resp = requests.post(url, json=params, headers=headers, timeout=30)
    
    if resp.status_code != 200:
        raise Exception('Generate embedding error status: %s body: %s', resp.status_code, resp.text)
    
    resp_json = resp.json()
    embedding = resp_json.get('embedding', [])
    return embedding
