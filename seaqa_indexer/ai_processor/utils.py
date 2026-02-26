import jwt
import time
import requests

from urllib.parse import urljoin

from seaqa_indexer.config import JWT_PRIVATE_KEY, SEAQA_AI_INNER_SERVER_URL
from seaqa_indexer.utils.constants import LLM_INPUT_CHARACTERS_LIMIT


def generate_summary(content, type, project_uuid, lang='en', username='seaqa-indexer', include_vector=True):
    params = {
        'content': content[:LLM_INPUT_CHARACTERS_LIMIT],
        'username': username,
        'type': type,
        'project_uuid': project_uuid,
        'include_vector': include_vector,
        'lang': lang,
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


def generate_embedding(query_text, project_uuid, username='seaqa-indexer'):
    params = {
        'content': query_text,
        'username': username,
        'project_uuid': project_uuid,
    }
    payload = {'exp': int(time.time()) + 300}
    token = jwt.encode(payload, JWT_PRIVATE_KEY, algorithm='HS256')
    headers = {"Authorization": "Token %s" % token}
    url = urljoin(SEAQA_AI_INNER_SERVER_URL, '/generate-embedding')
    resp = requests.post(url, json=params, headers=headers, timeout=30)
    
    if resp.status_code != 200:
        raise Exception('Generate embedding error status: %s body: %s', resp.status_code, resp.text)
    
    resp_json = resp.json()
    embedding = resp_json.get('embedding', [])
    return embedding

def generate_chunks_embedding(content, project_uuid, username='seaqa-indexer'):
    params = {
        'content': content,
        'username': username,
        'project_uuid': project_uuid,
    }
    payload = {'exp': int(time.time()) + 300}
    token = jwt.encode(payload, JWT_PRIVATE_KEY, algorithm='HS256')
    headers = {"Authorization": "Token %s" % token}
    url = urljoin(SEAQA_AI_INNER_SERVER_URL, '/generate-chunks-embedding')
    resp = requests.post(url, json=params, headers=headers)
    if resp.status_code != 200:
        raise Exception('generate chunks embedding error status: %s body: %s', resp.status_code, resp.text)
    resp_json = resp.json()
    chunks = resp_json.get('chunks', [])
    embeddings = resp_json.get('embeddings', [])
    return chunks, embeddings
