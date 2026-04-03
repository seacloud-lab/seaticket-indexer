import jwt
import time
import requests

from urllib.parse import urljoin

from seaqa_indexer.config import JWT_PRIVATE_KEY, SEAQA_AI_INNER_SERVER_URL
from seaqa_indexer.utils.constants import LLM_INPUT_CHARACTERS_LIMIT
from seaqa_indexer.utils.constants import AIScenario


def generate_summary(content, type, project_uuid, username='seaqa-indexer', include_vector=True, scenario=AIScenario.SUMMARY.value):
    params = {
        'content': content[:LLM_INPUT_CHARACTERS_LIMIT],
        'username': username,
        'type': type,
        'project_uuid': project_uuid,
        'include_vector': include_vector,
        'scenario': scenario,
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
    vector = resp_json.get('embeddings', [])
    return summary, vector


def generate_embedding(query_text, project_uuid, username='seaqa-indexer', scenario=AIScenario.UNKNOWN.value):
    params = {
        'content': query_text,
        'username': username,
        'project_uuid': project_uuid,
        'scenario': scenario,
    }
    payload = {'exp': int(time.time()) + 300}
    token = jwt.encode(payload, JWT_PRIVATE_KEY, algorithm='HS256')
    headers = {"Authorization": "Token %s" % token}
    url = urljoin(SEAQA_AI_INNER_SERVER_URL, '/generate-embedding')
    resp = requests.post(url, json=params, headers=headers, timeout=30)

    if resp.status_code != 200:
        raise Exception('Generate embedding error status: %s body: %s', resp.status_code, resp.text)

    resp_json = resp.json()
    assert 'embedding' in resp_json, 'Generate embedding error: invalid response'

    return resp_json['embedding']

def generate_chunks_embedding(content, project_uuid, username='seaqa-indexer', scenario=AIScenario.SUMMARY.value):
    params = {
        'content': content,
        'username': username,
        'project_uuid': project_uuid,
        'scenario': scenario,
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
