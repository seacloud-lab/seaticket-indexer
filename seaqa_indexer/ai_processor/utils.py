import jwt
import time
import requests

from urllib.parse import urljoin

from seaqa_indexer.config import JWT_PRIVATE_KEY, SEAQA_AI_SERVER_URL



def generate_summary(params):
    payload = {'exp': int(time.time()) + 300, }
    token = jwt.encode(payload, JWT_PRIVATE_KEY, algorithm='HS256')
    headers = {"Authorization": "Token %s" % token}
    url = urljoin(SEAQA_AI_SERVER_URL, '/generate-summary')
    resp = requests.post(url, json=params, headers=headers)
    if resp.status_code != 200:
        raise Exception('generate summary error status: %s body: %s', resp.status_code, resp.text)
    resp_json = resp.json()
    summary = resp_json.get('summary', '')
    embeddings = resp_json.get('embeddings', [])
    if embeddings:
        vector = embeddings[0].get('embedding', [])
    else:
        vector = []
    return summary, vector
