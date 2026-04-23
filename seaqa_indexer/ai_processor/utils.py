import logging
import jwt
import time
import requests

from urllib.parse import urljoin

from seaqa_indexer.config import JWT_PRIVATE_KEY, SEAQA_AI_INNER_SERVER_URL
from seaqa_indexer.utils.constants import LLM_INPUT_CHARACTERS_LIMIT
from seaqa_indexer.utils.constants import AIScenario


logger = logging.getLogger(__name__)


class SeaQAAIAPI:
    def _build_auth_headers(self):
        payload = {'exp': int(time.time()) + 300}
        token = jwt.encode(payload, JWT_PRIVATE_KEY, algorithm='HS256')
        return {"Authorization": "Token %s" % token}

    def _post_ai_request(self, api_path, params, timeout=None):
        url = urljoin(SEAQA_AI_INNER_SERVER_URL, api_path)
        request_kwargs = {
            'json': params,
            'headers': self._build_auth_headers(),
        }
        if timeout is not None:
            request_kwargs['timeout'] = timeout
        return requests.post(url, **request_kwargs)

    def generate_summary(self, content, type, project_uuid, username='seaqa-indexer', scenario=AIScenario.SUMMARY.value):
        params = {
            'content': content[:LLM_INPUT_CHARACTERS_LIMIT],
            'username': username,
            'type': type,
            'project_uuid': project_uuid,
            'scenario': scenario,
        }
        resp = self._post_ai_request('/generate-summary', params)
        if resp.status_code != 200:
            raise Exception('generate summary error status: %s body: %s', resp.status_code, resp.text)
        resp_json = resp.json()
        summary = resp_json.get('summary', '')
        return summary

    def generate_embedding(self, query_text, project_uuid, username='seaqa-indexer', scenario=AIScenario.SUMMARY.value):
        params = {
            'content': query_text,
            'username': username,
            'project_uuid': project_uuid,
            'scenario': scenario,
        }
        resp = self._post_ai_request('/embedding/generate', params, timeout=30)

        if resp.status_code != 200:
            raise Exception('Generate embedding error status: %s body: %s', resp.status_code, resp.text)

        resp_json = resp.json()
        embedding = resp_json.get('embedding')
        assert isinstance(embedding, list), 'Generate embedding error: invalid response'

        return embedding

    def batch_generate_embeddings(self, texts, project_uuid, username='seaqa-indexer', scenario=AIScenario.SUMMARY.value):
        params = {
            'content': texts,
            'username': username,
            'project_uuid': project_uuid,
            'scenario': scenario,
        }
        resp = self._post_ai_request('/embedding/batch-generate', params, timeout=30)

        if resp.status_code != 200:
            raise Exception('Batch generate embeddings error status: %s body: %s', resp.status_code, resp.text)

        resp_json = resp.json()
        embeddings = resp_json.get('embeddings')
        assert isinstance(embeddings, list), 'Batch generate embeddings error: invalid response'
        assert len(embeddings) == len(texts), 'Batch generate embeddings error: response length mismatch'
        return embeddings

    def generate_chunks_embedding(self, content, project_uuid, username='seaqa-indexer', scenario=AIScenario.SUMMARY.value):
        params = {
            'content': content,
            'username': username,
            'project_uuid': project_uuid,
            'scenario': scenario,
        }
        resp = self._post_ai_request('/embedding/generate-by-chunks', params)
        if resp.status_code != 200:
            raise Exception('generate chunks embedding error status: %s body: %s', resp.status_code, resp.text)
        resp_json = resp.json()
        chunks = resp_json.get('chunks', [])
        embeddings = resp_json.get('embeddings', [])
        assert isinstance(chunks, list), 'generate chunks embedding error: invalid chunks response'
        assert isinstance(embeddings, list), 'generate chunks embedding error: invalid embeddings response'
        assert len(chunks) == len(embeddings), 'generate chunks embedding error: length of chunks and embeddings mismatch'
        return chunks, embeddings
