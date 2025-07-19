import json
import logging
import requests

logger = logging.getLogger(__name__)


def parse_response(response):
    if response.status_code == 400:
        logger.warning('seahub error: %s', response.text)
    if response.status_code > 400:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            return json.loads(response.text)
        except:
            pass


class SeahubAPI(object):

    def __init__(self, server, token, timeout=180):
        self.token = token
        self.server = server
        self.timeout = timeout
        self.headers = None
        self.gen_header()

    def gen_header(self):
        self.headers = {
            'accept': 'application/json',
            'authorization': 'Bearer ' + self.token
        }

    def list_items_in_dir(self, path, item_type=None, is_recursive=1):
        """
        path: Path to the dir
        item_type: Items' type which will be list, limit in 'f' and 'd', means file and directory
        is_recursive: List items recursively
        """
        url = self.server + '/api/v2.1/via-repo-token/dir/'
        data = {
            'path': path,
            'type': item_type,
            'recursive': is_recursive,
        }

        response = requests.get(url, headers=self.headers, params=data)

        return parse_response(response)

    def get_download_link(self, path):
        import requests
        url = self.server + '/api/v2.1/via-repo-token/download-link/'

        data = {
            'path': path,
        }

        response = requests.get(url, headers=self.headers, params=data)

        return parse_response(response)

    def get_repo_info(self):
        url = self.server + '/api/v2.1/via-repo-token/repo-info/'

        response = requests.get(url, headers=self.headers)

        return parse_response(response)
