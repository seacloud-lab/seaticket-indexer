import requests

from seaqa_indexer.utils.constants import CONNECTION_TYPE_TO_INDEX_PREFIX, ConnectionType


def gen_index_name_by_type(project_uuid, connection_type, connection_id=None):
    if connection_type == ConnectionType.DISCOURSE_FORUM.value and connection_id:
        return CONNECTION_TYPE_TO_INDEX_PREFIX.get(connection_type) + str(connection_id)
    return CONNECTION_TYPE_TO_INDEX_PREFIX.get(connection_type) + str(project_uuid)


def get_content_by_download_link(download_url):
    response = requests.get(download_url)
    if response.status_code != 200:
        raise ConnectionError(response.status_code, response.text)
    file_content = response.content

    return file_content
