from seaqa_indexer.utils.constants import CONNECTION_TYPE_TO_INDEX_PREFIX
from seaqa_indexer.utils import md5
import requests


def gen_index_name_by_type(project_uuid, connection_type):
    return CONNECTION_TYPE_TO_INDEX_PREFIX.get(connection_type) + str(project_uuid)


def get_all_id_to_site_info_by_site_id(site_index, site_index_name, connection_id):
    per_size = 1000
    start = 0
    site_id_to_site_info = {}
    while True:
        hits, total = site_index.query_data_by_connection_id(site_index_name, connection_id, start, per_size)
        for hit in hits:
            source = hit['_source']
            url = source['url']
            key = md5(str(connection_id) + url)
            site_id_to_site_info[key] = source

        start += per_size
        if len(hits) < per_size:
            break
    return site_id_to_site_info


def get_path_to_index_info_by_connection_id(seafile_index, index_name, connection_id):
    per_size = 1000
    start = 0
    path_to_index_info = {}
    while True:
        hits, total = seafile_index.query_data_by_connection_id(index_name, connection_id, start, per_size)
        for hit in hits:
            source = hit['_source']
            # _id = hit['_id']
            repo_id = source['repo_id']
            path = source['path']  # parent_dir
            filename = source['filename']
            key = md5(str(connection_id) + repo_id + path + filename)
            path_to_index_info[key] = source

        start += per_size
        if len(hits) < per_size:
            break
    return path_to_index_info


def get_all_id_to_issue_info_by_issue_id(github_issue_index, site_index_name, connection_id):
    per_size = 1000
    start = 0
    site_id_to_site_info = {}
    while True:
        hits, total = github_issue_index.query_issue_by_ids(site_index_name, connection_id)
        for hit in hits:
            source = hit['_source']
            url = source['url']
            key = md5(str(connection_id) + url)
            site_id_to_site_info[key] = source

        start += per_size
        if len(hits) < per_size:
            break
    return site_id_to_site_info


def get_content_by_download_link(download_url):
    response = requests.get(download_url)
    if response.status_code != 200:
        raise ConnectionError(response.status_code, response.text)
    file_content = response.content

    return file_content
