from web_crawl_index.utils.constants import SITE_INDEX_PREFIX


def gen_site_index_name(project_uuid):
    return SITE_INDEX_PREFIX + str(project_uuid)


def get_all_index_by_site(site_index, site_index_name, site_id):
    per_size = 1000
    start = 0
    site_url_to_site_info = {}
    while True:
        hits, total = site_index.query_data_by_site(site_index_name, site_id, start, per_size)
        for hit in hits:
            source = hit['_source']
            url = source['url']
            site_url_to_site_info[url] = source

        start += per_size
        if len(hits) < per_size:
            break
    return site_url_to_site_info
