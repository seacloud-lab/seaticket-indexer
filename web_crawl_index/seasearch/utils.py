from web_crawl_index.utils.constants import SITE_INDEX_PREFIX


def gen_site_index_name(project_uuid):
    return SITE_INDEX_PREFIX + str(project_uuid)
