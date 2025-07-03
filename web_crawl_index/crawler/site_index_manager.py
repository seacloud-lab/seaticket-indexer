import os
import logging
import json
from sqlalchemy.sql import text

from web_crawl_index.db import init_db_session_class
from web_crawl_index.utils.constants import CrawlStatus
from web_crawl_index.config import SEASEARCH_URL, SEASEARCH_TOKEN
from web_crawl_index.seasearch.index_store.site_index import SiteIndex
from web_crawl_index.utils.seasearch_api import SeaSearchAPI
from web_crawl_index.seasearch.utils import gen_site_index_name, get_all_index_by_site
from web_crawl_index.crawler.utils import get_site_dir


logger = logging.getLogger(__name__)


class SiteIndexManager:
    def __init__(self):
        self.db_session = init_db_session_class()
        self.seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        self.site_index = SiteIndex(self.seasearch_api)

    def get_project_crawled_completed_sites_by_page(self, start, limit):
        with self.db_session() as session:
            sql = """SELECT s.id as site_id, s.url, s.sitemap_url, s.project_id, s.status, p.uuid, p.name as project_name
             FROM sites s 
            INNER JOIN projects p 
            ON p.id=s.project_id WHERE p.deleted=false AND s.status=:status LIMIT :start, :limit"""
            sites = session.execute(text(sql), {'start': start, 'limit': limit, 'status': CrawlStatus.COMPLETED}).fetchall()
            return sites

    def update_site_index(self, site):
        site_id = site.site_id
        site_url = site.url
        project_uuid = site.uuid

        site_dir = get_site_dir(site_url, project_uuid)
        crawled_site_files = [f for f in os.listdir(site_dir)]
        site_index_name = gen_site_index_name(project_uuid)
        self.site_index.create_index_if_missing(site_index_name)

        index_site_url_to_site_info = get_all_index_by_site(self.site_index, site_index_name, site_id)
        index_site_urls_set = set(index_site_url_to_site_info.keys())
        crawled_site_urls_set = set()

        indexed_site_info = index_site_url_to_site_info.get(site_url)

        for filename in crawled_site_files:
            filepath = os.path.join(site_dir, filename)

            with open(filepath, 'r', encoding='utf-8') as f:
                crawled_data = json.load(f)

            url = crawled_data.get('url')
            etag = crawled_data.get('etag')
            last_modified = crawled_data.get('last_modified')
            content = crawled_data.get('content')
            title = crawled_data.get('title')

            crawled_site_urls_set.add(url)

            logger.debug('start to update url: %s', url)

            if not indexed_site_info:
                self.site_index.add_document(site_index_name, site_id, content, url, etag, title, last_modified)
            else:
                if (etag and indexed_site_info.get('etag') == etag) or \
                        (last_modified and indexed_site_info.get('last_modified') == last_modified):
                    continue
                self.site_index.update_document(site_index_name, site_id, content, url, etag, title, last_modified)

        # clear deleted site crawled url
        need_cleared_site_url_index_set = index_site_urls_set - crawled_site_urls_set
        self.site_index.delete_indexes_by_urls(site_index_name, need_cleared_site_url_index_set)

    def update_site_indexes(self):
        start, limit = 0, 1000
        while True:
            try:
                project_sites = self.get_project_crawled_completed_sites_by_page(start, limit)
            except Exception as e:
                logger.error("Error: %s" % e)
                return
            start += 1000
            if len(project_sites) == 0:
                break
            for site in project_sites:
                self.update_site_index(site)

    def search(self, project_uuid, keyword, start=0, size=20):
        site_index_name = gen_site_index_name(project_uuid)
        return self.site_index.search(site_index_name, keyword, start, size)
