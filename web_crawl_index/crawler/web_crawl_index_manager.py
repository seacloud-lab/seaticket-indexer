import os
import logging
import time

import requests
import json

import datetime
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
from sqlalchemy.sql import text
from bs4 import BeautifulSoup

from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet.threads import deferToThread

from web_crawl_index.crawler.spiders.site_crawler import SiteCrawlerSpider
from web_crawl_index.crawler.spiders.sitemap_crawler import create_sitemap_spider

from web_crawl_index.db import init_db_session_class
from web_crawl_index.utils.constants import CrawlStatus
from web_crawl_index.mq import get_mq
from web_crawl_index.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, SEASEARCH_URL, SEASEARCH_TOKEN
from web_crawl_index.seasearch.index_store.site_index import SiteIndex
from web_crawl_index.utils.seasearch_api import SeaSearchAPI
from web_crawl_index.seasearch.utils import gen_site_index_name


logger = logging.getLogger(__name__)


class WebCrawlIndexManager:
    """Crawler manager, responsible for starting and managing crawler processes"""

    def __init__(self):
        self.db_session = init_db_session_class()
        self.active_crawlers = {}
        self.settings = get_project_settings()
        self.runner = CrawlerRunner(self.settings)
        self.seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        self.site_index = SiteIndex(self.seasearch_api)

        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def get_site_by_id(self, site_id):
        with self.db_session() as session:
            sql = "SELECT * FROM sites WHERE id=:site_id"
            site = session.execute(text(sql), {'site_id': site_id}).fetchone()
            return site

    def update_site_status(self, site_id, status):
        with self.db_session() as session:
            cmd = """UPDATE sites SET status = :status WHERE id = :site_id"""
            session.execute(text(cmd), {'site_id': site_id, 'status': status})
            session.commit()

    def update_site_status_and_crawl_time(self, site_id, status):
        last_crawled_at = datetime.datetime.now(datetime.UTC)
        with self.db_session() as session:
            cmd = """UPDATE sites SET status = :status, last_crawled_at=:last_crawled_at WHERE id = :site_id"""
            session.execute(text(cmd), {'site_id': site_id, 'status': status, 'last_crawled_at': last_crawled_at})
            session.commit()

    def get_project_by_id(self, project_id):
        with self.db_session() as session:
            sql = "SELECT * FROM projects WHERE id=:project_id"
            project = session.execute(text(sql), {'project_id': project_id}).fetchone()
            return project

    def update_sitemap_url(self, site_id, sitemap_url):
        with self.db_session() as session:
            cmd = """UPDATE sites SET sitemap_url = :sitemap_url WHERE id = :site_id"""
            session.execute(text(cmd), {'site_id': site_id, 'sitemap_url': sitemap_url})
            session.commit()

    def get_project_site_by_page(self, start, limit):
        with self.db_session() as session:
            sql = """SELECT w.id as site_id, w.url, w.sitemap_url, w.project_id, w.status, p.uuid, p.name as project_name
             FROM sites w 
            INNER JOIN projects p 
            ON p.id=w.project_id WHERE p.deleted=false LIMIT :start, :limit"""
            sites = session.execute(text(sql), {'start': start, 'limit': limit}).fetchall()
            return sites

    def update_site_index(self, site):
        """Start crawler process"""
        site_id = site.id
        logger.info(f"Start crawling site {site_id}")

        project_id = site.project_id
        project = self.get_project_by_id(project_id)
        project_uuid = project.uuid

        site_index_name = gen_site_index_name(project_uuid)
        self.site_index.create_index_if_missing(site_index_name)

        # Use deferToThread to push crawler task to thread pool
        d = deferToThread(
            self.prepare_crawler,
            site_id,
            site.url,
            site.sitemap_url,
            project_uuid,
        )

        d.addCallback(self.start_crawl)
        d.addErrback(self.deal_prepare_crawler_error, site_id, site.url)

    def get_sitemap_url(self, url):
        """Try to get site's sitemap URL"""
        common_sitemap_paths = [
            "sitemap.xml",
            "sitemap_index.xml",
            "sitemap-index.xml",
            "sitemap/sitemap.xml",
            "sitemaps/sitemap.xml",
            "sitemap/index.xml",
            "robots.txt"  # Find sitemap from robots.txt
        ]

        # Ensure URL ends with /
        if not url.endswith('/'):
            url = url + '/'

        # Try common sitemap paths
        for path in common_sitemap_paths:
            try:
                sitemap_url = urljoin(url, path)
                response = requests.get(sitemap_url, timeout=10)

                if response.status_code == 200:
                    # If it's robots.txt, parse Sitemap directives
                    if path == "robots.txt":
                        for line in response.text.split('\n'):
                            if line.lower().startswith('sitemap:'):
                                sitemap_url = line.split(':', 1)[1].strip()
                                return sitemap_url
                    else:
                        # Verify if it's valid XML
                        try:
                            ET.fromstring(response.content)
                            return sitemap_url
                        except ET.ParseError:
                            logger.debug(f"Found sitemap is not valid XML: {sitemap_url}")
                            continue
            except Exception as e:
                logger.debug(f"trying to get {url} sitemap error: {str(e)}")

        logger.info(f"No sitemap found for site {url}")
        return None

    def start_crawl(self, crawler_params):
        """Callback function for starting crawler"""
        site_id = crawler_params['site_id']
        url = crawler_params['url']
        sitemap_url = crawler_params['sitemap_url']
        project_uuid = crawler_params['project_uuid']

        # Create crawler instance
        if sitemap_url:
            SitemapSpiderClass = create_sitemap_spider(
                name=f"sitemap_spider_{site_id}",  # Use unique name
                project_uuid=project_uuid,
                site_id=site_id,
                sitemap_url=sitemap_url,
                site_index=self.site_index
            )
            d = self.runner.crawl(SitemapSpiderClass)
        else:
            # Use regular spider
            d = self.runner.crawl(
                SiteCrawlerSpider,
                project_uuid,
                site_id,
                url,
                self.site_index
            )
        d.addCallback(lambda _: self.finish_crawl_site(site_id, url, CrawlStatus.COMPLETED))
        d.addErrback(lambda failure: self.finish_crawl_site(site_id, url, CrawlStatus.FAILED, str(failure)))

        return d

    def prepare_crawler(self, site_id, url, sitemap_url, project_uuid):
        """Prepare crawler parameters, execute in thread pool"""
        if not sitemap_url:
            sitemap_url = self.get_sitemap_url(url)
            if sitemap_url:
                self.update_sitemap_url(site_id, sitemap_url)

        # Return crawler parameters
        return {
            'site_id': site_id,
            'url': url,
            'sitemap_url': sitemap_url,
            'project_uuid': project_uuid,
        }

    def deal_prepare_crawler_error(self, failure, site_id, url):
        self.finish_crawl_site(site_id, url, CrawlStatus.FAILED, str(failure))
        return failure

    def finish_crawl_site(self, site_id, url, status, error_msg=None):
        if status == CrawlStatus.FAILED:
            logger.warning('crawl site failed, id: %s, url: %s, error: %s', site_id, url, error_msg)
            self.update_site_status(site_id, CrawlStatus.FAILED)
            return
        self.update_site_status_and_crawl_time(site_id, CrawlStatus.COMPLETED)

    def update_sites_indexes(self):
        """Check if all sites need to be updated"""

        start, limit = 0, 1000
        while True:
            try:
                project_sites = self.get_project_site_by_page(start, limit)
            except Exception as e:
                logger.error("Error: %s" % e)
                return
            start += 1000
            if len(project_sites) == 0:
                break
            for site in project_sites:
                site_id = site.site_id
                url = site.url

                try:
                    # Check if the site needs to be updated
                    self.update_exist_site_index(site)
                except Exception as e:
                    logger.warning('update site index failed, site_id: %s, error: %s', site_id, e)
                    self.finish_crawl_site(site_id, url, CrawlStatus.FAILED, e)
                    continue

                self.finish_crawl_site(site_id, url, CrawlStatus.COMPLETED)

    def update_exist_site_index(self, site):
        """Check if a single site needs to be updated
        """
        site_id = site.site_id
        project_uuid = site.uuid

        per_size = 100
        start = 0
        site_index_name = gen_site_index_name(project_uuid)
        while True:
            hits, total = self.site_index.query_data_by_site(site_index_name, site_id, start, per_size)
            for hit in hits:
                source = hit['_source']
                etag = source['etag']
                url = source['url']
                last_modified = source['last_modified']
                self.update_index_file(project_uuid, site_id, url, etag, last_modified)

            if len(hits) < per_size:
                break

    def update_index_file(self, project_uuid, site_id, url, etag, last_modified):
        """Check if a single URL needs to be updated
        """
        is_changed, new_etag, new_last_modified = self.check_webpage_is_changed(url, etag, last_modified)

        # If an update is needed, send a GET request to retrieve new content.
        if is_changed:
            need_update_content, title, content = self.get_webpage_content(url, new_etag, new_last_modified)
            site_index_name = gen_site_index_name(project_uuid)
            self.site_index.add_document(site_index_name, site_id, content, url, etag, title, last_modified)
        else:
            logger.debug(f"No update needed for {url}")

    def check_webpage_is_changed(self, url, etag, last_modified):
        response = requests.head(url, timeout=10, allow_redirects=True)
        # Check the ETag or Last-Modified in the HTTP header.
        new_etag = response.headers.get('ETag')
        new_last_modified = response.headers.get('Last-Modified')

        is_changed = False
        # If the server returns 304 Not Modified, there is no need to update.
        if response.status_code == 304:
            is_changed = False

        if new_etag and etag != new_etag:
            is_changed = True

        if new_last_modified and last_modified != new_last_modified:
            is_changed = True

        return is_changed, new_etag, new_last_modified

    def get_webpage_content(self, url, etag, last_modified):
        headers = {}
        if etag:
            headers['If-None-Match'] = etag
        if last_modified:
            headers['If-Modified-Since'] = last_modified

        response = requests.get(url, headers=headers, timeout=15)

        need_update_content = False
        if response.status_code == 200:
            need_update_content = True
            soup = BeautifulSoup(response.text, 'html.parser')
            title = soup.title.text if soup.title else ''
            # Extract content
            for script in soup(["script", "style"]):
                script.extract()
            content = soup.get_text(" ", strip=True)

            return need_update_content, title, content
        return need_update_content, '', ''

    def add_crawl_site_task(self, site_id):
        msg_content = {
            'site_id': site_id,
        }
        self.mq.lpush('web_crawl_task', json.dumps(msg_content))

    def search(self, project_uuid, keyword, start=0, size=20):
        site_index_name = gen_site_index_name(project_uuid)
        return self.site_index.search(site_index_name, keyword, start, size)
