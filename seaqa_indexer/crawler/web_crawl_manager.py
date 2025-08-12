import os
import logging
import requests
import json

import datetime
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
from sqlalchemy.sql import text

from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet.threads import deferToThread

from seaqa_indexer.crawler.spiders.site_crawler import SiteCrawlerSpider
from seaqa_indexer.crawler.spiders.sitemap_crawler import SitemapCrawlerSpider

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.config import CRAWL_OUTPUT_DIR
from seaqa_indexer.utils.constants import CrawlStatus
from seaqa_indexer.mq import get_mq
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.crawler.utils import get_site_dir


logger = logging.getLogger(__name__)


class WebCrawlManager:
    """Crawler manager, responsible for starting and managing crawler processes"""

    def __init__(self):
        self.db_session = init_db_session_class()
        self.output_dir = CRAWL_OUTPUT_DIR
        self.active_crawlers = {}
        self.settings = get_project_settings()
        self.runner = CrawlerRunner(self.settings)

        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def get_project_connection_by_id(self, pc_id, conn_type='site'):
        with self.db_session() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid
            FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.type=:conn_type AND pc.id=:pc_id"""
            connection = session.execute(text(sql), {'pc_id': pc_id, 'conn_type': conn_type}).fetchone()
            return connection

    def update_site_status(self, connection_id, status):
        with self.db_session() as session:
            cmd = """UPDATE project_connection SET status = :status WHERE id = :connection_id"""
            session.execute(text(cmd), {'connection_id': connection_id, 'status': status})
            session.commit()

    def update_site_status_and_crawl_time(self, site_id, status):
        updated_at = datetime.datetime.now(datetime.UTC)
        with self.db_session() as session:
            cmd = """UPDATE project_connection SET status = :status, updated_at=:updated_at WHERE id = :site_id"""
            session.execute(text(cmd), {'site_id': site_id, 'status': status, 'updated_at': updated_at})
            session.commit()

    def get_project_by_id(self, project_id):
        with self.db_session() as session:
            sql = "SELECT * FROM projects WHERE id=:project_id"
            project = session.execute(text(sql), {'project_id': project_id}).fetchone()
            return project

    def update_site_config(self, site_id, site_config):
        with self.db_session() as session:
            cmd = """UPDATE project_connection SET config = :site_config WHERE id = :site_id"""
            session.execute(text(cmd), {'site_id': site_id, 'site_config': site_config})
            session.commit()

    def get_project_site_by_page(self, start, limit):
        with self.db_session() as session:
            sql = """SELECT pc.id as site_id, pc.config, pc.project_id, pc.status, p.uuid
             FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.type='site' LIMIT :start, :limit"""
            sites = session.execute(text(sql), {'start': start, 'limit': limit}).fetchall()
            return sites

    def crawl_site(self, site_id):
        """Start crawler process"""
        site = self.get_project_connection_by_id(site_id)
        if not site:
            logger.warning('site: %s not found', site_id)
            return
        if site.status == CrawlStatus.CRAWLING:
            logger.warning('site: %s is crawling', site_id)
            return
        self.update_site_status(site_id, CrawlStatus.CRAWLING)

        logger.info(f"Start crawling site {site_id}")

        site_id = site.id
        site_config = json.loads(site.config)
        site_url = site_config.get('url')
        project_uuid = str(site.project_uuid)
        site_dir = get_site_dir(site_id, project_uuid)
        os.makedirs(site_dir, exist_ok=True)

        # Use deferToThread to push crawler task to thread pool
        d = deferToThread(
            self.prepare_site_crawler,
            site_id,
            site_url,
            site_config,
            project_uuid
        )

        d.addCallback(self.start_crawl)
        d.addErrback(self.deal_prepare_crawler_error, site_id, site_url)

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
        site_url = crawler_params['site_url']
        sitemap_url = crawler_params['sitemap_url']
        project_uuid = crawler_params['project_uuid']

        # Create crawler instance
        if sitemap_url:
            d = self.runner.crawl(
                SitemapCrawlerSpider,
                site_url,
                project_uuid,
                site_id,
                sitemap_url
            )
        else:
            d = self.runner.crawl(
                SiteCrawlerSpider,
                site_url=site_url,
                project_uuid=project_uuid,
                site_id=site_id,
            )

        d.addCallback(lambda _: self.finish_crawl_site(site_id, site_url, CrawlStatus.COMPLETED))
        d.addErrback(lambda failure: self.finish_crawl_site(site_id, site_url, CrawlStatus.FAILED, str(failure)))
        return d

    def prepare_site_crawler(self, site_id, site_url, site_config, project_uuid):
        """Prepare crawler parameters, execute in thread pool"""
        sitemap_url = site_config.get('sitemap_url')

        if not sitemap_url:
            sitemap_url = self.get_sitemap_url(site_url)
            if sitemap_url:
                site_config['sitemap_url'] = sitemap_url
                self.update_site_config(site_id, json.dumps(site_config))

        return {
            'site_id': site_id,
            'site_url': site_url,
            'sitemap_url': sitemap_url,
            'project_uuid': project_uuid
        }

    def deal_prepare_crawler_error(self, failure, site_id, site_url):
        self.finish_crawl_site(site_id, site_url, CrawlStatus.FAILED, str(failure))
        return failure

    def finish_crawl_site(self, site_id, site_url, status, error_msg=None):
        if status == CrawlStatus.FAILED:
            logger.warning('crawl site failed, id: %s, url: %s, error: %s', site_id, site_url, error_msg)
            self.update_site_status(site_id, CrawlStatus.FAILED)
            return

        self.update_site_status_and_crawl_time(site_id, CrawlStatus.COMPLETED)
        self.add_site_index_task(site_id)
        logger.info('finish crawl site: %s', site_id)

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
                site_config = json.loads(site.config)
                site_url = site_config.get('url')
                site_id = site.site_id
                try:
                    self.crawl_site(site_id)
                except Exception as e:
                    logger.warning('update crawl site failed, site_id: %s, error: %s', site_id, e)
                    self.finish_crawl_site(site_id, site_url, CrawlStatus.FAILED, e)

    def add_crawl_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('web_crawl_task', json.dumps(msg_content))

    def add_site_index_task(self, site_id):
        msg_content = {
            'connection_id': site_id,
        }
        self.mq.lpush('index_task', json.dumps(msg_content))
