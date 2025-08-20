import os
import logging
import requests
import json

import datetime
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
from sqlalchemy.sql import text

from seaqa_indexer.utils.constants import CrawlStatus
from seaqa_indexer.mq import get_mq
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet.threads import deferToThread

from seaqa_indexer.crawler.spiders.site_crawler import SiteCrawlerSpider
from seaqa_indexer.crawler.spiders.sitemap_crawler import SitemapCrawlerSpider
from seaqa_indexer.crawler.utils import get_site_dir


logger = logging.getLogger(__name__)


class WebCrawl:
    def __init__(self, db_session):
        self.db_session = db_session

        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

        self.settings = get_project_settings()
        self.runner = CrawlerRunner(self.settings)

    def get_site_by_id(self, site_id):
        with self.db_session() as session:
            sql = """SELECT pc.id as site_id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid
             FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.type='site' AND pc.id=:site_id"""
            site = session.execute(text(sql), {'site_id': site_id}).fetchone()
            return site

    def update_connection_status(self, site_id, status, is_update_time=False):
        with self.db_session() as session:
            cmd = """UPDATE project_connection SET status = :status"""
            params = {'site_id': site_id, 'status': status}
            if is_update_time:
                updated_at = datetime.datetime.now(datetime.UTC)
                cmd += ", updated_at=:updated_at"
                params['updated_at'] = updated_at

            cmd += " WHERE id = :site_id"

            session.execute(text(cmd), params)
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

    def failed_crawl_site(self, error_msg, params):
        site_id, site_url, connection_status = params
        connection_status['last_sync_status'] = CrawlStatus.FAILED
        self.update_connection_status(site_id, json.dumps(connection_status))
        logger.warning('crawl site failed, id: %s, url: %s, error: %s', site_id, site_url, error_msg)

    def success_call_back(self, msg, params):
        site_id = params[0]
        self.add_site_index_task(site_id)

    def add_site_index_task(self, site_id):
        msg_content = {
            'connection_id': site_id,
        }
        self.mq.lpush('index_task', json.dumps(msg_content))

    def crawl_site(self, site_id):
        """Start crawler process"""
        logger.info(f"Start crawling site {site_id}")
        site = self.get_site_by_id(site_id)
        if not site:
            logger.warning('site: %s not found', site_id)
            return
        site_status = json.loads(site.status)
        last_sync_status = site_status.get('site_status')
        if last_sync_status == CrawlStatus.CRAWLING:
            logger.warning('site: %s is crawling', site_id)
            return

        site_id = site.site_id
        connection_status = json.loads(site.status)
        connection_status['last_sync_status'] = CrawlStatus.CRAWLING
        self.update_connection_status(site_id, json.dumps(connection_status))

        site_config = json.loads(site.config)
        site_url = site_config.get('url')
        sitemap_url = site_config.get('sitemap_url')
        project_uuid = str(site.project_uuid)

        if not sitemap_url:
            sitemap_url = self.get_sitemap_url(site_url)
            if sitemap_url:
                site_config['sitemap_url'] = sitemap_url
                self.update_site_config(site_id, json.dumps(site_config))

        try:
            self.crawl(site_id, site_url, sitemap_url, project_uuid, connection_status, self.success_call_back, self.failed_crawl_site)
        except Exception as e:
            logger.exception('failed to crawl site: %s, error: %s', site_id, e)
            site_status['last_sync_status'] = CrawlStatus.FAILED
            self.update_connection_status(site_id, json.dumps(site_status))

    def crawl(self, site_id, site_url, sitemap_url, project_uuid, connection_status, success_call_back,
              fail_call_back):
        site_dir = get_site_dir(site_id, project_uuid)
        os.makedirs(site_dir, exist_ok=True)
        # Use deferToThread to push crawler task to thread pool
        d = deferToThread(
            self.prepare_crawler,
            site_id,
            site_url,
            sitemap_url,
            project_uuid,
            connection_status,
            success_call_back,
            fail_call_back,
        )

        d.addCallback(self.start_crawl)

    def prepare_crawler(self, site_id, site_url, sitemap_url, project_uuid, connection_status, success_call_back,
                        fail_call_back):
        """Prepare crawler parameters, execute in thread pool"""
        return {
            'site_id': site_id,
            'site_url': site_url,
            'sitemap_url': sitemap_url,
            'project_uuid': project_uuid,
            'connection_status': connection_status,
            'success_call_back': success_call_back,
            'fail_call_back': fail_call_back,
        }

    def start_crawl(self, crawler_params):
        """Callback function for starting crawler"""
        site_id = crawler_params['site_id']
        site_url = crawler_params['site_url']
        sitemap_url = crawler_params['sitemap_url']
        project_uuid = crawler_params['project_uuid']
        connection_status = crawler_params['connection_status']
        success_call_back = crawler_params['success_call_back']
        fail_call_back = crawler_params['fail_call_back']

        # Create crawler instance
        if sitemap_url:
            d = self.runner.crawl(
                SitemapCrawlerSpider,
                site_url,
                project_uuid,
                site_id,
                sitemap_url,
                connection_status,
                self.update_connection_status
            )
        else:
            d = self.runner.crawl(
                SiteCrawlerSpider,
                site_url=site_url,
                project_uuid=project_uuid,
                site_id=site_id,
                connection_status=connection_status,
                update_connection_status=self.update_connection_status,
            )

        d.addCallback(success_call_back, (site_id,))
        d.addErrback(fail_call_back, (site_id, site_url, connection_status))
        return d
