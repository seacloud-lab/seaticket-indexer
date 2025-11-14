import logging
import requests

from urllib.parse import urljoin
import xml.etree.ElementTree as ET
from sqlalchemy.sql import text

from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet.threads import deferToThread

from seaqa_indexer.crawler.spiders.site_crawler import SiteCrawlerSpider
from seaqa_indexer.crawler.spiders.sitemap_crawler import SitemapCrawlerSpider
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.crawler.storage import get_storage


logger = logging.getLogger(__name__)

class WebCrawl:
    def __init__(self, db_session_class, sync_logger):
        self.db_session_class = db_session_class
        self.sync_logger = sync_logger

        self.settings = get_project_settings()
        self.runner = CrawlerRunner(self.settings)
        self.seadb_api = SeaDBAPI()
        self.storage = get_storage()

    def update_connection_status(self, site_id, status, last_sync_time=None):
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET status = :status"""
            params = {'site_id': site_id, 'status': status}
            if last_sync_time:
                cmd += """, last_sync_time = :last_sync_time"""
                params['last_sync_time'] = last_sync_time
            cmd += " WHERE id = :site_id"
            session.execute(text(cmd), params)
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
                            self.sync_logger.log(f"Found sitemap is not valid XML: {sitemap_url}")
                            continue
            except Exception as e:
                logger.debug(f"trying to get {url} sitemap error: {str(e)}")
                self.sync_logger.log(f"Trying to get {url} sitemap error")

        logger.info(f"No sitemap found for site {url}")
        self.sync_logger.log(f"No sitemap found for site {url}")
        return None

    def crawl(self, site_id, site_url, sitemap_url, project_uuid, connection_status, success_call_back,
              fail_call_back):
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
                update_connection_status=self.update_connection_status,
                seadb_api=self.seadb_api,
                storage=self.storage,
                sync_logger=self.sync_logger
            )
        else:
            d = self.runner.crawl(
                SiteCrawlerSpider,
                site_url=site_url,
                project_uuid=project_uuid,
                site_id=site_id,
                connection_status=connection_status,
                update_connection_status=self.update_connection_status,
                seadb_api=self.seadb_api,
                storage=self.storage,
                sync_logger=self.sync_logger
            )

        d.addCallback(success_call_back, (site_id, project_uuid))
        d.addErrback(fail_call_back, (site_id, site_url, connection_status))
        return d
