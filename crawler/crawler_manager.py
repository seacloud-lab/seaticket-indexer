# Install the correct reactor before importing any twisted modules
from scrapy.utils.reactor import install_reactor
# Use AsyncioSelectorReactor instead of SelectReactor
install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')

import os
import logging
import requests

from datetime import datetime
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

# Import Scrapy runner
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
# Import Twisted related modules
from twisted.internet import reactor
from twisted.internet.threads import deferToThread
from twisted.python.threadpool import ThreadPool

from models import db, Website, Project
# Import spider class
from crawler.spiders.site_crawler import SiteCrawlerSpider


logger = logging.getLogger(__name__)

class CrawlerManager:
    """Crawler manager, responsible for starting and managing crawler processes"""

    def __init__(self, app, output_dir):
        self.app = app
        self.output_dir = output_dir
        self.active_crawlers = {}
        # Get Scrapy project settings
        self.settings = get_project_settings()
        # Create Scrapy crawler runner
        self.runner = CrawlerRunner(self.settings)
        
        # Create a thread pool with maximum thread count
        self.thread_pool = ThreadPool(minthreads=1, maxthreads=5)
        # Start thread pool
        self.thread_pool.start()
        # Set reactor to use our thread pool
        reactor.threadpool = self.thread_pool
        
        # Flag to indicate if reactor is running
        self.reactor_running = False

    def start_crawler(self, website_id):
        """Start crawler process"""
        # Get website information
        website = Website.query.get(website_id)
        if not website:
            logger.error(f"Website ID does not exist: {website_id}")
            return False

        # Check if a crawler is already running
        if website_id in self.active_crawlers:
            logger.warning(f"Website {website.url} already has a crawler running")
            return False

        # Update website status
        website.status = 'crawling'
        db.session.commit()

        # Create project-specific output directory
        project = Project.query.get(website.project_id)
        project_output_dir = os.path.join(self.output_dir, f"project_{project.name}")
        os.makedirs(project_output_dir, exist_ok=True)

        # Use deferToThread to push crawler task to thread pool
        d = deferToThread(
            self._prepare_crawler,
            website_id, 
            website.url, 
            website.sitemap_url, 
            project_output_dir
        )
        
        # Add callback and error handling
        d.addCallback(self._start_crawler_callback)
        d.addErrback(self._crawler_error_callback, website_id, website.url)

        # Record active crawler
        self.active_crawlers[website_id] = {
            'deferred': d,
            'start_time': datetime.utcnow()
        }

        # If reactor is not running, start it
        if not self.reactor_running:
            # Run reactor in background
            def run_reactor():
                self.reactor_running = True
                reactor.run(installSignalHandlers=False)
            
            from threading import Thread
            reactor_thread = Thread(target=run_reactor)
            reactor_thread.daemon = True
            reactor_thread.start()

        logger.info(f"Started crawler for website {website.url}")
        return True

    def _try_get_sitemap_url(self, url):
        """Try to get website's sitemap URL"""
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
                                logger.info(f"Found sitemap from robots.txt: {sitemap_url}")
                                return sitemap_url
                    else:
                        # Verify if it's valid XML
                        try:
                            ET.fromstring(response.content)
                            logger.info(f"Found valid sitemap: {sitemap_url}")
                            return sitemap_url
                        except ET.ParseError:
                            logger.debug(f"Found sitemap is not valid XML: {sitemap_url}")
                            continue
            except Exception as e:
                logger.debug(f"Error trying to get sitemap {sitemap_url}: {str(e)}")

        logger.info(f"No sitemap found for website {url}")
        return None

    def _prepare_crawler(self, website_id, url, sitemap_url, output_dir):
        """Prepare crawler parameters, execute in thread pool"""
        # If sitemap_url is not provided, try to get it automatically
        if not sitemap_url:
            sitemap_url = self._try_get_sitemap_url(url)
            # If sitemap_url is successfully obtained, update database
            if sitemap_url:
                with self.app.app_context():
                    website = Website.query.get(website_id)
                    if website:
                        website.sitemap_url = sitemap_url
                        db.session.commit()
                        logger.info(f"Updated sitemap URL for website {url}: {sitemap_url}")
        
        # Return crawler parameters
        return {
            'website_id': website_id,
            'url': url,
            'sitemap_url': sitemap_url,
            'output_dir': output_dir
        }

    def _start_crawler_callback(self, crawler_params):
        """Callback function for starting crawler"""
        website_id = crawler_params['website_id']
        url = crawler_params['url']
        sitemap_url = crawler_params['sitemap_url']
        output_dir = crawler_params['output_dir']
        
        # Create crawler instance
        if sitemap_url:
            # Use sitemap spider
            from crawler.spiders.sitemap_crawler import create_sitemap_spider
            
            SitemapSpiderClass = create_sitemap_spider(
                name=f"sitemap_spider_{website_id}",  # Use unique name
                sitemap_url=sitemap_url,
                output_dir=output_dir  # Use provided output directory
            )
            d = self.runner.crawl(SitemapSpiderClass)
        else:
            # Use regular spider
            d = self.runner.crawl(
                SiteCrawlerSpider,
                url=url,
                output_dir=output_dir
            )
        
        # Add callback function to handle crawler completion
        d.addCallback(lambda _: self._crawler_done(website_id, url, True))
        d.addErrback(lambda failure: self._crawler_done(website_id, url, False, str(failure)))
        
        return d

    def _crawler_error_callback(self, failure, website_id, url):
        """Handle errors in crawler preparation stage"""
        logger.exception(f"Error in crawler preparation stage: {str(failure)}")
        self._crawler_done(website_id, url, False, str(failure))
        return failure

    def _crawler_done(self, website_id, url, success, error_msg=None):
        """Callback function after crawler completes"""
        try:
            # Use application context
            with self.app.app_context():
                # Update website status
                website = Website.query.get(website_id)
                if website:
                    if success:
                        website.status = 'completed'
                        website.last_crawled_at = datetime.utcnow()
                        logger.info(f"Website {url} crawling completed")
                    else:
                        website.status = 'failed'
                        logger.error(f"Website {url} crawling failed: {error_msg}")

                    db.session.commit()
        except Exception as e:
            logger.exception(f"Error updating website status: {str(e)}")
        finally:
            # Remove active crawler record
            if website_id in self.active_crawlers:
                del self.active_crawlers[website_id]

    def get_website_by_url_and_project(self, url, project_name):
        """Get website by URL and project name"""
        with self.app.app_context():
            project = Project.query.filter_by(name=project_name).first()
            if not project:
                return None

            return Website.query.filter_by(url=url, project_id=project.id).first()

    def start_crawler_by_url_and_project(self, url, project_name, sitemap_url=None):
        """Start crawler by URL and project name"""
        # Find project
        with self.app.app_context():
            project = Project.query.filter_by(name=project_name).first()
            if not project:
                logger.error(f"Project name does not exist: {project_name}")
                return False, f"Project name does not exist: {project_name}"

            # Find website
            website = Website.query.filter_by(url=url, project_id=project.id).first()

            # If website doesn't exist, create it
            if not website:
                website = Website(
                    url=url,
                    sitemap_url=sitemap_url,
                    project_id=project.id,
                    status='pending'
                )
                db.session.add(website)
                db.session.commit()
                logger.info(f"Created new website: {url}, project: {project_name}")

            # Start crawler
            success = self.start_crawler(website.id)
            if success:
                return True, f"Crawling task for website {url} has been started"
            else:
                return False, f"Failed to start crawling task for website {url}, a crawler may already be running"
