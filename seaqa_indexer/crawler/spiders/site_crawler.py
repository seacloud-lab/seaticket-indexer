import re
from urllib.parse import urlparse, urlunparse
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from seaqa_indexer.crawler.utils import spider_parse_item, load_existing_site_info


class SiteCrawlerSpider(CrawlSpider):
    name = 'site_crawler'

    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=True),
    )

    def __init__(self, site_url, project_uuid, site_id, connection_status, update_connection_status, seadb_api, storage, sync_logger, *args, **kwargs):
        super(SiteCrawlerSpider, self).__init__(*args, **kwargs)
        self.start_urls = [site_url]
        parsed_uri = urlparse(site_url)
        self.base_url = self._normalize_base_url(parsed_uri)
        self.allowed_domains = [parsed_uri.netloc]
        self.project_uuid = project_uuid
        self.site_id = site_id
        self.connection_status = connection_status
        self.update_connection_status = update_connection_status
        self.crawled_count = 0
        self.processed_urls = set()

        self.existing_site_info = {}
        self.seadb_api = seadb_api
        self.storage = storage
        self.sync_logger = sync_logger
        self.rules = (
            Rule(LinkExtractor(allow=(re.escape(self.base_url),)), callback='parse_item', follow=True),
        )
        self._load_existing_site_info()
        self.logger.info(f"Spider loaded {len(self.existing_site_info)} cached URLs for {site_url}")
        self.sync_logger.log(f"Spider loaded {len(self.existing_site_info)} cached URLs for {site_url}")

    def _normalize_base_url(self, parsed_uri):
        path = parsed_uri.path or '/'
        if not path.endswith('/'):
            path += '/'
        return urlunparse((parsed_uri.scheme, parsed_uri.netloc, path, '', '', ''))

    def _load_existing_site_info(self):
        """Load the ETag and Last-Modified information of an existing file"""
        load_existing_site_info(self.site_id, self.project_uuid, self.existing_site_info, self.seadb_api)

    def parse_item(self, response):
        """Parse the page content and extract the data"""
        if not response.url.startswith(self.base_url):
            return
        item = spider_parse_item(self, response)
        if item:
            yield item
