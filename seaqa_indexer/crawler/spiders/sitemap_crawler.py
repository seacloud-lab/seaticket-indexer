from scrapy.spiders import SitemapSpider
from seaqa_indexer.crawler.utils import spider_parse_item, load_existing_site_info


class SitemapCrawlerSpider(SitemapSpider):
    name = 'sitemap_crawler'
    sitemap_rules = [('.*', 'parse_item')]
    sitemap_follow = ['.xml']
    sitemap_urls = []

    def __init__(self, site_url, project_uuid, site_id, sitemap_url, connection_status, update_connection_status, seadb_api, sync_logger, *args, **kwargs):
        super(SitemapCrawlerSpider, self).__init__(*args, **kwargs)
        self.start_urls = [site_url]
        self.project_uuid = project_uuid
        self.site_id = site_id
        self.connection_status = connection_status
        self.update_connection_status = update_connection_status
        self.crawled_count = 0
        self.sitemap_urls = [sitemap_url]
        self.parse_item = self.parse_item
        self.processed_urls = set()

        self.existing_site_info = {}
        self.seadb_api = seadb_api
        self.sync_logger = sync_logger
        self._load_existing_site_info()
        self.logger.info(f"Spider loaded {len(self.existing_site_info)} cached URLs for {site_url}")
        self.sync_logger.log(f"Spider loaded {len(self.existing_site_info)} cached URLs for {site_url}")

    def _load_existing_site_info(self):
        """Load the ETag and Last-Modified information of an existing file"""
        load_existing_site_info(self.site_id, self.project_uuid, self.existing_site_info, self.seadb_api)

    def parse_item(self, response):
        """Parse the page content and extract the data"""
        item = spider_parse_item(self, response)
        if item:
            yield item

