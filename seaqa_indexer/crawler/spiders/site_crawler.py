from urllib.parse import urlparse
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from seaqa_indexer.crawler.utils import spider_parse_item, load_existing_site_info


class SiteCrawlerSpider(CrawlSpider):
    name = 'site_crawler'

    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=True),
    )

    def __init__(self, site_url, project_uuid, site_id, *args, **kwargs):
        super(SiteCrawlerSpider, self).__init__(*args, **kwargs)
        self.start_urls = [site_url]
        parsed_uri = urlparse(site_url)
        domain = '{uri.netloc}'.format(uri=parsed_uri)
        self.allowed_domains = [domain]
        self.project_uuid = project_uuid
        self.site_id = site_id
        self.processed_urls = set()

        self.existing_site_info = {}
        self._load_existing_site_info()
        self.logger.info(f"Spider loaded {len(self.existing_site_info)} cached URLs for {site_url}")

    def _load_existing_site_info(self):
        """Load the ETag and Last-Modified information of an existing file"""
        load_existing_site_info(self.site_id, self.project_uuid, self.existing_site_info)

    def parse_item(self, response):
        """Parse the page content and extract the data"""
        item = spider_parse_item(self, response)
        if item:
            yield item
