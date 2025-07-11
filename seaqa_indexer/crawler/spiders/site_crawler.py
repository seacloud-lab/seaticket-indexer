import re
import os

from urllib.parse import urlparse
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class SiteCrawlerSpider(CrawlSpider):
    name = 'site_crawler'

    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=True),
    )

    def __init__(self, site_url, project_uuid, *args, **kwargs):
        super(SiteCrawlerSpider, self).__init__(*args, **kwargs)
        # Set Starting URL
        self.start_urls = [site_url]
        # Extract the domain name from the URL.
        parsed_uri = urlparse(site_url)
        domain = '{uri.netloc}'.format(uri=parsed_uri)
        self.allowed_domains = [domain]
        self.project_uuid = project_uuid
        self.site_url = site_url

    def parse_item(self, response):
        """
            Parse the page content and extract the data
        """
        self.logger.debug(f"Processing the page: {response.url}")

        # Extract page title
        title = response.css('title::text').get()

        # Extract the main content of the page (
        selector_list = response.xpath('//article//text()')
        body_content = ''
        for selector in selector_list:
            selector_text = selector.get().strip()
            body_content += selector_text

        # Return the extracted data
        yield {
            'url': response.url,
            'title': title,
            'content': body_content
        }
