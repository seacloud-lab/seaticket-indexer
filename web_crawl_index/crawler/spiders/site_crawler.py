import re
import os

from urllib.parse import urlparse
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class SiteCrawlerSpider(CrawlSpider):
    name = "site_crawler"

    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=True),
    )

    def __init__(self, project_uuid, site_id, url, site_index, *args, **kwargs):
        super(SiteCrawlerSpider, self).__init__(*args, **kwargs)
        # Set Starting URL
        self.start_urls = [url]
        # Extract the domain name from the URL.
        parsed_uri = urlparse(url)
        domain = '{uri.netloc}'.format(uri=parsed_uri)
        self.allowed_domains = [domain]

        self.site_index = site_index
        self.site_id = site_id
        self.project_uuid = project_uuid

    def parse_item(self, response):
        """
            Parse the page content and extract the data
        """
        self.logger.info(f"Processing the page: {response.url}")

        # Extract page title
        title = response.css('title::text').get()

        # Extract the main content of the page (
        # Remove scripts, styles, and other unnecessary elements
        body_content = response.xpath('//body').extract_first()
        if body_content:
            # Remove HTML tags
            clean_text = re.sub(r'<script.*?</script>', '', body_content, flags=re.DOTALL)
            clean_text = re.sub(r'<style.*?</style>', '', clean_text, flags=re.DOTALL)
            clean_text = re.sub(r'<[^>]+>', ' ', clean_text)
            # Remove extra spaces
            clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        else:
            clean_text = ""

        if not clean_text:
            self.logger.warning(f"Page {response.url} no text content was found.")

        # Return the extracted data
        yield {
            'url': response.url,
            'title': title,
            'content': clean_text,
            'timestamp': response.meta.get('timestamp', None),
            'source': 'normal'
        }
