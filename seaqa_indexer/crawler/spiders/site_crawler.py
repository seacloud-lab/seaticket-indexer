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
            self.logger.warning(f"Page {response.url} no text content was found")

        # Return the extracted data
        yield {
            'url': response.url,
            'title': title,
            'content': clean_text,
            'timestamp': response.meta.get('timestamp', None),
            'source': 'normal'
        }
