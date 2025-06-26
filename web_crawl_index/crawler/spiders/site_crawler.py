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
    
    def __init__(self, url=None, output_dir='crawled_content', *args, **kwargs):
        super(SiteCrawlerSpider, self).__init__(*args, **kwargs)
        if url:
            # Set Starting URL
            self.start_urls = [url]
            # Extract the domain name from the URL.
            parsed_uri = urlparse(url)
            domain = '{uri.netloc}'.format(uri=parsed_uri)
            self.allowed_domains = [domain]

        # Set output directory
        self.output_dir = output_dir

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            self.logger.info(f"Create output directory: {os.path.abspath(self.output_dir)}")
        else:
            self.logger.info(f"Use the existing output directory: {os.path.abspath(self.output_dir)}")
    
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
            self.logger.warning(f"Page {response.url} no text content was found.")
        
        if not clean_text:
            self.logger.warning(f"Page {response.url} no text content was found, skip save.")
        
        # Return the extracted data
        yield {
            'url': response.url,
            'title': title,
            'content': clean_text,
            'timestamp': response.meta.get('timestamp', None),
            'source': 'normal'
        }
