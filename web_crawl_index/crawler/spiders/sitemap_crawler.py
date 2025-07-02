import re
import os

from scrapy.spiders import SitemapSpider


def parse_sitemap_item(self, response):
    """
    Process each URL obtained from sitemap
    """
    self.logger.info(f"Processing sitemap item: {response.url}")
    return self.parse_item(response)


def parse_item(self, response):
    """
    Parse page content and extract data
    """
    self.logger.debug(f"Processing page: {response.url}")

    # Extract page title
    title = response.css('title::text').get()

    # Extract page body content
    # Remove scripts, styles and other unnecessary elements
    body_content = response.xpath('//body').extract_first()
    if body_content:
        # Remove HTML tags
        clean_text = re.sub(r'<script.*?</script>', '', body_content, flags=re.DOTALL)
        clean_text = re.sub(r'<style.*?</style>', '', clean_text, flags=re.DOTALL)
        clean_text = re.sub(r'<[^>]+>', ' ', clean_text)
        # Remove excess whitespace
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    else:
        clean_text = ""

    if not clean_text:
        self.logger.warning(f"Page {response.url} has no valid content")

    # Return extracted data
    yield {
        'url': response.url,
        'title': title,
        'content': clean_text,
        'timestamp': response.meta.get('timestamp', None),
        'source': 'sitemap'
    }


def create_sitemap_spider(name, project_uuid, site_id, sitemap_url, site_index, sitemap_follow=None, sitemap_rules=None):
    """
    Dynamically create a SitemapSpider class

    Parameters:
        name (str): Spider name
        sitemap_url (str): Sitemap URL
        sitemap_follow (list): List of sitemap types to follow
        sitemap_rules (list): List of sitemap rules

    Returns:
        type: Dynamically created spider class
    """
    # Set default values
    if sitemap_follow is None:
        sitemap_follow = ['.xml']

    if sitemap_rules is None:
        sitemap_rules = [('.*', 'parse_item')]

    # Create class attributes dictionary
    attrs = {
        'name': name.lower(),
        'sitemap_urls': [sitemap_url],
        'sitemap_follow': sitemap_follow,
        'sitemap_rules': sitemap_rules,
        'parse': parse_sitemap_item,
        'parse_item': parse_item,
        'site_index': site_index,
        'site_id': site_id,
        'project_uuid': project_uuid,
    }

    # Create class
    spider_class = type(f'{name}Spider', (SitemapSpider,), attrs)

    return spider_class
