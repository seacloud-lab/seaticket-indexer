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
    selector_list = response.xpath('//article//text()')
    body_content = ''
    for selector in selector_list:
        selector_text = selector.get().strip()
        body_content += selector_text

    # Return extracted data
    yield {
        'url': response.url,
        'title': title,
        'content': body_content
    }


def create_sitemap_spider(name, sitemap_url, site_url, project_uuid, site_id, sitemap_follow=None, sitemap_rules=None):
    """
    Dynamically create a SitemapSpider class

    Parameters:
        name (str): Spider name
        sitemap_url (str): Sitemap URL
        site_url (str): Site URL
        project_uuid (str): project_uuid
        site_id (str): site_id
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
        'project_uuid': project_uuid,
        'site_url': site_url,
        'parse': parse_sitemap_item,
        'parse_item': parse_item,
        'site_id': site_id,
    }

    # Create class
    spider_class = type(f'{name}Spider', (SitemapSpider,), attrs)

    return spider_class
