import json
import os
from scrapy.spiders import SitemapSpider
from seaqa_indexer.crawler.utils import get_site_dir

def parse_item(self, response):
    """Parse page content and extract data"""
    url = response.url

    if 'not_modified' in response.flags:
        self.logger.debug(f"No changes detected for {url}, yielding cached item")
        cache_info = self.existing_cache_info[url]

        try:
            with open(cache_info['filepath'], 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to read cached file {cache_info['filepath']} for url {url}: {e}")
            return

        yield {
            'url': url,
            'title': existing_data.get('title', ''),
            'content': existing_data.get('content', ''),
            'not_modified': True,
            'etag': cache_info.get('etag'),
            'last_modified': cache_info.get('last_modified')
        }
        return

    self.logger.info(f"Processing page: {url}")

    # Extract page title
    title = response.css('title::text').get()

    # Extract page body content
    selector_list = response.xpath('//article//text()')
    body_content = ''
    for selector in selector_list:
        selector_text = selector.get().strip()
        body_content += selector_text

    # Parse HTML, extract plain text/metadata
    response_etag = response.headers.get('ETag')
    response_last_modified = response.headers.get('Last-Modified')
    
    try:
        if response_etag:
            response_etag = response_etag.decode('utf-8')
        if response_last_modified:
            response_last_modified = response_last_modified.decode('utf-8')
    except UnicodeDecodeError as e:
        self.logger.warning(f"Failed to decode cache headers for {url}: {e}")
        response_etag = None
        response_last_modified = None

    yield {
        'url': url,
        'title': title,
        'content': body_content,
        'not_modified': False,
        'etag': response_etag,
        'last_modified': response_last_modified
    }

def create_sitemap_spider(name, sitemap_url, site_url, project_uuid, site_id, sitemap_follow=None, sitemap_rules=None):
    """Dynamically create a SitemapSpider class"""
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
        'parse_item': parse_item,
        'site_id': site_id,
        'existing_cache_info': {},
        '_load_existing_cache_info': _load_existing_cache_info
    }

    # Create class
    spider_class = type(f'{name}Spider', (SitemapSpider,), attrs)

    # Initialize cache info
    spider_class._load_existing_cache_info = classmethod(_load_existing_cache_info)
    spider_class.__init__ = lambda self, *args, **kwargs: (
        SitemapSpider.__init__(self, *args, **kwargs),
        setattr(self, 'existing_cache_info', {}),
        self._load_existing_cache_info(),
        self.logger.info(f"Spider loaded {len(self.existing_cache_info)} cached URLs for {self.site_url}")
    )

    return spider_class

def _load_existing_cache_info(self):
    """Load the ETag and Last-Modified information of an existing file"""
    site_dir = get_site_dir(self.site_id, self.project_uuid)
    if not os.path.exists(site_dir):
        return
        
    for filename in os.listdir(site_dir):
        if filename.endswith('.json'):
            filepath = os.path.join(site_dir, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    url = data.get('url')
                    if url:
                        self.existing_cache_info[url] = {
                            'etag': data.get('etag'),
                            'last_modified': data.get('last_modified'),
                            'filepath': filepath
                        }
            except Exception as e:
                self.logger.warning(f"Error loading cache info from {filename}: {e}")
