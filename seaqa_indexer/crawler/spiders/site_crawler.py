import requests
import json
import os

from urllib.parse import urlparse
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from seaqa_indexer.crawler.utils import get_site_dir


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
        self.site_url = site_url
        self.site_id = site_id

        self.existing_cache_info = {}
        self._load_existing_cache_info()
        self.logger.info(f"Spider loaded {len(self.existing_cache_info)} cached URLs for {site_url}")
    
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

    def parse_item(self, response):
        """Parse the page content and extract the data"""
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

        self.logger.debug(f"Processing page: {url}")
        
        # Redownload HTML and process
        title = response.css('title::text').get()
        
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
    
    def _check_with_conditional_get(self, url, cache_info):
        """Use a condition GET request to check if the content has changed."""
        headers = {}
        if cache_info['etag']:
            headers['If-None-Match'] = cache_info['etag']
        if cache_info['last_modified']:
            headers['If-Modified-Since'] = cache_info['last_modified']
        
        try:
            response = requests.get(url, headers=headers, timeout=15)
            # 304 Not Modified
            return response.status_code != 304
        except Exception as e:
            self.logger.warning(f"Conditional GET failed for {url}: {e}")
            return True  # When an error occurs, assume that the content has changed
