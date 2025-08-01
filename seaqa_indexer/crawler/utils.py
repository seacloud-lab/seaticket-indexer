from urllib.parse import urljoin, urlparse
import os
import re
import hashlib
import json
import urllib.parse
from seaqa_indexer.config import CRAWL_OUTPUT_DIR


def url_to_filename(url):
    """
    Convert URL to valid filename
    """
    # Remove protocol prefix
    url = re.sub(r'^https?://', '', url)

    # Replace invalid characters
    filename = urllib.parse.quote_plus(url)
    # filename = unquote(urllib.parse.quote_plus(url))

    # Ensure filename doesn't exceed maximum length limit (255 characters)
    if len(filename) > 240:
        # Keep beginning and end, use hash value in the middle
        hash_part = hashlib.md5(url.encode('utf-8')).hexdigest()[:16]
        filename = filename[:110] + '_' + hash_part + '_' + filename[-110:]

    # Add .json extension
    return filename + '.json'


def get_site_dir(site_id, project_uuid):
    base_dir = os.path.join(CRAWL_OUTPUT_DIR, project_uuid)
    site_dir = os.path.join(base_dir, str(site_id))
    return site_dir


def spider_parse_item(spider, response):
    """Parse page content and extract data"""
    url = response.url
    response_etag = response.headers.get('ETag')
    response_last_modified = response.headers.get('Last-Modified')

    try:
        if response_etag:
            response_etag = response_etag.decode('utf-8')
        if response_last_modified:
            response_last_modified = response_last_modified.decode('utf-8')
    except UnicodeDecodeError as e:
        spider.logger.debug(f"Failed to decode cache headers for {url}: {e}")
        response_etag = None
        response_last_modified = None

    spider.processed_urls.add(url)
    cache_info = spider.existing_site_info.get(url, {})
    content_changed = True
    if cache_info:
        if response_etag and cache_info.get('etag') == response_etag:
            content_changed = False
        elif response_last_modified and cache_info.get('last_modified') == response_last_modified:
            content_changed = False

    if not content_changed:
        spider.logger.info(f"No changes detected for {url}")
        return

    spider.logger.info(f"Processing page: {url}")

    # Extract page title
    title = response.css('title::text').get()

    # Extract page body content
    selector_list = response.xpath('//article//text()')
    body_content = ''
    for selector in selector_list:
        selector_text = selector.get().strip()
        body_content += selector_text

    return {
        'url': url,
        'title': title,
        'content': body_content,
        'etag': response_etag,
        'last_modified': response_last_modified
    }


def load_existing_site_info(site_id, project_uuid, existing_site_info):
    """Load the ETag and Last-Modified information of an existing file"""
    site_dir = get_site_dir(site_id, project_uuid)
    if not os.path.exists(site_dir):
        return

    for filename in os.listdir(site_dir):
        if not filename.endswith('.json'):
            continue
        filepath = os.path.join(site_dir, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            url = data.get('url')
            existing_site_info[url] = {
                'etag': data.get('etag'),
                'last_modified': data.get('last_modified'),
            }
