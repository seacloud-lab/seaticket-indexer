import re
import hashlib
import urllib.parse
from email.utils import parsedate_to_datetime
from seaqa_indexer.crawler.extractor import extract_html
from seaqa_indexer.utils.constants import WEB_CRAWL_TABLE


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
            response_last_modified = parsedate_to_datetime(response_last_modified).isoformat()
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
    body_content = extract_html(url, response.text)
    return {
        'url': url,
        'title': title,
        'content': body_content,
        'etag': response_etag,
        'last_modified': response_last_modified
    }


def load_existing_site_info(site_id, project_uuid, existing_site_info, seadb_api=None, limit=1000):
    """Load the ETag and Last-Modified information of an existing file"""
    if seadb_api is None:
        return
    offset = 0
    while True:
        sql = f"""SELECT `{WEB_CRAWL_TABLE.columns.url.name}`, `{WEB_CRAWL_TABLE.columns.etag.name}`, `{WEB_CRAWL_TABLE.columns.last_modified.name}`
        FROM `{site_id}`
        LIMIT {limit} OFFSET {offset}"""
        result = seadb_api.query_rows(project_uuid, sql)
        if not result.get('results', []):
            break
        for row in result.get('results', []):
            existing_site_info[row.get('url')] = {
                'etag': row.get('etag'),
                'last_modified': row.get('last_modified'),
            }
        offset += limit
