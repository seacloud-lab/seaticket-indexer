import json
import os
import re
import hashlib
import urllib.parse

from urllib.parse import urlparse, unquote
from datetime import datetime


class BatchCrawlPipeline:
    def __init__(self):
        self.output_dir = 'output'

    def open_spider(self, spider):
        # Get spider's output directory (if available)
        if hasattr(spider, 'output_dir'):
            self.output_dir = spider.output_dir

        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)

    def close_spider(self, spider):
        pass

    def process_item(self, item, spider):
        # If content exists, save to individual file
        if 'url' in item and 'title' in item and 'content' in item:
            try:
                self.save_content_to_file(
                    item['url'],
                    item['title'],
                    item['content'],
                    spider
                )
            except Exception as e:
                spider.logger.error(f"Error saving content to file: {str(e)}")

        return item

    def save_content_to_file(self, url, title, content, spider):
        """Save extracted content to JSON file"""
        # Extract domain from URL as intermediate directory
        parsed_uri = urlparse(url)
        domain = '{uri.netloc}'.format(uri=parsed_uri)

        # Convert URL to valid filename
        filename = self.url_to_filename(url)

        # Build site directory path
        site_dir = os.path.join(self.output_dir, domain)

        # Ensure site directory exists
        if not os.path.exists(site_dir):
            os.makedirs(site_dir)
            spider.logger.info(f"Created site directory: {os.path.abspath(site_dir)}")

        # Build complete file path
        filepath = os.path.join(site_dir, filename)

        # Get HTTP headers from response if available
        etag = None
        last_modified = None
        if hasattr(spider, 'crawler') and hasattr(spider.crawler, 'stats'):
            stats = spider.crawler.stats.get_stats()
            if 'etag' in stats:
                etag = stats['etag']
            if 'last-modified' in stats:
                last_modified = stats['last-modified']

        # Prepare JSON content
        json_content = {
            "url": url,
            "title": title,
            "content": content,
            "etag": etag,
            "last_modified": last_modified,
            "crawled_at": datetime.now().isoformat()
        }

        # Write to JSON file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(json_content, f, ensure_ascii=False, indent=2)

        spider.logger.info(f"Content saved to JSON file: {os.path.abspath(filepath)}")

    def url_to_filename(self, url):
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
