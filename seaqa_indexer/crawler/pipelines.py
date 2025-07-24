import json
import os

from datetime import datetime
from seaqa_indexer.crawler.utils import url_to_filename, get_site_dir


class BatchCrawlPipeline:
    def __init__(self):
        self.project_uuid = None
        self.site_id = None

    def open_spider(self, spider):
        self.project_uuid = spider.project_uuid
        self.site_id = spider.site_id

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
        filename = url_to_filename(url)
        site_dir = get_site_dir(self.site_id, self.project_uuid)
        os.makedirs(site_dir, exist_ok=True)

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

