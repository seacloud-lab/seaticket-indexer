import json
import os

from datetime import datetime
from seaqa_indexer.crawler.utils import url_to_filename, get_site_dir


class BatchCrawlPipeline:
    def __init__(self):
        self.project_uuid = None
        self.site_id = None
        self.existing_files = set()
        self.processed_urls = set()

    def open_spider(self, spider):
        self.project_uuid = spider.project_uuid
        self.site_id = spider.site_id

        site_dir = get_site_dir(self.site_id, self.project_uuid)
        if os.path.exists(site_dir):
            self.existing_files = set(f for f in os.listdir(site_dir) if f.endswith('.json'))

    def close_spider(self, spider):
        site_dir = get_site_dir(self.site_id, self.project_uuid)
        processed_files = set(url_to_filename(url) for url in self.processed_urls)
        files_to_delete = self.existing_files - processed_files

        for filename in files_to_delete:
            filepath = os.path.join(site_dir, filename)
            try:
                os.remove(filepath)
                spider.logger.info(f"Deleted removed page file: {filename}")
            except Exception as e:
                spider.logger.error(f"Error deleting file {filename}: {e}")

        spider.logger.info(f"Update completed: processed {len(self.processed_urls)} URLs, deleted {len(files_to_delete)} removed pages")

    def process_item(self, item, spider):
        try:
            url = item['url']
            # All items that reach here, whether modified or not, are considered "processed"
            self.processed_urls.add(url)

            not_modified = item.get('not_modified', False)
            if not_modified:
                spider.logger.debug(f"URL not modified, skipping save: {url}")
                return item

            # If the page has been modified, save it.
            self.save_content_with_update_logic(
                url,
                item['title'],
                item['content'],
                spider,
                item,
            )
            spider.logger.debug(f"Processed and saved URL {url}")
        except Exception as e:
            spider.logger.error(f"Error processing item for url {item.get('url', 'N/A')}: {str(e)}")

        return item

    def save_content_with_update_logic(self, url, title, content, spider, item=None):
        """Save content and implement add/update/delete logic"""
        filename = url_to_filename(url)
        site_dir = get_site_dir(self.site_id, self.project_uuid)
        os.makedirs(site_dir, exist_ok=True)
        filepath = os.path.join(site_dir, filename)

        # Prepare new content data - get ETag/Last-Modified directly from item
        new_content = {
            "url": url,
            "title": title,
            "content": content,
            "etag": item.get('etag') if item else None,
            "last_modified": item.get('last_modified') if item else None,
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(new_content, f, ensure_ascii=False, indent=2)
        spider.logger.info(f"Process” file complete: {filename}")
