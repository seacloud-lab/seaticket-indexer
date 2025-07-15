import json
import os

from datetime import datetime
from seaqa_indexer.crawler.utils import url_to_filename, get_site_dir


class BatchCrawlPipeline:
    def __init__(self):
        self.project_uuid = None
        self.site_url = None
        self.site_id = None
        self.existing_files = set()
        self.processed_urls = set()

    def open_spider(self, spider):
        self.project_uuid = spider.project_uuid
        self.site_url = spider.site_url
        self.site_id = spider.site_id
        
        site_dir = get_site_dir(self.site_url, self.project_uuid)
        if os.path.exists(site_dir):
            self.existing_files = set(f for f in os.listdir(site_dir) if f.endswith('.json'))

    def close_spider(self, spider):
        site_dir = get_site_dir(self.site_url, self.project_uuid)
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
        if 'url' in item and 'title' in item and 'content' in item:
            try:
                not_modified = item.get('not_modified', False)
                
                if not_modified:
                    self._handle_not_modified(item, spider)
                else:
                    self.save_content_with_update_logic(
                        item['url'],
                        item['title'], 
                        item['content'],
                        spider,
                        item  # Pass the entire item
                    )
                
                self.processed_urls.add(item['url'])
                spider.logger.debug(f"Processed URL {item['url']}")
            except Exception as e:
                spider.logger.error(f"Error processing item: {str(e)}")
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
            "crawled_at": datetime.now().isoformat()
        }
        
        # Check if file exists
        if os.path.exists(filepath):
            # File exists - update it
            with open(filepath, 'r', encoding='utf-8') as f:
                existing_content = json.load(f)
            
            new_content['updated_at'] = datetime.now().isoformat()
            new_content['created_at'] = existing_content.get('created_at', existing_content.get('crawled_at'))
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(new_content, f, ensure_ascii=False, indent=2)
            
            spider.logger.info(f"Updated existing file: {filename}")
        else:
            # File doesn't exist - create
            new_content['created_at'] = datetime.now().isoformat()
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(new_content, f, ensure_ascii=False, indent=2)
            
            spider.logger.info(f"Created new file: {filename}")

    def _handle_not_modified(self, item, spider):
        url = item['url']
        filename = url_to_filename(url)
        site_dir = get_site_dir(self.site_url, self.project_uuid)
        filepath = os.path.join(site_dir, filename)
        
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                existing_content = json.load(f)
            
            # Only update the cache header and check the time.
            existing_content['etag'] = item.get('etag')
            existing_content['last_modified'] = item.get('last_modified')
            existing_content['last_checked_at'] = datetime.now().isoformat()
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(existing_content, f, ensure_ascii=False, indent=2)
            
            spider.logger.debug(f"Skipped unmodified URL: {url}")
