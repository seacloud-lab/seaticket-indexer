import json
import os

from seaqa_indexer.crawler.utils import url_to_filename, get_site_dir


class BatchCrawlPipeline:
    def __init__(self):
        self.project_uuid = None
        self.site_id = None

    def open_spider(self, spider):
        self.project_uuid = spider.project_uuid
        self.site_id = spider.site_id

    def close_spider(self, spider):
        site_dir = get_site_dir(self.site_id, self.project_uuid)
        urls_to_delete = set(spider.existing_site_info.keys()) - spider.processed_urls

        for url in urls_to_delete:
            filename = url_to_filename(url)
            filepath = os.path.join(site_dir, filename)
            os.remove(filepath)
            spider.logger.info('delete filename: %s', url)

        spider.logger.info(f"Update completed: processed {len(spider.processed_urls)} URLs, deleted {len(urls_to_delete)} removed pages")

    def process_item(self, item, spider):
        self.save_content(item)
        return item

    def save_content(self, item):
        url = item['url']
        title = item['title']
        content = item['content']
        etag = item.get('etag') if item else None
        last_modified = item.get('last_modified') if item else None
        filename = url_to_filename(url)
        site_dir = get_site_dir(self.site_id, self.project_uuid)
        filepath = os.path.join(site_dir, filename)

        new_content = {
            "url": url,
            "title": title,
            "content": content,
            "etag": etag,
            "last_modified": last_modified,
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(new_content, f, ensure_ascii=False, indent=2)
