import json
import uuid

from seaqa_indexer.crawler.utils import url_to_filename
from seaqa_indexer.utils.constants import CrawlStatus
from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.crawler.storage import get_storage

from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.constants import CRAWL_OUTPUT_TABLE

class BatchCrawlPipeline:
    def __init__(self):
        self.project_uuid = None
        self.site_id = None
        self.storage = get_storage()
        self.seadb_api = SeaDBAPI('')

    def open_spider(self, spider):
        self.project_uuid = spider.project_uuid
        self.site_id = spider.site_id

    def close_spider(self, spider):
        urls_to_delete = set(spider.existing_site_info.keys()) - spider.processed_urls

        if self.storage is None:
            spider.logger.warning('Update failed: storage is not initialized')
            return
        for url in urls_to_delete:
            filename = url_to_filename(url)
            self.storage.delete_file(self.site_id, self.project_uuid, filename)
            spider.logger.info('delete filename: %s', url)

        spider.connection_status['total_records'] = len(spider.processed_urls)
        spider.connection_status['last_sync_status'] = CrawlStatus.COMPLETED
        spider.update_connection_status(self.site_id, json.dumps(spider.connection_status), is_update_time=True)

        spider.logger.info(f"Update completed: processed {len(spider.processed_urls)} URLs, deleted {len(urls_to_delete)} removed pages")

    def process_item(self, item, spider):
        self.save_content(item)
        spider.crawled_count += 1
        spider.connection_status['last_sync_count'] = spider.crawled_count
        if need_update_connection_status(spider.crawled_count):
            spider.update_connection_status(self.site_id, json.dumps(spider.connection_status))
        return item

    def save_content(self, item):
        url = item['url']
        title = item['title']
        content = item['content']
        etag = item.get('etag') if item else None
        last_modified = item.get('last_modified') if item else None
        filename = url_to_filename(url)
        if self.storage is None:
            return
        new_content = {
            "url": url,
            "title": title,
            "content": content,
            "etag": etag,
            "last_modified": last_modified,
        }
        self.storage.write_json(self.site_id, self.project_uuid, filename, new_content)
        uuid_str = str(uuid.UUID(self.project_uuid))
        insert_rows = [
            {
                CRAWL_OUTPUT_TABLE.columns.url.name: url,
                CRAWL_OUTPUT_TABLE.columns.title.name: title,
                # CRAWL_OUTPUT_TABLE.columns.content.name: content,
                CRAWL_OUTPUT_TABLE.columns.etag.name: etag,
                CRAWL_OUTPUT_TABLE.columns.last_modified.name: last_modified,
            }
        ]
        self.seadb_api.insert_rows(uuid_str, self.site_id, insert_rows)
