import json

from seaqa_indexer.crawler.utils import url_to_filename
from seaqa_indexer.utils.constants import CrawlStatus, WEB_CRAWL_TABLE
from seaqa_indexer.utils import need_update_connection_status

class BatchCrawlPipeline:
    def __init__(self):
        self.project_uuid = None
        self.site_id = None


    def open_spider(self, spider):
        self.project_uuid = spider.project_uuid
        self.site_id = spider.site_id
        self.seadb_api = spider.seadb_api
        self.storage = spider.storage

    def close_spider(self, spider):
        urls_to_delete = set(spider.existing_site_info.keys()) - spider.processed_urls

        if self.storage is None:
            spider.logger.warning('Update failed: storage is not initialized')
            return

        if urls_to_delete:
            sql = f"""SELECT _pk FROM `{self.site_id}` WHERE url in ({urls_to_delete}) LIMIT {len(urls_to_delete)}"""
            pks = self.seadb_api.query_rows(self.project_uuid, sql)
            self.seadb_api.delete_rows(self.project_uuid, self.site_id, pks)
            files = [url_to_filename(url) for url in urls_to_delete]
            self.storage.batch_delete_files(self.site_id, self.project_uuid, files)
            spider.logger.info('delete files: %s', files)

        spider.connection_status['total_records'] = len(spider.processed_urls)
        spider.connection_status['last_sync_status'] = CrawlStatus.COMPLETED
        spider.update_connection_status(self.site_id, json.dumps(spider.connection_status), spider.last_sync_time)

        spider.logger.info(f"Update completed: processed {len(spider.processed_urls)} URLs, deleted {len(urls_to_delete)} removed pages")

    def process_item(self, item, spider):
        try:
            self.save_content(item)
        except Exception as e:
            spider.logger.warning(f'Insert into SeaDB or save to storage failed for {item.get("url")}: {e}')
        spider.crawled_count += 1
        spider.connection_status['last_sync_count'] = spider.crawled_count
        if need_update_connection_status(spider.crawled_count):
            spider.update_connection_status(self.site_id, json.dumps(spider.connection_status), spider.last_sync_time)
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
        insert_rows = [
            {
                WEB_CRAWL_TABLE.columns.url.name: url,
                WEB_CRAWL_TABLE.columns.title.name: title,
                WEB_CRAWL_TABLE.columns.etag.name: etag,
                WEB_CRAWL_TABLE.columns.last_modified.name: last_modified,
            }
        ]
        self.storage.write_json(self.site_id, self.project_uuid, filename, {'content': content})
        self.seadb_api.insert_rows(self.project_uuid, self.site_id, insert_rows)
