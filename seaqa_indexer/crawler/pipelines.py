import json
import datetime

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

        if urls_to_delete:
            formatted_urls = ", ".join("'{}'".format(url) for url in urls_to_delete)
            sql = f"""SELECT _pk FROM `{self.site_id}` WHERE url in ({formatted_urls}) LIMIT {len(urls_to_delete)}"""
            resp_results = self.seadb_api.query_rows(self.project_uuid, sql)
            results = resp_results.get('results', [])
            rows = []
            for result in results:
                row = {
                    "pk": result.get('_pk'),
                    "row": {
                        WEB_CRAWL_TABLE.columns.deleted.name: True,
                        WEB_CRAWL_TABLE.columns.updated_at.name: datetime.datetime.now(datetime.UTC).isoformat(),
                    }
                }
                rows.append(row)
            self.seadb_api.update_rows(self.project_uuid, str(self.site_id), rows)
            files = [url_to_filename(url) for url in urls_to_delete]
            self.storage.batch_delete_files(self.site_id, self.project_uuid, files)
            spider.logger.info('delete files: %s', files)

        spider.connection_status['total_records'] = len(spider.processed_urls)
        spider.connection_status['last_sync_status'] = CrawlStatus.COMPLETED
        last_sync_time = datetime.datetime.now(datetime.UTC)
        spider.update_connection_status(self.site_id, json.dumps(spider.connection_status), last_sync_time)

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
        self.storage.write_json(self.site_id, self.project_uuid, filename, {'content': content})

        sql = f"""SELECT _pk FROM `{self.site_id}` WHERE url='{url}'"""
        resp_result = self.seadb_api.query_rows(self.project_uuid, sql)
        results = resp_result.get('results')
        if results:
            rows = [
                {
                    "pk": results[0].get('_pk'),
                    "row": {
                        WEB_CRAWL_TABLE.columns.title.name: title,
                        WEB_CRAWL_TABLE.columns.etag.name: etag,
                        WEB_CRAWL_TABLE.columns.last_modified.name: last_modified,
                        WEB_CRAWL_TABLE.columns.updated_at.name: datetime.datetime.now(datetime.UTC).isoformat(),
                    }
                }
            ]
            self.seadb_api.update_rows(self.project_uuid, str(self.site_id), rows)
        else:
            rows = [
                {
                    WEB_CRAWL_TABLE.columns.url.name: url,
                    WEB_CRAWL_TABLE.columns.title.name: title,
                    WEB_CRAWL_TABLE.columns.etag.name: etag,
                    WEB_CRAWL_TABLE.columns.last_modified.name: last_modified,
                    WEB_CRAWL_TABLE.columns.updated_at.name: datetime.datetime.now(datetime.UTC).isoformat(),
                }
            ]
            self.seadb_api.insert_rows(self.project_uuid, str(self.site_id), rows)
