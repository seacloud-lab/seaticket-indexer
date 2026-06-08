import json
import datetime

from seaqa_indexer.crawler.utils import url_to_filename
from seaqa_indexer.utils.constants import CrawlStatus
from seaqa_indexer.utils import need_update_connection_last_sync_count
from seaqa_indexer.seadb_models.models import SchemaTables


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
            table_name = SchemaTables.WEB_CRAWL.table_name(self.site_id)
            sql = f"""SELECT _pk FROM `{table_name}` WHERE url in ({formatted_urls}) LIMIT {len(urls_to_delete)}"""
            resp_results = self.seadb_api.query_rows(self.project_uuid, sql)
            results = resp_results.get('results', [])
            rows = []
            for result in results:
                row = {
                    "pk": result.get('_pk'),
                    "row": {
                        SchemaTables.WEB_CRAWL.column.deleted.name: True,
                        SchemaTables.WEB_CRAWL.column.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
                        SchemaTables.WEB_CRAWL.column.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
                    }
                }
                rows.append(row)
            self.seadb_api.update_rows(self.project_uuid, table_name, rows)
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
        if not hasattr(spider, '_status_last_update_time'):
            spider._status_last_update_time = datetime.datetime.now()

        spider.connection_status['last_sync_count'] = spider.crawled_count
        should, new_time, new_count = need_update_connection_last_sync_count(
            spider.crawled_count, getattr(spider, '_status_last_update_time'), getattr(spider, '_status_last_update_count', 0)
        )
        if should:
            spider.update_connection_status(self.site_id, json.dumps(spider.connection_status))
            spider._status_last_update_time = new_time
            spider._status_last_update_count = new_count
        return item

    def save_content(self, item):
        url = item['url']
        title = item['title']
        content = item['content']
        hash = item.get('hash') if item else None
        etag = item.get('etag') if item else None
        modified_time = item.get('last_modified') if item else None
        filename = url_to_filename(url)

        table_name = SchemaTables.WEB_CRAWL.table_name(connection_id=self.site_id)
        sql = f"""SELECT _pk FROM `{table_name}` WHERE url='{url}'"""
        resp_result = self.seadb_api.query_rows(self.project_uuid, sql)
        results = resp_result.get('results')
        if results:
            old_hash = results[0].get('hash')
            if hash and hash != old_hash:
                self.storage.write_json(self.site_id, self.project_uuid, filename, {'content': content})
                return
            rows = [
                {
                    "pk": results[0].get('_pk'),
                    "row": {
                        SchemaTables.WEB_CRAWL.column.title.name: title,
                        SchemaTables.WEB_CRAWL.column.etag.name: etag,
                        SchemaTables.WEB_CRAWL.column.modified_time.name: modified_time,
                        SchemaTables.WEB_CRAWL.column.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
                        SchemaTables.WEB_CRAWL.column.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
                        SchemaTables.WEB_CRAWL.column.hash.name: hash,
                    }
                }
            ]
            self.seadb_api.update_rows(self.project_uuid, table_name, rows)
        else:
            self.storage.write_json(self.site_id, self.project_uuid, filename, {'content': content})
            rows = [
                {
                    SchemaTables.WEB_CRAWL.column.url.name: url,
                    SchemaTables.WEB_CRAWL.column.title.name: title,
                    SchemaTables.WEB_CRAWL.column.etag.name: etag,
                    SchemaTables.WEB_CRAWL.column.modified_time.name: modified_time,
                    SchemaTables.WEB_CRAWL.column.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
                    SchemaTables.WEB_CRAWL.column.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
                    SchemaTables.WEB_CRAWL.column.hash.name: hash,
                }
            ]
            self.seadb_api.insert_rows(self.project_uuid, table_name, rows)
