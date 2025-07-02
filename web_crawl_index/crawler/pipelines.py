from web_crawl_index.seasearch.utils import gen_site_index_name


class BatchCrawlPipeline:
    def __init__(self):
        self.site_index = None

    def open_spider(self, spider):
        self.site_index = spider.site_index

    def close_spider(self, spider):
        pass

    def process_item(self, item, spider):
        url = item['url']
        title = item['title']
        content = item['content']
        etag = None
        last_modified = None
        if hasattr(spider, 'crawler') and hasattr(spider.crawler, 'stats'):
            stats = spider.crawler.stats.get_stats()
            if 'etag' in stats:
                etag = stats['etag']
            if 'last-modified' in stats:
                last_modified = stats['last-modified']
        site_id = spider.site_id
        project_uuid = spider.project_uuid
        site_index_name = gen_site_index_name(project_uuid)

        self.site_index.add_document(site_index_name, site_id, content, url, etag, title, last_modified)

        return item
