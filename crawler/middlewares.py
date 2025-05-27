import logging
from scrapy import signals


logger = logging.getLogger(__name__)


class HttpHeadersMiddleware:
    """Middleware for saving HTTP header information"""
    
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware
        
    def spider_opened(self, spider):
        logger.info(f"HTTP Headers middleware enabled for {spider.name}")
        
    def process_response(self, request, response, spider):
        """Process the response, save HTTP header information"""
        # Save ETag
        if 'ETag' in response.headers:
            etag = response.headers['ETag'].decode('utf-8')
            spider.crawler.stats.set_value('etag', etag)
            
        # Save Last-Modified
        if 'Last-Modified' in response.headers:
            last_modified = response.headers['Last-Modified'].decode('utf-8')
            spider.crawler.stats.set_value('last-modified', last_modified)
            
        return response
