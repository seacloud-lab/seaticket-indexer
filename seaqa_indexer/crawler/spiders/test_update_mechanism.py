import unittest
import os
import json
import shutil
import datetime
from unittest.mock import MagicMock, patch


from seaqa_indexer.crawler.spiders.sitemap_crawler import create_sitemap_spider
from seaqa_indexer.crawler.utils import get_site_dir, url_to_filename
from seaqa_indexer.crawler.web_crawl_manager import WebCrawlManager
from seaqa_indexer.crawler.pipelines import BatchCrawlPipeline
from seaqa_indexer.config import CRAWL_OUTPUT_DIR


class UpdateMechanismTest(unittest.TestCase):
    """Test suite for comprehensively testing the URL content update mechanism"""
    
    def setUp(self):
        """Initialize the test environment"""
        self.project_uuid = 'test_project_update'
        self.site_id = 'test_site_update'
        self.connection_id = 123
        self.site_url = 'http://example.com'
        self.sitemap_url = 'http://example.com/sitemap.xml'
        
        # Create test directory
        self.base_dir = os.path.join(CRAWL_OUTPUT_DIR, self.project_uuid)
        self.test_dir = get_site_dir(self.site_id, self.project_uuid)
        os.makedirs(self.test_dir, exist_ok=True)
        
        # Initialize test components
        self._setup_spider()
        self._setup_pipeline()
        self._setup_managers()
        
    def tearDown(self):
        """Clean up the test environment"""
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)
    
    def _setup_spider(self):
        """Initialize the spider component"""
        self.spider_class = create_sitemap_spider(
            name='TestUpdateSpider',
            sitemap_url=self.sitemap_url,
            site_url=self.site_url,
            project_uuid=self.project_uuid,
            site_id=self.site_id
        )
        self.spider = self.spider_class()
    
    def _setup_pipeline(self):
        """Initialize the pipeline component"""
        self.pipeline = BatchCrawlPipeline()
        self.pipeline.project_uuid = self.project_uuid
        self.pipeline.site_id = self.site_id
    
    @patch('seaqa_indexer.mq.get_mq')
    def _setup_managers(self, mock_redis):
        """Initialize the manager components"""
        # Mock Redis connection to avoid connection errors
        mock_redis.return_value = MagicMock()
        
        # Mock database and API
        self.mock_db_session = MagicMock()
        
        # Create manager instances
        with patch('seaqa_indexer.crawler.web_crawl_manager.init_db_session_class'):
            self.web_crawl_manager = WebCrawlManager()
            self.web_crawl_manager.db_session = self.mock_db_session

    def _create_test_cache_file(self, url, etag=None, last_modified=None, content='test content', title='test title'):
        """Create a test cache file"""
        filename = url_to_filename(url)
        filepath = os.path.join(self.test_dir, filename)
        data = {
            'url': url,
            'etag': etag,
            'last_modified': last_modified,
            'title': title,
            'content': content,
            'updated_at': datetime.datetime.now().isoformat()
        }
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return filepath
    
    def _create_mock_response(self, url, etag=None, last_modified=None, title='Mock Title', content='Mock Content', flags=None):
        """Create a mock response object"""
        response = MagicMock()
        response.url = url
        
        # Create a mutable headers dictionary
        headers_dict = {}
        if etag:
            headers_dict['ETag'] = etag.encode() if isinstance(etag, str) else etag
        if last_modified:
            headers_dict['Last-Modified'] = last_modified.encode() if isinstance(last_modified, str) else last_modified
        
        # Create a headers mock object
        headers_mock = MagicMock()
        headers_mock.get = MagicMock(side_effect=lambda key, default=None: headers_dict.get(key, default))
        
        # Support dictionary-style access
        def getitem(key):
            return headers_dict[key]
        headers_mock.__getitem__ = getitem
        headers_mock.__contains__ = lambda key: key in headers_dict
        
        response.headers = headers_mock
        
        # Set CSS and XPath selectors
        css_mock = MagicMock()
        css_mock.get.return_value = title
        response.css.return_value = css_mock
        
        xpath_mock = MagicMock()
        xpath_mock.get.return_value = content + ' ' if content else ' '
        response.xpath.return_value = [xpath_mock]
        
        response.flags = flags or []
        return response

    # Test methods - Spider level
    @patch('seaqa_indexer.crawler.spiders.sitemap_crawler.SitemapSpider.logger')
    def test_spider_add_new_url(self, mock_logger):
        """Test spider handling of new URLs"""
        test_url = 'http://example.com/new-page'
        response = self._create_mock_response(
            url=test_url,
            etag='"new-etag"',
            last_modified='Wed, 21 Oct 2015 07:28:00 GMT'
        )
        
        # Execute parsing
        items = list(self.spider.parse_item(response))
        
        # Validate results
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item['url'], test_url)
        self.assertEqual(item['etag'], '"new-etag"')
        self.assertEqual(item['last_modified'], 'Wed, 21 Oct 2015 07:28:00 GMT')
        self.assertEqual(item['title'], 'Mock Title')
        self.assertEqual(item['content'], 'Mock Content')
        
        # Validate log calls
        mock_logger.info.assert_called()

    @patch('seaqa_indexer.crawler.spiders.sitemap_crawler.SitemapSpider.logger')
    def test_spider_detect_content_change(self, mock_logger):
        """Test spider detection of content changes"""
        test_url = 'http://example.com/changed-page'
        
        # Create old cache file
        self._create_test_cache_file(
            url=test_url,
            etag='"old-etag"',
            last_modified='Tue, 20 Oct 2015 07:28:00 GMT',
            content='old content',
            title='Old Title'
        )
        
        # Create new response
        response = self._create_mock_response(
            url=test_url,
            etag='"new-etag"',
            last_modified='Wed, 21 Oct 2015 07:28:00 GMT',
            title='New Title',
            content='new content'
        )
        
        # Execute parsing
        items = list(self.spider.parse_item(response))
        
        # Validate results
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item['url'], test_url)
        self.assertEqual(item['etag'], '"new-etag"')
        self.assertEqual(item['title'], 'New Title')
        self.assertEqual(item['content'], 'new content')

    @patch('seaqa_indexer.crawler.spiders.sitemap_crawler.SitemapSpider.logger')
    def test_spider_detect_no_change(self, mock_logger):
        """Test spider detection of no content change"""
        test_url = 'http://example.com/unchanged-page'
        
        # Create cache file
        self._create_test_cache_file(
            url=test_url,
            etag='"same-etag"',
            last_modified='Wed, 21 Oct 2015 07:28:00 GMT',
            content='unchanged content',
            title='Unchanged Title'
        )
        self.spider._load_existing_cache_info()

        # Create a response with the not_modified flag
        response = self._create_mock_response(
            url=test_url,
            flags=['not_modified']
        )
        
        # Execute parsing
        items = list(self.spider.parse_item(response))
        
        # Validate results - should return cached content
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item['url'], test_url)
        self.assertEqual(item['etag'], '"same-etag"')
        self.assertEqual(item['title'], 'Unchanged Title')
        self.assertEqual(item['content'], 'unchanged content')

    @patch('seaqa_indexer.crawler.spiders.sitemap_crawler.SitemapSpider.logger')
    def test_spider_handle_deleted_cache(self, mock_logger):
        """Test spider handling of deleted cache files"""
        test_url = 'http://example.com/deleted-cache-page'
        
        # Do not create a cache file to simulate the case where the cache is deleted
        response = self._create_mock_response(
            url=test_url,
            etag='"new-etag"',
            last_modified='Wed, 21 Oct 2015 07:28:00 GMT'
        )
        
        # Execute parsing
        items = list(self.spider.parse_item(response))
        
        # Validate results
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item['url'], test_url)
        self.assertEqual(item['etag'], '"new-etag"')
        self.assertEqual(item['last_modified'], 'Wed, 21 Oct 2015 07:28:00 GMT')

    # Test methods - Pipeline level
    @patch('seaqa_indexer.crawler.spiders.sitemap_crawler.SitemapSpider.logger')
    def test_pipeline_save_new_content(self, mock_logger):
        """Test pipeline saving new content"""
        item = {
            'url': 'http://example.com/new-content',
            'title': 'New Content Title',
            'content': 'New content body',
            'etag': '"new-etag"',
            'last_modified': 'Wed, 21 Oct 2015 07:28:00 GMT'
        }
        
        # Process item
        result = self.pipeline.process_item(item, self.spider)
        
        # Verify file was created
        filename = url_to_filename(item['url'])
        filepath = os.path.join(self.test_dir, filename)
        self.assertTrue(os.path.exists(filepath))
        # Verify file content
        with open(filepath, 'r', encoding='utf-8') as f:
            saved_data = json.load(f)
        self.assertEqual(saved_data['url'], item['url'])
        self.assertEqual(saved_data['title'], item['title'])
        self.assertEqual(saved_data['content'], item['content'])
        self.assertEqual(saved_data['etag'], item['etag'])
        self.assertEqual(saved_data['last_modified'], item['last_modified'])

    @patch('seaqa_indexer.crawler.spiders.sitemap_crawler.SitemapSpider.logger')
    def test_pipeline_update_existing_content(self, mock_logger):
        """Test pipeline updating existing content"""
        test_url = 'http://example.com/existing-content'
        
        # Create existing file
        self._create_test_cache_file(
            url=test_url,
            etag='"old-etag"',
            last_modified='Tue, 20 Oct 2015 07:28:00 GMT',
            content='old content',
            title='Old Title'
        )
        
        # Update item
        item = {
            'url': test_url,
            'title': 'Updated Title',
            'content': 'Updated content',
            'etag': '"updated-etag"',
            'last_modified': 'Wed, 21 Oct 2015 07:28:00 GMT'
        }
        
        # Process item
        result = self.pipeline.process_item(item, self.spider)
        
        # Verify file was updated
        filename = url_to_filename(item['url'])
        filepath = os.path.join(self.test_dir, filename)
        
        with open(filepath, 'r', encoding='utf-8') as f:
            updated_data = json.load(f)
        
        self.assertEqual(updated_data['title'], 'Updated Title')
        self.assertEqual(updated_data['content'], 'Updated content')
        self.assertEqual(updated_data['etag'], '"updated-etag"')

if __name__ == '__main__':
    unittest.main()
