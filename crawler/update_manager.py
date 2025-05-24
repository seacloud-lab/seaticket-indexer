import os
import json
import logging
import requests
import hashlib

from datetime import datetime
from threading import Thread, Event
from urllib.parse import urlparse
from models import db, Website, Project

logger = logging.getLogger(__name__)

class UpdateManager:
    """Manage the scheduled updates of website content"""
    
    def __init__(self, app, crawler_manager, update_interval=604800):
        """Initialize the update manager
        
        Args:
            app: Flask application instance
            crawler_manager: Crawler manager instance
            update_interval: Update check interval (seconds), default is 1 week
        """
        self.app = app
        self.crawler_manager = crawler_manager
        self.update_interval = update_interval
        self.stop_event = Event()
        self.update_thread = None
        
    def start(self):
        """Start the update manager"""
        if self.update_thread is not None and self.update_thread.is_alive():
            logger.warning("Update manager is already running")
            return False
            
        self.stop_event.clear()
        self.update_thread = Thread(target=self._update_loop)
        self.update_thread.daemon = True
        self.update_thread.start()
        logger.info("Update manager started")
        return True
        
    def stop(self):
        """Stop the update manager"""
        if self.update_thread is None or not self.update_thread.is_alive():
            logger.warning("Update manager is not running")
            return False
            
        self.stop_event.set()
        self.update_thread.join(timeout=5.0)
        logger.info("Update manager stopped")
        return True
        
    def _update_loop(self):
        """Update loop, regularly check all websites"""
        while not self.stop_event.is_set():
            try:
                with self.app.app_context():
                    self._check_websites_for_updates()
            except Exception as e:
                logger.exception(f"Error in update loop: {str(e)}")
                
            # Waiting for the next update check, while allowing early exit
            self.stop_event.wait(self.update_interval)
    
    def _check_websites_for_updates(self):
        """Check if all websites need to be updated"""
        websites = Website.query.filter_by(status='completed').all()
        logger.info(f"Checking {len(websites)} websites for updates")
        
        for website in websites:
            try:
                # Obtain the website's affiliated project
                project = Project.query.get(website.project_id)
                if not project:
                    logger.error(f"Project not found for website {website.url}")
                    continue
                    
                # Construct output directory path
                output_dir = os.path.join(
                    self.crawler_manager.output_dir,
                    f"project_{project.name}"
                )
                
                # Check if the website needs to be updated
                self._check_website_for_update(website, output_dir)
                
            except Exception as e:
                logger.exception(f"Error checking website {website.url}: {str(e)}")
    
    def _check_website_for_update(self, website, output_dir):
        """Check if a single website needs to be updated
        
        Args:
            website: Website model instance
            output_dir: output dir path
        """
        # parse domain
        parsed_uri = urlparse(website.url)
        domain = '{uri.netloc}'.format(uri=parsed_uri)
        
        # Build website content directory
        website_dir = os.path.join(output_dir, domain)
        if not os.path.exists(website_dir):
            logger.warning(f"Content directory not found for {website.url}")
            return
            
        # Retrieve all crawled URL files
        crawled_files = [f for f in os.listdir(website_dir) if f.endswith('.json')]
        logger.info(f"Found {len(crawled_files)} crawled files for {website.url}")
        
        # Check each URL to see if it needs to be updated
        for filename in crawled_files:
            try:
                filepath = os.path.join(website_dir, filename)
                self._check_url_for_update(filepath, website)
            except Exception as e:
                logger.exception(f"Error checking file {filename}: {str(e)}")
    
    def _check_url_for_update(self, filepath, website):
        """Check if a single URL needs to be updated
        
        Args:
            filepath: JSON file path
            website: Website instance model
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            url = data.get('url')
            if not url:
                logger.warning(f"URL not found in file {filepath}")
                return
                
            # Send a HEAD request to check the HTTP headers
            response = requests.head(url, timeout=10, allow_redirects=True)
            
            # Check the ETag or Last-Modified in the HTTP header.
            etag = response.headers.get('ETag')
            last_modified = response.headers.get('Last-Modified')
            
            # If these values already exist in the file, compare whether there is a change.
            need_update = False
            
            if etag and data.get('etag') != etag:
                need_update = True
                logger.info(f"ETag changed for {url}")
            
            if last_modified and data.get('last_modified') != last_modified:
                need_update = True
                logger.info(f"Last-Modified changed for {url}")
                
            # If the server returns 304 Not Modified, there is no need to update.
            if response.status_code == 304:
                need_update = False
                logger.info(f"Server returned 304 Not Modified for {url}")
            
            # If an update is needed, send a GET request to retrieve new content.
            if need_update:
                self._update_url_content(url, filepath, etag, last_modified, website)
            else:
                logger.info(f"No update needed for {url}")
                
        except Exception as e:
            logger.exception(f"Error checking URL from {filepath}: {str(e)}")
    
    def _update_url_content(self, url, filepath, etag, last_modified, website):
        """Update URL content
        
        Args:
            url: Web URL
            filepath: JSON file path
            etag: New ETag value
            last_modified: New Last-Modified value
            website: Website model instance
        """
        try:
            # Send a GET request condition
            headers = {}
            if etag:
                headers['If-None-Match'] = etag
            if last_modified:
                headers['If-Modified-Since'] = last_modified
                
            response = requests.get(url, headers=headers, timeout=15)
            
            # If the content has not been modified, the server will return 304.
            if response.status_code == 304:
                logger.info(f"Content not modified for {url}")
                return
                
            # If new content is obtained
            if response.status_code == 200:
                # Read the original JSON file
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Extract new content
                from bs4 import BeautifulSoup
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract title
                title = soup.title.text if soup.title else data.get('title', '')
                
                # Extract content
                for script in soup(["script", "style"]):
                    script.extract()
                content = soup.get_text(" ", strip=True)
                
                # Update JSON data
                data['title'] = title
                data['content'] = content
                data['etag'] = etag
                data['last_modified'] = last_modified
                data['updated_at'] = datetime.now().isoformat()
                
                # Calculate content hash
                content_hash = hashlib.sha512(content.encode('utf-8')).hexdigest()
                
                # If the content hash value changes, save the update.
                if data.get('content_hash') != content_hash:
                    data['content_hash'] = content_hash
                    
                    # Save the updated JSON
                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                        
                    logger.info(f"Updated content for {url}")
                    
                    # Update the website's last_crawled_at time
                    website.last_crawled_at = datetime.now()
                    db.session.commit()
                else:
                    logger.info(f"Content hash unchanged for {url}")
            else:
                logger.warning(f"Failed to get content for {url}, status code: {response.status_code}")
                
        except Exception as e:
            logger.exception(f"Error updating content for {url}: {str(e)}")
