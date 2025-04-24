import requests
from bs4 import BeautifulSoup
import time
import logging
from celery import Celery
from celery.signals import worker_shutdown
from celery.utils.log import get_task_logger

# Configure Celery app
app = Celery('crawler', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')
logger = get_task_logger(__name__)

# Configure logging for worker node
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Crawler:
    def __init__(self, crawl_delay=1):
        self.delay = crawl_delay

    def fetch_page(self, url):
        """Fetch HTML content of the given URL."""
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            time.sleep(self.delay)
            return response.text
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def extract_text(self, html):
        """Extract and clean visible text from HTML."""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            # Remove unwanted tags (e.g., script, style)
            for script in soup(["script", "style"]):
                script.decompose()
            return soup.get_text(separator=' ', strip=True)
        except Exception as e:
            logger.error(f"Error extracting text: {e}")
            return ""

    def extract_links(self, html, base_url):
        """Extract all anchor tag href links."""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            links = set()
            for tag in soup.find_all('a', href=True):
                href = tag['href']
                if href.startswith('http'):
                    links.add(href)
                elif href.startswith('/'):
                    links.add(base_url + href)
            return list(links)
        except Exception as e:
            logger.error(f"Error extracting links: {e}")
            return []

    def crawl(self, url):
        """Main crawl logic: fetch, parse, extract."""
        html = self.fetch_page(url)
        if not html:
            return None, []
        text = self.extract_text(html)
        links = self.extract_links(html, base_url=url)
        return text, links

@app.task
def crawl_url_task(url):
    """Task that is sent to a worker to crawl a URL"""
    crawler = Crawler(crawl_delay=1)
    text, new_links = crawler.crawl(url)
    return {'url': url, 'text': text, 'new_links': new_links}

@app.task
def heartbeat_task():
    """Task to simulate a heartbeat/ping from the worker node."""
    logger.info("Worker is alive and processing.")
    return "heartbeat successful"

@worker_shutdown.connect
def on_worker_shutdown(sender, **kwargs):
    """Gracefully handle worker shutdown."""
    logger.info("Worker is shutting down gracefully.")

