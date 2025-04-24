# crawler_node.py
import requests
from bs4 import BeautifulSoup
import time
import logging
from celery import Celery

# Configure Celery app (assumes Redis running on localhost)
app = Celery('crawler', broker='redis://localhost:6379/0')

# Configure logging
logging.basicConfig(
    filename='crawler.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class Crawler:
    def __init__(self, crawl_delay=1):
        self.delay = crawl_delay

    def fetch_page(self, url):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            time.sleep(self.delay)
            return response.text
        except Exception as e:
            logging.error(f"Failed to fetch {url}: {e}")
            return None

    def extract_text(self, html):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            for script in soup(["script", "style"]):
                script.decompose()
            headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
            paragraphs = soup.find_all('p')
            text = ' '.join([tag.get_text(separator=' ', strip=True) for tag in headings + paragraphs])
            clean_text = soup.get_text(separator=' ', strip=True)
            return text if text else clean_text
        except Exception as e:
            logging.error(f"Error extracting text: {e}")
            return ""

    def extract_links(self, html, base_url):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            links = set()
            for tag in soup.find_all('a', href=True):
                href = tag['href']
                if '#' in href:
                    continue
                if '?' in href:
                    continue
                if href.startswith('http'):
                    links.add(href)
                elif href.startswith('/'):
                    links.add(base_url + href)
            return list(links)
        except Exception as e:
            logging.error(f"Error extracting links: {e}")
            return []

    def crawl(self, url):
        try:
            html = self.fetch_page(url)
            if not html:
                logging.warning(f"Failed to fetch or empty content for URL: {url}")
                return None, []
            text = self.extract_text(html)
            links = self.extract_links(html, base_url=url)
            return text, links
        except Exception as e:
            logging.error(f"Error during crawl for URL {url}: {e}")
            return None, []

@app.task
def crawl_task(url):
    logging.info(f"Starting crawl task for {url}")
    crawler = Crawler(crawl_delay=1)
    text, links = crawler.crawl(url)
    result = {
        'url': url,
        'text': text,
        'new_links': links
    }
    logging.info(f"Completed crawl task for {url} with {len(links)} links.")
    return result
