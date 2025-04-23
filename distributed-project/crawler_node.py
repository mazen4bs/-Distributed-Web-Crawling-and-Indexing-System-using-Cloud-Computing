# crawler_node.py

from mpi4py import MPI
import requests
from bs4 import BeautifulSoup
import time
import logging
from urllib.parse import urljoin

# Logging setup (logs each crawler in a separate file)
#logging.basicConfig(
 #   filename=f'crawler_{MPI.COMM_WORLD.Get_rank()}.log',
  #  level=logging.INFO,
   # format='%(asctime)s - %(levelname)s - %(message)s'
#)

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
            for tag in soup(['script', 'style']):
                tag.decompose()

            headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
            paragraphs = soup.find_all('p')
            text = ' '.join(tag.get_text(separator=' ', strip=True) for tag in headings + paragraphs)
            return text if text else soup.get_text(separator=' ', strip=True)
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
                full_url = urljoin(base_url, href)
                links.add(full_url)
            return list(links)
        except Exception as e:
            logging.error(f"Error extracting links: {e}")
            return []

    def crawl(self, url):
        try:
            html = self.fetch_page(url)
            if not html:
                return None, []

            text = self.extract_text(html)
            links = self.extract_links(html, base_url=url)
            return text, links
        except Exception as e:
            logging.error(f"Error during crawl for URL {url}: {e}")
            return None, []

def crawler_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    crawler = Crawler(crawl_delay=1)

    logging.info(f"Crawler {rank} ready")

    while True:
        try:
            url = comm.recv(source=0, tag=0)
            if url == "STOP":
                logging.info(f"Crawler {rank} received STOP signal. Shutting down.")
                break

            logging.info(f"Crawler {rank} processing URL: {url}")
            text, new_links = crawler.crawl(url)

            if text is None and not new_links:
                logging.warning(f"Crawler {rank} failed to extract data from URL: {url}")

            result = {
                'url': url,
                'text': text,
                'new_links': new_links
            }
            comm.send(result, dest=0, tag=1)
            logging.info(f"Crawler {rank} sent results for {url}")
        except Exception as e:
            logging.error(f"Error in crawler {rank} while processing URL: {url}. Error: {e}")

    logging.info(f"Crawler {rank} process finished.")
    MPI.Finalize()

if __name__ == '__main__':
    crawler_process()
