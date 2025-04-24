from celery import Celery
from celery.result import AsyncResult
import logging
import time

# Initialize Celery app
app = Celery('crawler', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')
app.conf.task_acks_late = True

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@app.task
def crawl_url_task(url):
    """Task that is sent to a worker to crawl a URL"""
    from crawler_node import Crawler
    crawler = Crawler(crawl_delay=1)
    text, new_links = crawler.crawl(url)
    return {'url': url, 'text': text, 'new_links': new_links}

def distribute_tasks(seed_urls):
    """Distribute URLs to the task queue"""
    results = []
    for url in seed_urls:
        result = crawl_url_task.apply_async(args=[url])
        results.append(result)
    return results

def monitor_tasks(results):
    """Monitor and fetch results from tasks"""
    for result in results:
        # Wait for the result to be ready
        while not result.ready():
            time.sleep(1)
        
        # Get the result
        if result.successful():
            logging.info(f"Task completed for URL: {result.result['url']}")
            logging.info(f"Extracted {len(result.result['new_links'])} links.")
        else:
            logging.error(f"Task failed for URL: {result.id}")

if __name__ == '__main__':
    seed_urls = ['http://example.com', 'http://example.org']  # Replace with your seed URLs
    task_results = distribute_tasks(seed_urls)
    monitor_tasks(task_results)
