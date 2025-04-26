import requests
from bs4 import BeautifulSoup
import time
import logging
import boto3
import hashlib
import json
import threading
import queue
import urllib.parse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# AWS S3 and SQS Clients
sqs = boto3.client('sqs', region_name='eu-north-1')
s3 = boto3.client('s3')

# Constants (set your own)
QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/543442417201/mycrawlerQueue'
BUCKET_NAME = 'distributed-crawler-data'
CRAWL_DELAY = 1  # seconds
MAX_QUEUE_SIZE = 1000  # Maximum URLs to keep in the queue
MAX_WORKERS = 5  # Number of worker nodes to dispatch URLs to

class MasterNode:
    def __init__(self, queue_url=QUEUE_URL, bucket_name=BUCKET_NAME):
        self.queue_url = queue_url
        self.bucket_name = bucket_name
        self.visited_urls = set()
        self.url_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
        self.lock = threading.Lock()
        
    def add_urls_to_queue(self, urls):
        """Add URLs to the SQS queue"""
        for url in urls:
            if url not in self.visited_urls:
                with self.lock:
                    self.visited_urls.add(url)
                
                # Add URL to SQS queue
                try:
                    message_body = json.dumps({'url': url})
                    sqs.send_message(
                        QueueUrl=self.queue_url,
                        MessageBody=message_body
                    )
                    logging.info(f"✅ Added to queue: {url}")
                except Exception as e:
                    logging.error(f"❌ Failed to add URL to queue: {url}, Error: {e}")
    
    def get_queue_status(self):
        """Get the current status of the SQS queue"""
        try:
            response = sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
            )
            
            visible = int(response['Attributes']['ApproximateNumberOfMessages'])
            in_flight = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])
            
            logging.info(f"Queue Status: {visible} messages visible, {in_flight} messages in flight")
            return visible, in_flight
        except Exception as e:
            logging.error(f"❌ Failed to get queue status: {e}")
            return 0, 0
    
    def monitor_worker_activity(self):
        """Monitor the S3 bucket for new results from worker nodes"""
        # This is a placeholder for monitoring worker activity
        # You could implement logic to check for new files in the S3 bucket
        # or check CloudWatch metrics for SQS activity
        while True:
            visible, in_flight = self.get_queue_status()
            logging.info(f"Monitoring workers: {in_flight} URLs being processed")
            time.sleep(10)  # Check every 10 seconds
    
    def heartbeat_check(self):
      """Replace with SQS-only monitoring"""
      while True:
          visible, in_flight = self.get_queue_status()
          logging.info(f"Workers processing: {in_flight} URLs")
          time.sleep(30)

    def start(self, seed_urls):
        """Start the master node with seed URLs"""
        # Add seed URLs to the queue
        self.add_urls_to_queue(seed_urls)
        
        # Start monitoring thread
        monitor_thread = threading.Thread(target=self.monitor_worker_activity)
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self.heartbeat_check)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()
        
        logging.info("Master node started successfully")
        
        # Main loop to keep the master node running
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Master node shutting down...")

def normalize_url(url):
    """Normalize URL to avoid duplicates"""
    parsed = urllib.parse.urlparse(url)
    # Remove fragments
    normalized = parsed._replace(fragment='').geturl()
    # Ensure URL has a scheme
    if not parsed.scheme:
        normalized = 'http://' + normalized
    return normalized

if __name__ == '__main__':
    # Replace with your seed URLs
    seed_urls = [
        'http://example.com',
        'http://example.org',
        'https://www.python.org',
        'https://aws.amazon.com'
    ]
    
    # Normalize seed URLs
    normalized_seeds = [normalize_url(url) for url in seed_urls]
    
    # Start master node
    master = MasterNode()
    master.start(normalized_seeds)


"""

# master_node.py

import boto3
import logging
import json
import time

# Setup
logging.basicConfig(level=logging.INFO)

# AWS clients
sqs = boto3.client('sqs', region_name='eu-north-1')

# Constants (replace with your values)
CRAWLER_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/543442417201/mycrawlerQueue'

# Example seed URLs
SEED_URLS = [
    "https://example.com",
    "https://news.ycombinator.com",
    "https://docs.python.org/3/",
    "https://www.wikipedia.org/"
]

def send_urls_to_crawler_queue(urls):
    for url in urls:
        try:
            message = json.dumps({"url": url})
            sqs.send_message(QueueUrl=CRAWLER_QUEUE_URL, MessageBody=message)
            logging.info(f"✅ Sent to Crawler Queue: {url}")
        except Exception as e:
            logging.error(f"❌ Failed to send URL to Crawler Queue: {url} | Error: {e}")

def main():
    logging.info("🚀 Master Node starting...")
    send_urls_to_crawler_queue(SEED_URLS)
    logging.info("📬 All seed URLs sent to crawler queue.")

if __name__ == '__main__':
    main()
"""