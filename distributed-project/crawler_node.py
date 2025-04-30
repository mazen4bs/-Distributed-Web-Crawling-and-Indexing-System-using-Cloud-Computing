import requests
from bs4 import BeautifulSoup
import time
import logging
import boto3
import hashlib
import json
import socket

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# AWS Clients
sqs = boto3.client('sqs', region_name='eu-north-1')
s3 = boto3.client('s3')

# Constants
CRAWLER_ID = socket.gethostname()  # Unique ID per crawler VM
QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/543442417201/mycrawlerQueue'
HEARTBEAT_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/543442417201/mycrawlerHeartbeat'
BUCKET_NAME = 'distributed-crawler-data'
CRAWL_DELAY = 1

# Counters for monitoring
crawled_count = 0
failed_count = 0
uploaded_count = 0

class Crawler:
    def __init__(self, delay=1):
        self.delay = delay
        self.s3 = boto3.client('s3')
        self.sqs = boto3.client('sqs')

    def fetch_page(self, url):
        try:
            logging.info(f"🌐 Fetching: {url}")
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            time.sleep(self.delay)
            return response.text
        except Exception as e:
            global failed_count
            failed_count += 1
            logging.error(f"❌ Failed to fetch {url}: {e}")
            return None

    def extract_text(self, html):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            for tag in soup(['script', 'style']):
                tag.decompose()
            return soup.get_text(separator=' ', strip=True)
        except Exception as e:
            logging.error(f"❌ Text extraction error: {e}")
            return ""

    def extract_links(self, html, base_url):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            links = set()
            for tag in soup.find_all('a', href=True):
                href = tag['href']
                if href.startswith('http'):
                    links.add(href)
                elif href.startswith('/'):
                    links.add(base_url.rstrip('/') + href)
            return list(links)
        except Exception as e:
            logging.error(f"❌ Link extraction error: {e}")
            return []

    def upload_to_s3(self, html, url):
        global uploaded_count
        try:
            filename = hashlib.md5(url.encode()).hexdigest() + '.html'
            self.s3.put_object(
                Bucket=BUCKET_NAME,
                Key=filename,
                Body=html,
                ContentType='text/html',
                Metadata={'original-url': url}
            )
            uploaded_count += 1
            logging.info(f"✅ Uploaded to S3: {filename}")
            return filename
        except Exception as e:
            logging.error(f"❌ S3 upload failed: {e}")
            return None

    def send_heartbeat(self):
        """Send a heartbeat signal to Master."""
        try:
            heartbeat = {
                "crawler_id": CRAWLER_ID,
                "status": "alive",
                "timestamp": time.time(),
                "crawled": crawled_count,
                "uploaded": uploaded_count,
                "failed": failed_count
            }
            sqs.send_message(
                QueueUrl=HEARTBEAT_QUEUE_URL,
                MessageBody=json.dumps(heartbeat)
            )
            logging.info("💓 Heartbeat sent to Master")
        except Exception as e:
            logging.error(f"❌ Failed to send heartbeat: {e}")

def poll_and_crawl():
    crawler = Crawler(delay=CRAWL_DELAY)

    heartbeat_interval = 60  # seconds
    last_heartbeat_time = time.time()

    while True:
        now = time.time()
        if now - last_heartbeat_time >= heartbeat_interval:
            crawler.send_heartbeat()
            last_heartbeat_time = now

        messages = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=10)
        if 'Messages' not in messages:
            logging.info("📭 No messages in queue, waiting...")
            continue

        for message in messages['Messages']:
            try:
                raw_body = message['Body']
                try:
                    body = json.loads(raw_body)
                    url = body.get('url')
                except json.JSONDecodeError:
                    url = raw_body.strip()
                    logging.info(f"Treating message body as plain URL: {url}")

                if not url:
                    logging.warning("⚠️ No URL in message")
                    continue

                html = crawler.fetch_page(url)
                if html:
                    global crawled_count
                    crawled_count += 1
                    crawler.upload_to_s3(html, url)
                    text = crawler.extract_text(html)
                    links = crawler.extract_links(html, url)

                    logging.info(f"🔍 Crawled {url}, extracted {len(links)} links")
            except Exception as e:
                logging.error(f"❌ Error processing message: {e}")
            finally:
                sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message['ReceiptHandle'])

if __name__ == '__main__':
    poll_and_crawl()
