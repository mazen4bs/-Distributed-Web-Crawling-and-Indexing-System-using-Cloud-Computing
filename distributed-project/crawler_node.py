import requests
from bs4 import BeautifulSoup
import time
import logging
import boto3
import hashlib
import json
import socket
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# AWS Clients with retry configuration
session = boto3.Session(region_name='eu-north-1')
sqs = session.client('sqs')
s3 = session.client('s3')

# Constants
CRAWLER_ID = socket.gethostname()
QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/543442417201/mycrawlerQueue'
HEARTBEAT_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/543442417201/mycrawlerHeartbeat'
BUCKET_NAME = 'distributed-crawler-data'
CRAWL_DELAY = 1
MAX_RETRIES = 3
HEARTBEAT_INTERVAL = 60  # seconds

class Crawler:
    def __init__(self, delay=1, max_retries=MAX_RETRIES):
        self.delay = delay
        self.s3 = s3
        self.sqs = sqs
        self.crawled_count = 0
        self.failed_count = 0
        self.uploaded_count = 0
        self.max_retries = max_retries
        self.session = self._create_session()
        
    def _create_session(self):
        """Create a requests session with retry logic"""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def fetch_page(self, url):
        """Fetch a web page with error handling"""
        try:
            logging.info(f"🌐 Fetching: {url}")
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            time.sleep(self.delay)  # Simple delay between requests
            return response.text
        except requests.exceptions.RequestException as e:
            self.failed_count += 1
            logging.error(f"❌ Failed to fetch {url}: {e}")
            return None

    def extract_text(self, html):
        """Extract text from HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            for tag in soup(['script', 'style']):
                tag.decompose()
            return soup.get_text(separator=' ', strip=True)
        except Exception as e:
            logging.error(f"❌ Text extraction error: {e}")
            return ""

    def extract_links(self, html, base_url):
        """Extract links from HTML"""
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
        """Upload HTML content to S3 with retry logic"""
        try:
            filename = hashlib.md5(url.encode()).hexdigest() + '.html'
            
            # Upload to S3 with retry
            for attempt in range(self.max_retries):
                try:
                    self.s3.put_object(
                        Bucket=BUCKET_NAME,
                        Key=filename,
                        Body=html,
                        ContentType='text/html',
                        Metadata={'original-url': url}
                    )
                    break
                except ClientError as e:
                    if attempt < self.max_retries - 1:
                        logging.warning(f"⚠️ S3 upload attempt {attempt+1} failed, retrying: {e}")
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        raise
            
            self.uploaded_count += 1
            logging.info(f"✅ Uploaded to S3: {filename}")
            return filename
        except Exception as e:
            self.failed_count += 1
            logging.error(f"❌ S3 upload failed: {e}")
            return None

    def send_heartbeat(self):
        """Send heartbeat with basic retry"""
        try:
            heartbeat = {
                "crawler_id": CRAWLER_ID,
                "status": "alive",
                "timestamp": time.time(),
                "crawled": self.crawled_count,
                "uploaded": self.uploaded_count,
                "failed": self.failed_count
            }
            
            # Try to send with simple retry
            for attempt in range(self.max_retries):
                try:
                    self.sqs.send_message(
                        QueueUrl=HEARTBEAT_QUEUE_URL,
                        MessageBody=json.dumps(heartbeat)
                    )
                    break
                except ClientError:
                    if attempt < self.max_retries - 1:
                        time.sleep(1)  # Simple delay between retries
            
            logging.info("💓 Heartbeat sent")
        except Exception as e:
            logging.error(f"❌ Failed to send heartbeat: {e}")

def poll_and_crawl():
    """Main crawling function with error handling"""
    crawler = Crawler(delay=CRAWL_DELAY)
    last_heartbeat_time = time.time()
    
    logging.info(f"🚀 Crawler node {CRAWLER_ID} starting")
    
    # Send initial heartbeat
    crawler.send_heartbeat()

    while True:
        try:
            # Send heartbeat at regular intervals
            now = time.time()
            if now - last_heartbeat_time >= HEARTBEAT_INTERVAL:
                crawler.send_heartbeat()
                last_heartbeat_time = now

            # Get message from queue
            messages = sqs.receive_message(
                QueueUrl=QUEUE_URL, 
                MaxNumberOfMessages=1, 
                WaitTimeSeconds=10
            )
            
            if 'Messages' not in messages:
                logging.info("📭 No messages in queue, waiting...")
                continue

            for message in messages['Messages']:
                try:
                    # Process message body
                    raw_body = message['Body']
                    try:
                        body = json.loads(raw_body)
                        url = body.get('url')
                    except json.JSONDecodeError:
                        url = raw_body.strip()
                        logging.info(f"Processing plain URL: {url}")

                    if not url:
                        logging.warning("⚠️ No URL in message")
                        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message['ReceiptHandle'])
                        continue

                    # Fetch and process page
                    html = crawler.fetch_page(url)
                    if html:
                        crawler.crawled_count += 1
                        crawler.upload_to_s3(html, url)
                        links = crawler.extract_links(html, url)
                        logging.info(f"🔍 Crawled {url}, found {len(links)} links")

                except Exception as e:
                    logging.error(f"❌ Error processing message: {e}")
                    crawler.failed_count += 1
                finally:
                    # Always delete the message when done
                    sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message['ReceiptHandle'])

        except KeyboardInterrupt:
            logging.info("👋 Crawler stopping")
            crawler.send_heartbeat()  # Final heartbeat
            break
            
        except Exception as e:
            logging.error(f"❌ Unexpected error: {e}")
            time.sleep(5)  # Back off on errors

if __name__ == '__main__':
    try:
        poll_and_crawl()
    except Exception as e:
        logging.critical(f"🚨 Fatal error: {e}")
