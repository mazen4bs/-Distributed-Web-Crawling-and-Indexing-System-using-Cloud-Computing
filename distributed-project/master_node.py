import boto3
import logging
import json
import time
import threading
import queue
import urllib.parse

# Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# AWS Clients
sqs = boto3.client("sqs", region_name="eu-north-1")
s3 = boto3.client("s3")

# Constants
CRAWLER_QUEUE_URL = "https://sqs.eu-north-1.amazonaws.com/543442417201/mycrawlerQueue"
HEARTBEAT_QUEUE_URL = "https://sqs.eu-north-1.amazonaws.com/543442417201/mycrawlerHeartbeat"
INDEXER_HEARTBEAT_QUEUE_URL = "https://sqs.eu-north-1.amazonaws.com/543442417201/myindexerHeartbeat"
BUCKET_NAME = "distributed-crawler-data"
MAX_QUEUE_SIZE = 1000
TASK_TIMEOUT = 180  # seconds

class MasterNode:
    def __init__(self):
        self.url_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
        self.visited_urls = set()
        self.task_status = {}  # url -> {timestamp, status}
        self.crawler_status = {}  # crawler_id -> last_heartbeat_time
        self.stats = {"total_urls": 0, "requeued": 0, "active_crawlers": 0, "failed_crawlers": 0}

    def add_urls_to_queue(self, urls):
        for url in urls:
            # Normalize URL to avoid duplicates
            url = normalize_url(url)
            
            if url not in self.visited_urls:
                self.visited_urls.add(url)
                message = json.dumps({"url": url})
                
                try:
                    sqs.send_message(QueueUrl=CRAWLER_QUEUE_URL, MessageBody=message)
                    self.task_status[url] = {"timestamp": time.time(), "status": "queued"}
                    self.stats["total_urls"] += 1
                    logging.info(f"✅ Sent URL to queue: {url}")
                except Exception as e:
                    logging.error(f"❌ Failed to send URL to queue: {url}, {str(e)}")

    def monitor_heartbeats(self):
        while True:
            try:
                messages = sqs.receive_message(
                    QueueUrl=HEARTBEAT_QUEUE_URL, MaxNumberOfMessages=10, WaitTimeSeconds=10
                )
                if "Messages" in messages:
                    for msg in messages["Messages"]:
                        try:
                            data = json.loads(msg["Body"])
                            cid = data["crawler_id"]
                            self.crawler_status[cid] = time.time()
                            logging.info(f"💓 Heartbeat from {cid} | Crawled: {data.get('crawled', 0)} | Failed: {data.get('failed', 0)}")
                        except Exception as e:
                            logging.error(f"❌ Error processing heartbeat: {e}")
                        finally:
                            sqs.delete_message(
                                QueueUrl=HEARTBEAT_QUEUE_URL, 
                                ReceiptHandle=msg["ReceiptHandle"]
                            )
            except Exception as e:
                logging.error(f"❌ Error in heartbeat monitor: {e}")
                time.sleep(5)  # Back off on errors

    def monitor_indexer_heartbeats(self):
        while True:
            try:
                messages = sqs.receive_message(
                    QueueUrl=INDEXER_HEARTBEAT_QUEUE_URL, MaxNumberOfMessages=10, WaitTimeSeconds=10
                )
                if "Messages" in messages:
                    for msg in messages["Messages"]:
                        try:
                            data = json.loads(msg["Body"])
                            iid = data["indexer_id"]
                            logging.info(f"💓 Heartbeat from indexer {iid} | Indexed: {data.get('indexed', 0)}")
                        except Exception as e:
                            logging.error(f"❌ Error processing indexer heartbeat: {e}")
                        finally:
                            sqs.delete_message(
                                QueueUrl=INDEXER_HEARTBEAT_QUEUE_URL, 
                                ReceiptHandle=msg["ReceiptHandle"]
                            )
            except Exception as e:
                logging.error(f"❌ Error in indexer heartbeat monitor: {e}")
                time.sleep(5)  # Back off on errors

    def check_task_timeouts(self):
        while True:
            time.sleep(15)
            try:
                now = time.time()
                for url, meta in list(self.task_status.items()):
                    if meta["status"] == "queued":
                        if now - meta["timestamp"] > TASK_TIMEOUT:
                            logging.warning(f"⏱️ Task timeout for URL: {url}, requeuing...")
                            message = json.dumps({"url": url})
                            try:
                                sqs.send_message(QueueUrl=CRAWLER_QUEUE_URL, MessageBody=message)
                                self.task_status[url]["timestamp"] = time.time()
                                self.stats["requeued"] += 1
                            except Exception as e:
                                logging.error(f"❌ Failed to requeue URL: {url}, {str(e)}")
            except Exception as e:
                logging.error(f"❌ Error checking task timeouts: {e}")
                time.sleep(5)  # Back off on errors

    def monitor_crawler_health(self):
        while True:
            time.sleep(30)
            try:
                now = time.time()
                active = 0
                failed = 0
                for cid, ts in self.crawler_status.items():
                    if now - ts <= 90:
                        active += 1
                    else:
                        failed += 1
                        logging.warning(f"❌ Crawler {cid} considered FAILED (no heartbeat in 90s)")
                self.stats["active_crawlers"] = active
                self.stats["failed_crawlers"] = failed
                logging.info(f"📊 Crawler Status | Active: {active} | Failed: {failed}")
            except Exception as e:
                logging.error(f"❌ Error monitoring crawler health: {e}")
                time.sleep(5)  # Back off on errors

    def report_stats(self):
        while True:
            time.sleep(60)
            try:
                logging.info(f"📈 Stats: {self.stats}")
            except Exception as e:
                logging.error(f"❌ Error reporting stats: {e}")

    def start(self, seed_urls):
        self.add_urls_to_queue(seed_urls)

        # Start only the essential monitoring threads
        threading.Thread(target=self.monitor_heartbeats, daemon=True).start()
        threading.Thread(target=self.check_task_timeouts, daemon=True).start()
        threading.Thread(target=self.monitor_crawler_health, daemon=True).start()
        threading.Thread(target=self.monitor_indexer_heartbeats, daemon=True).start()
        threading.Thread(target=self.report_stats, daemon=True).start()

        logging.info("🚀 Master Node started with heartbeat & timeout monitoring")
        logging.info(f"🌱 Added {len(seed_urls)} seed URLs to the crawl queue")
        
        # Keep main thread alive
        while True:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                logging.info("👋 Shutting down gracefully...")
                break

def normalize_url(url):
    """Normalize URL to canonical form"""
    try:
        parsed = urllib.parse.urlparse(url)
        # Remove fragments and standardize scheme
        normalized = parsed._replace(fragment="").geturl()
        # Add http:// if no scheme
        if not parsed.scheme:
            normalized = "http://" + normalized
        # Remove trailing slashes for consistency
        return normalized.rstrip("/")
    except Exception:
        return url  # Return original on error

if __name__ == "__main__":
    seed_urls = [
        "http://example.com",
        "https://aws.amazon.com",
        "https://www.python.org",
        "https://docs.python.org/3/",
        "https://en.wikipedia.org/wiki/Web_crawler",
        "https://news.ycombinator.com"
    ]
    
    # Normalize all seed URLs
    normalized_seeds = [normalize_url(url) for url in seed_urls]
    
    # Create and start master node
    master = MasterNode()
    master.start(normalized_seeds)
