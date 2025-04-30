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
            if url not in self.visited_urls:
                self.visited_urls.add(url)
                message = json.dumps({"url": url})
                sqs.send_message(QueueUrl=CRAWLER_QUEUE_URL, MessageBody=message)
                self.task_status[url] = {"timestamp": time.time(), "status": "queued"}
                self.stats["total_urls"] += 1
                logging.info(f"✅ Sent URL to queue: {url}")

    def monitor_heartbeats(self):
        while True:
            messages = sqs.receive_message(
                QueueUrl=HEARTBEAT_QUEUE_URL, MaxNumberOfMessages=10, WaitTimeSeconds=10
            )
            if "Messages" in messages:
                for msg in messages["Messages"]:
                    try:
                        data = json.loads(msg["Body"])
                        cid = data["crawler_id"]
                        self.crawler_status[cid] = time.time()
                        logging.info(f"💓 Heartbeat from {cid} | Crawled: {data['crawled']} | Failed: {data['failed']}")
                    except Exception as e:
                        logging.error(f"❌ Error processing heartbeat: {e}")
                    finally:
                        sqs.delete_message(QueueUrl=HEARTBEAT_QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])

    def check_task_timeouts(self):
        while True:
            time.sleep(15)
            now = time.time()
            for url, meta in list(self.task_status.items()):
                if meta["status"] == "queued":
                    if now - meta["timestamp"] > TASK_TIMEOUT:
                        logging.warning(f"⏱️ Task timeout for URL: {url}, requeuing...")
                        message = json.dumps({"url": url})
                        sqs.send_message(QueueUrl=CRAWLER_QUEUE_URL, MessageBody=message)
                        self.task_status[url]["timestamp"] = time.time()
                        self.stats["requeued"] += 1

    def monitor_crawler_health(self):
        while True:
            time.sleep(30)
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

    def report_stats(self):
        while True:
            time.sleep(60)
            logging.info(f"📈 Stats: {self.stats}")

    def start(self, seed_urls):
        self.add_urls_to_queue(seed_urls)

        threading.Thread(target=self.monitor_heartbeats, daemon=True).start()
        threading.Thread(target=self.check_task_timeouts, daemon=True).start()
        threading.Thread(target=self.monitor_crawler_health, daemon=True).start()
        threading.Thread(target=self.report_stats, daemon=True).start()

        logging.info("🚀 Master Node started with heartbeat & timeout monitoring")
        while True:
            time.sleep(1)

def normalize_url(url):
    parsed = urllib.parse.urlparse(url)
    normalized = parsed._replace(fragment="").geturl()
    return normalized if parsed.scheme else "http://" + normalized

if __name__ == "__main__":
    seed_urls = [
        "http://example.com",
        "https://aws.amazon.com",
        "https://www.python.org",
        "https://docs.python.org/3/"
    ]
    normalized_seeds = [normalize_url(url) for url in seed_urls]
    master = MasterNode()
    master.start(normalized_seeds)
