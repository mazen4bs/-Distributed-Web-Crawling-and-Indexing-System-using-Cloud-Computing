import boto3
import json
import time
from datetime import datetime

# AWS setup
sqs = boto3.client("sqs", region_name="eu-north-1")
crawler_queue_url = "https://sqs.eu-north-1.amazonaws.com/543442417201/mycrawlerHeartbeat"
indexer_queue_url = "https://sqs.eu-north-1.amazonaws.com/543442417201/myindexerHeartbeat"

# State
crawlers = {}
indexers = {}

print("🧭 Monitoring dashboard running... Ctrl+C to stop.\n")

try:
    while True:
        # Fetch crawler heartbeats
        crawler_messages = sqs.receive_message(QueueUrl=crawler_queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        if "Messages" in crawler_messages:
            for msg in crawler_messages["Messages"]:
                body = json.loads(msg["Body"])
                cid = body["crawler_id"]
                crawlers[cid] = {
                    "last_seen": datetime.fromtimestamp(body["timestamp"]).strftime("%H:%M:%S"),
                    "crawled": body["crawled"],
                    "uploaded": body["uploaded"],
                    "failed": body["failed"]
                }
                sqs.delete_message(QueueUrl=crawler_queue_url, ReceiptHandle=msg["ReceiptHandle"])

        # Fetch indexer heartbeats
        indexer_messages = sqs.receive_message(QueueUrl=indexer_queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        if "Messages" in indexer_messages:
            for msg in indexer_messages["Messages"]:
                body = json.loads(msg["Body"])
                iid = body["indexer_id"]
                indexers[iid] = {
                    "last_seen": datetime.fromtimestamp(body["timestamp"]).strftime("%H:%M:%S"),
                    "indexed": body["indexed"]
                }
                sqs.delete_message(QueueUrl=indexer_queue_url, ReceiptHandle=msg["ReceiptHandle"])

        # Clear screen and display dashboard
        print("\033c", end="")  # ANSI clear screen
        print("📊 CRAWLER DASHBOARD\n")
        for cid, info in crawlers.items():
            print(f"{cid}")
            print(f"  ⏱️  Last Seen: {info['last_seen']}")
            print(f"  ✅ Crawled: {info['crawled']} | ☁️ Uploaded: {info['uploaded']} | ❌ Failed: {info['failed']}\n")

        print("📦 INDEXER DASHBOARD\n")
        for iid, info in indexers.items():
            print(f"{iid}")
            print(f"  ⏱️  Last Seen: {info['last_seen']}")
            print(f"  📚 Indexed: {info['indexed']}\n")

        print("🔁 Refreshing every 10 seconds...")
        time.sleep(10)

except KeyboardInterrupt:
    print("\n🛑 Exiting dashboard.")
