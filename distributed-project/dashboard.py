import boto3
import json
import time
from datetime import datetime

# AWS setup
sqs = boto3.client("sqs", region_name="eu-north-1")
queue_url = "https://sqs.eu-north-1.amazonaws.com/543442417201/mycrawlerHeartbeat"

# State
crawlers = {}

print("🧭 Monitoring dashboard running... Ctrl+C to stop.\n")

try:
    while True:
        messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=10)
        if "Messages" in messages:
            for msg in messages["Messages"]:
                body = json.loads(msg["Body"])
                cid = body["crawler_id"]
                crawlers[cid] = {
                    "last_seen": datetime.fromtimestamp(body["timestamp"]).strftime("%H:%M:%S"),
                    "crawled": body["crawled"],
                    "uploaded": body["uploaded"],
                    "failed": body["failed"]
                }
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
        
        # Clear screen
        print("\033c", end="")
        print("📊 CRAWLER DASHBOARD\n")
        for cid, info in crawlers.items():
            print(f"{cid}")
            print(f"  ⏱️  Last Seen: {info['last_seen']}")
            print(f"  ✅ Crawled: {info['crawled']} | ☁️ Uploaded: {info['uploaded']} | ❌ Failed: {info['failed']}\n")

        print("🔁 Refreshing every 10 seconds...")
        time.sleep(10)

except KeyboardInterrupt:
    print("\n🛑 Exiting dashboard.")
