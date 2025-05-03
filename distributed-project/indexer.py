import os
import boto3
import logging
from bs4 import BeautifulSoup
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.analysis import StemmingAnalyzer
from whoosh.qparser import MultifieldParser
from whoosh.qparser import MultifieldParser, QueryParser, OrGroup, AndGroup
from whoosh import qparser
from datetime import datetime
from whoosh.qparser import OrGroup
from whoosh.qparser import MultifieldParser, OrGroup, OperatorsPlugin
import tarfile
import socket
import json
import time


# Constants
BUCKET_NAME = "distributed-crawler-data"
INDEX_DIR = "indexdir"
BACKUP_BUCKET = "distributed-index-backups"
LOG_FILE = "indexed_files.log"
INDEXER_ID = socket.gethostname()
HEARTBEAT_QUEUE_URL = "https://sqs.eu-north-1.amazonaws.com/543442417201/myindexerHeartbeat"
CRAWLER_HEARTBEAT_QUEUE_URL = "https://sqs.eu-north-1.amazonaws.com/543442417201/mycrawlerHeartbeat"

# AWS clients
s3 = boto3.client("s3", region_name="eu-north-1")
sqs = boto3.client("sqs", region_name="eu-north-1")

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Whoosh schema
def create_schema():
    return Schema(
        url=ID(stored=True, unique=True),
        title=TEXT(stored=True, analyzer=StemmingAnalyzer()),
        content=TEXT(stored=True, analyzer=StemmingAnalyzer())
    )

# Ensure indexdir exists
schema = create_schema()
if not os.path.exists(INDEX_DIR):
    os.mkdir(INDEX_DIR)
    ix = create_in(INDEX_DIR, schema)
else:
    ix = open_dir(INDEX_DIR)

# Load already indexed keys
def load_indexed_keys():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            return set(line.strip() for line in f.readlines())
    return set()

def mark_as_indexed(key):
    with open(LOG_FILE, "a") as f:
        f.write(key + "\n")

def extract_text_from_html(html, url="unknown"):
    soup = BeautifulSoup(html, "html.parser")
    title = "No Title"
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
    else:
        h1 = soup.find("h1")
        if h1 and h1.string:
            title = h1.string.strip()
        else:
            logging.warning(f"⚠️ No title found for {url}")

    for tag in soup(["script", "style"]):
        tag.decompose()
    content = soup.get_text(separator=" ", strip=True)
    return title, content

def send_indexer_heartbeat(indexed_count):
    try:
        heartbeat = {
            "indexer_id": INDEXER_ID,
            "timestamp": time.time(),
            "status": "alive",
            "indexed": indexed_count
        }
        sqs.send_message(
            QueueUrl=HEARTBEAT_QUEUE_URL,
            MessageBody=json.dumps(heartbeat)
        )
        logging.info("💓 Indexer heartbeat sent")
    except Exception as e:
        logging.error(f"❌ Failed to send indexer heartbeat: {e}")

def ingest_from_s3():
    indexed_count = 0
    already_indexed = load_indexed_keys()

    try:
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME).get("Contents", [])
        writer = ix.writer()
        for obj in objects:
            key = obj["Key"]
            if key in already_indexed:
                logging.info(f"⏩ Skipping already indexed: {key}")
                continue

            try:
                response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                html = response["Body"].read().decode("utf-8")
                url = response["Metadata"].get("original-url", key)
                title, content = extract_text_from_html(html, url)
                writer.update_document(url=url, title=title, content=content)

                indexed_count += 1
                mark_as_indexed(key)
                logging.info(f"✅ Indexed: {url} | Title: {title}")
            except Exception as e:
                logging.error(f"❌ Failed to process {key}: {e}")

        writer.commit()
        logging.info(f"📦 Total indexed this session: {indexed_count}")
        send_indexer_heartbeat(indexed_count)

    except Exception as e:
        logging.error(f"❌ Indexing failed: {e}")

def backup_indexdir_to_s3():
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    archive_name = f"indexdir_backup_{timestamp}.tar.gz"
    try:
        with tarfile.open(archive_name, "w:gz") as tar:
            tar.add(INDEX_DIR, arcname=os.path.basename(INDEX_DIR))

        s3.upload_file(archive_name, BACKUP_BUCKET, archive_name)
        logging.info(f"☁️ Index backup uploaded to S3: {archive_name}")
        os.remove(archive_name)
    except Exception as e:
        logging.error(f"❌ Failed to upload index backup: {e}")

def show_status():
    try:
        print("\n📡 CRAWLER STATUS:")
        crawler_msgs = sqs.receive_message(
            QueueUrl=CRAWLER_HEARTBEAT_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1
        )
        if "Messages" in crawler_msgs:
            for msg in crawler_msgs["Messages"]:
                body = json.loads(msg["Body"])
                print(f"🖥️  {body['crawler_id']}")
                print(f"   ⏱️ Last Seen: {datetime.fromtimestamp(body['timestamp']).strftime('%H:%M:%S')}")
                print(f"   ✅ Crawled: {body['crawled']} | ☁️ Uploaded: {body['uploaded']} | ❌ Failed: {body['failed']}")
                sqs.delete_message(QueueUrl=CRAWLER_HEARTBEAT_QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])
        else:
            print("No active crawler heartbeat messages.")

        print("\n🧠 INDEXER STATUS:")
        indexer_msgs = sqs.receive_message(
            QueueUrl=HEARTBEAT_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1
        )
        if "Messages" in indexer_msgs:
            for msg in indexer_msgs["Messages"]:
                body = json.loads(msg["Body"])
                print(f"🖥️  {body['indexer_id']}")
                print(f"   ⏱️ Last Seen: {datetime.fromtimestamp(body['timestamp']).strftime('%H:%M:%S')}")
                print(f"   🗂️ Indexed: {body['indexed']}")
                sqs.delete_message(QueueUrl=HEARTBEAT_QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])
        else:
            print("No active indexer heartbeat messages.")
        print()
    except Exception as e:
        print(f"❌ Failed to fetch status: {e}")


from whoosh.qparser import MultifieldParser, OrGroup, OperatorsPlugin

def interactive_search():
    print("\n🔎 Welcome to the Distributed Web Search Engine")
    print("Type a keyword or phrase, or boolean search (e.g., AI AND python).")
    print("Type 'status' to check crawler/indexer health.")
    print("Type 'quit' to exit.\n")

    try:
        with ix.searcher() as searcher:
            parser = MultifieldParser(["title", "content"], schema=ix.schema, group=OrGroup.factory(0.9))
            parser.add_plugin(OperatorsPlugin())  # ✅ Enables AND, OR, NOT
            while True:
                try:
                    query_str = input("Search > ").strip()
                except KeyboardInterrupt:
                    print("\n🛑 Interrupted by user. Exiting gracefully...")
                    break

                if query_str.lower() == "quit":
                    print("👋 Goodbye!")
                    break
                elif query_str.lower() == "status":
                    show_status()
                    continue
                try:
                    query = parser.parse(query_str)
                    results = searcher.search(query, limit=10)
                    if results:
                        print(f"\n🔍 Found {len(results)} result(s):\n")
                        for i, hit in enumerate(results, 1):
                            print(f"{i}. {hit['title']}\n   → {hit['url']}\n")
                    else:
                        print("⚠️ No results found.\n")
                except Exception as e:
                    print(f"❌ Search error: {e}")
    except Exception as e:
        print(f"❌ Fatal error: {e}")


if __name__ == "__main__":
    ingest_from_s3()
    backup_indexdir_to_s3()
    interactive_search()
