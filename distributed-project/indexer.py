# enhanced_indexer.py

import os
import boto3
import logging
from bs4 import BeautifulSoup
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.analysis import StemmingAnalyzer
from whoosh.qparser import MultifieldParser

# Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
BUCKET_NAME = "distributed-crawler-data"
INDEX_DIR = "indexdir"

# Initialize AWS S3 client
s3 = boto3.client("s3")

# Define the schema for the Whoosh index
schema = Schema(
    url=ID(stored=True, unique=True),
    title=TEXT(stored=True, analyzer=StemmingAnalyzer()),
    content=TEXT(stored=True, analyzer=StemmingAnalyzer())
)

# Create index directory if not exists
if not os.path.exists(INDEX_DIR):
    os.mkdir(INDEX_DIR)
    ix = create_in(INDEX_DIR, schema)
else:
    ix = open_dir(INDEX_DIR)

def extract_text_from_html(html):
    """Extract visible text and title from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string.strip() if soup.title and soup.title.string else "No Title"
    for tag in soup(["script", "style"]):
        tag.decompose()
    content = soup.get_text(separator=" ", strip=True)
    return title, content

def ingest_from_s3():
    """Fetch HTML files from S3 and index them."""
    try:
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME).get("Contents", [])
        writer = ix.writer()
        for obj in objects:
            key = obj["Key"]
            response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            html = response["Body"].read().decode("utf-8")
            url = response["Metadata"].get("original-url", key)

            title, content = extract_text_from_html(html)

            writer.update_document(
                url=url,
                title=title,
                content=content
            )
            logging.info(f"✅ Indexed: {url}")
        writer.commit()
        logging.info(f"📦 Finished indexing {len(objects)} pages")
    except Exception as e:
        logging.error(f"❌ Failed to ingest and index: {e}")

def interactive_search():
    """Allow user to perform searches."""
    with ix.searcher() as searcher:
        parser = MultifieldParser(["title", "content"], schema=ix.schema)
        while True:
            query_str = input("🔍 Enter search query (or 'quit'): ").strip()
            if query_str.lower() == "quit":
                break
            try:
                query = parser.parse(query_str)
                results = searcher.search(query, limit=10)
                print(f"\n🔎 Found {len(results)} result(s):")
                for hit in results:
                    print(f"→ {hit['url']} | Title: {hit['title']}")
                print()
            except Exception as e:
                print(f"❌ Invalid query: {e}")

if __name__ == "__main__":
    ingest_from_s3()
    interactive_search()
