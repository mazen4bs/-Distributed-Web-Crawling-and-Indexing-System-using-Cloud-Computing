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

# AWS S3 client
s3 = boto3.client("s3")

# Define Whoosh schema
schema = Schema(
    url=ID(stored=True, unique=True),
    title=TEXT(stored=True, analyzer=StemmingAnalyzer()),
    content=TEXT(stored=True, analyzer=StemmingAnalyzer())
)

# Create or open the index
if not os.path.exists(INDEX_DIR):
    os.mkdir(INDEX_DIR)
    ix = create_in(INDEX_DIR, schema)
else:
    ix = open_dir(INDEX_DIR)

def extract_text_from_html(html, url="unknown"):
    """Extract page title and visible content text."""
    soup = BeautifulSoup(html, "html.parser")

    # Title fallback: use <title> or fallback to first <h1>
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
    else:
        h1 = soup.find('h1')
        if h1 and h1.string:
            title = h1.string.strip()
        else:
            title = "No Title"
            logging.warning(f"⚠️ No valid title found for: {url}")

    for tag in soup(["script", "style"]):
        tag.decompose()

    content = soup.get_text(separator=" ", strip=True)
    return title, content

def ingest_from_s3():
    """Download HTML files from S3 and index them."""
    try:
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME).get("Contents", [])
        writer = ix.writer()
        for obj in objects:
            key = obj["Key"]
            response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            html = response["Body"].read().decode("utf-8")
            url = response["Metadata"].get("original-url", key)

            title, content = extract_text_from_html(html, url)

            writer.update_document(
                url=url,
                title=title,
                content=content
            )
            logging.info(f"✅ Indexed: {url} | Title: {title}")
        writer.commit()
        logging.info(f"📦 Finished indexing {len(objects)} pages")
    except Exception as e:
        logging.error(f"❌ Indexing failed: {e}")

def interactive_search():
    """Search interface with instructions."""
    print("\n🔎 Welcome to the Distributed Web Search Engine")
    print("Enter a keyword, phrase (in quotes), or boolean query (e.g., python AND aws)")
    print("Type 'quit' to exit.\n")

    with ix.searcher() as searcher:
        parser = MultifieldParser(["title", "content"], schema=ix.schema)
        while True:
            query_str = input("Search > ").strip()
            if query_str.lower() == "quit":
                print("👋 Exiting search engine. Goodbye!")
                break
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
                print(f"❌ Invalid query: {e}\n")

if __name__ == "__main__":
    ingest_from_s3()
    interactive_search()
