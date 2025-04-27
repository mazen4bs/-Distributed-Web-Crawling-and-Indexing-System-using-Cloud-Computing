import boto3
import logging
from bs4 import BeautifulSoup
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SimpleIndexer:
    def __init__(self, bucket_name):
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name
        self.index = defaultdict(list)  # {keyword: [url1, url2]}
        
    def ingest_from_s3(self):
        """Fetch HTML files from S3 and build the index"""
        try:
            objects = self.s3.list_objects_v2(Bucket=self.bucket_name)['Contents']
            for obj in objects:
                file_key = obj['Key']
                html = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)['Body'].read().decode('utf-8')
                url = self._extract_url_from_filename(file_key)  # Or store metadata
                self._index_html(html, url)
            logging.info(f"✅ Indexed {len(objects)} pages")
        except Exception as e:
            logging.error(f"❌ S3 ingestion failed: {e}")

    def _extract_url_from_filename(self, filename):
        """Mock: Extract URL from filename (or use a metadata file)"""
        return f"http://{filename.replace('.html', '')}"  # Simplified

    def _index_html(self, html, url):
        """Extract text and add to index"""
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text().lower().split()  # Basic tokenization
        for word in set(text):  # Dedupe per page
            self.index[word].append(url)

    def search(self, keyword):
        """Exact match search"""
        return self.index.get(keyword.lower(), [])

if __name__ == '__main__':
    indexer = SimpleIndexer(bucket_name='distributed-crawler-data')
    
    # Step 1: Ingest data from S3
    indexer.ingest_from_s3()
    
    # Step 2: Interactive search
    while True:
        query = input("Enter keyword (or 'quit'): ").strip()
        if query == 'quit':
            break
        results = indexer.search(query)
        print(f"🔍 Found {len(results)} results for '{query}':")
        for url in results:
            print(f"→ {url}")