# -Distributed-Web-Crawling-and-Indexing-System-using-Cloud-Computing

#  Distributed Web Crawler System

This is a distributed cloud-based web crawling and indexing system using AWS and Python. It allows users to submit URLs, crawl and store web pages, index them, and perform keyword or boolean searches across the indexed data and allows users to monitore the nodes status.

---

##  Features

-  Multi-node architecture (Crawler, Indexer)
-  Robots.txt compliance with crawl delay handling
-  Fault tolerance: heartbeat + task re-queuing + node failure
-  Persistent storage via AWS S3
-  Search engine powered by Whoosh
-  User CLI for interaction, dashboard, and searching
-  Depth-limited and domain-restricted crawling
-  Live monitoring dashboard with crawler/indexer status

---

##  System Architecture

### Components:

- **Master Node**: Distributes URLs to crawl, monitors crawler/indexer heartbeats, handles re-queuing, and collects stats.
- **Crawler Nodes** (Multiple): Fetch pages, parse content, respect robots.txt, extract and queue links, and upload HTML to S3.
- **Indexer Nodes** (Multiple): Read HTML files from S3, extract content, build a local Whoosh index, and respond to search queries.
- **User Interface**: Allows users to submit URLs, view dashboard, and search indexed data.

### AWS Services:

- **SQS**:
  - `mycrawlerQueue`: URL distribution
  - `mycrawlerHeartbeat`: Crawler status
  - `myindexerHeartbeat`: Indexer status
- **S3**:
  - `distributed-crawler-data`: Stores crawled HTML
  - `distributed-index-backups`: Stores index backups

---

##  Setup Instructions

> Instances are using Python 3, Boto3, BeautifulSoup, Whoosh, etc. and virual enviroment for each node virtual environments for each role.
> Master source venv/bin/activate
> Crawler source myenv/bin/activate
> indexer source myenv/bin/activate

### to start user
open bash then copy and paste this lines:
ssh -i /e/Programs/Downloads/123.pem ubuntu@51.21.25.188
source myenv/bin/activate
python3 user.py
Note:nodes must be running on the cloud platform to run the user.  
