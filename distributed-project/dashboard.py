import boto3
import json
import time
from datetime import datetime, timedelta
import os
import sys
import argparse
from collections import deque
import colorama
from colorama import Fore, Back, Style
import threading

# Initialize colorama for cross-platform colored terminal output
colorama.init(autoreset=True)

# AWS setup - use explicit credentials and region to ensure connectivity
try:
    # Use standard boto3 credential resolution (environment vars, AWS config files, etc.)
    sqs = boto3.client("sqs", region_name="eu-north-1")
    s3 = boto3.client("s3", region_name="eu-north-1")
except Exception as e:
    print(f"Failed to initialize AWS clients: {e}")
    sys.exit(1)

# Queue URLs - double check that these match your actual SQS queue URLs
crawler_queue_url = "https://sqs.eu-north-1.amazonaws.com/543442417201/mycrawlerHeartbeat"
indexer_queue_url = "https://sqs.eu-north-1.amazonaws.com/543442417201/myindexerHeartbeat"
work_queue_url = "https://sqs.eu-north-1.amazonaws.com/543442417201/mycrawlerQueue"
BUCKET_NAME = "distributed-crawler-data"
BACKUP_BUCKET = "distributed-index-backups"

# Constants 
REFRESH_INTERVAL = 3  # seconds (reduced for faster updates)
HISTORY_LENGTH = 30    # Number of data points to track for trends
INACTIVE_THRESHOLD = 90  # Seconds after which a node is considered inactive
MAX_HEARTBEAT_AGE = 300  # Keep heartbeats in memory for this many seconds
INITIAL_LOAD_TIME = 15  # Initial time to collect heartbeats before starting display

# State
crawler_status = {}  # crawler_id -> {last_seen, crawled, uploaded, failed}
indexer_status = {}  # indexer_id -> {last_seen, indexed}
historical_data = {
    "crawl_rates": deque(maxlen=HISTORY_LENGTH),
    "index_rates": deque(maxlen=HISTORY_LENGTH),
    "error_rates": deque(maxlen=HISTORY_LENGTH),
    "timestamps": deque(maxlen=HISTORY_LENGTH),
}

# Node health status tracking
node_health = {
    "active_crawlers": 0,
    "inactive_crawlers": 0,
    "active_indexers": 0,
    "inactive_indexers": 0,
}

# Debug mode
debug_mode = False

# Parse command line arguments
parser = argparse.ArgumentParser(description="Distributed Web Crawler Monitoring Dashboard")
parser.add_argument("--refresh", type=int, default=REFRESH_INTERVAL, help="Dashboard refresh interval in seconds")
parser.add_argument("--log", action="store_true", help="Write dashboard data to log file")
parser.add_argument("--all", action="store_true", help="Show all heartbeats, even old ones")
parser.add_argument("--debug", action="store_true", help="Show debug information")
args = parser.parse_args()

if args.debug:
    debug_mode = True

# Thread control
running = True

# Setup logging if enabled
if args.log:
    import logging
    log_file = f"dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("Dashboard started")

def debug(message):
    """Print debug messages if debug mode is enabled"""
    if debug_mode:
        print(f"{Fore.YELLOW}[DEBUG] {message}{Style.RESET_ALL}")
        if args.log:
            logging.debug(message)

def fetch_queue_stats():
    """Get statistics about the work queue"""
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=work_queue_url,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )
        visible = int(response['Attributes']['ApproximateNumberOfMessages'])
        in_flight = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])
        return visible, in_flight
    except Exception as e:
        debug(f"Error getting queue stats: {e}")
        return 0, 0

def fetch_s3_stats():
    """Get statistics about S3 storage"""
    try:
        # Get crawled content stats
        content_response = s3.list_objects_v2(Bucket=BUCKET_NAME)
        content_count = content_response.get('KeyCount', 0)
        
        # Get index backup stats
        backup_response = s3.list_objects_v2(Bucket=BACKUP_BUCKET)
        backup_count = backup_response.get('KeyCount', 0)
        
        return content_count, backup_count
    except Exception as e:
        debug(f"Error getting S3 stats: {e}")
        return 0, 0

def calculate_rates():
    """Calculate performance rates based on collected data"""
    now = time.time()
    
    # Calculate crawler performance
    total_crawled = sum(info["crawled"] for info in crawler_status.values())
    total_failed = sum(info["failed"] for info in crawler_status.values())
    
    # Calculate indexer performance
    total_indexed = sum(info["indexed"] for info in indexer_status.values()) if indexer_status else 0
    
    # Track historical data for trends
    historical_data["crawl_rates"].append(total_crawled)
    historical_data["index_rates"].append(total_indexed)
    historical_data["error_rates"].append(total_failed)
    historical_data["timestamps"].append(now)
    
    # Calculate rates if we have enough data points
    if len(historical_data["timestamps"]) >= 2:
        time_diff = historical_data["timestamps"][-1] - historical_data["timestamps"][0]
        if time_diff > 0:
            crawl_diff = historical_data["crawl_rates"][-1] - historical_data["crawl_rates"][0]
            index_diff = historical_data["index_rates"][-1] - historical_data["index_rates"][0]
            error_diff = historical_data["error_rates"][-1] - historical_data["error_rates"][0]
            
            crawl_rate = crawl_diff / time_diff * 60  # per minute
            index_rate = index_diff / time_diff * 60  # per minute
            error_rate = error_diff / time_diff * 60  # per minute
            
            return crawl_rate, index_rate, error_rate
    
    return 0, 0, 0

def collect_initial_heartbeats():
    """Collect initial heartbeats to populate the dashboard at startup"""
    print(f"{Fore.CYAN}Collecting initial heartbeats for {INITIAL_LOAD_TIME} seconds...{Style.RESET_ALL}")
    start_time = time.time()
    
    # Continue collecting until timeout
    while time.time() - start_time < INITIAL_LOAD_TIME:
        crawler_count, _ = receive_and_process_crawler_heartbeats()
        indexer_count, _ = receive_and_process_indexer_heartbeats()
        
        if crawler_count > 0 or indexer_count > 0:
            print(f"Found: {crawler_count} crawler and {indexer_count} indexer heartbeats")
        
        # Quick pause between batches
        time.sleep(0.5)
    
    # After initial collection, show summary
    active_crawlers = sum(1 for _, info in crawler_status.items() 
                           if time.time() - info["timestamp"] <= INACTIVE_THRESHOLD)
    active_indexers = sum(1 for _, info in indexer_status.items() 
                           if time.time() - info["timestamp"] <= INACTIVE_THRESHOLD)
    
    print(f"{Fore.GREEN}Initial collection complete: Found {active_crawlers} active crawlers and {active_indexers} active indexers{Style.RESET_ALL}")
    time.sleep(1)  # Brief pause for user to read the message

def receive_and_process_crawler_heartbeats():
    """Receive and process crawler heartbeats, return count of processed messages"""
    heartbeats_found = 0
    deleted_count = 0
    
    try:
        # Get crawler heartbeats - use long polling
        messages = sqs.receive_message(
            QueueUrl=crawler_queue_url, 
            MaxNumberOfMessages=10,  # Get multiple messages
            WaitTimeSeconds=1,       # Short wait time
            VisibilityTimeout=5      # Short visibility timeout
        )
        
        if "Messages" in messages:
            debug(f"Received {len(messages['Messages'])} crawler heartbeat messages")
            
            for msg in messages["Messages"]:
                try:
                    body = json.loads(msg["Body"])
                    cid = body["crawler_id"]
                    
                    # Debug the raw heartbeat data
                    debug(f"Crawler heartbeat from {cid}: {json.dumps(body)}")
                    
                    # Update or add crawler status
                    crawler_status[cid] = {
                        "timestamp": body["timestamp"],
                        "last_seen": datetime.fromtimestamp(body["timestamp"]).strftime("%H:%M:%S"),
                        "crawled": body["crawled"],
                        "uploaded": body["uploaded"],
                        "failed": body["failed"],
                    }
                    
                    heartbeats_found += 1
                    
                    # Log if enabled
                    if args.log:
                        logging.info(f"Heartbeat from crawler: {cid}")
                except Exception as e:
                    debug(f"Error processing crawler heartbeat: {e}")
                    if args.log:
                        logging.error(f"Error processing crawler heartbeat: {e}")
                finally:
                    # Always delete the message
                    sqs.delete_message(
                        QueueUrl=crawler_queue_url, 
                        ReceiptHandle=msg["ReceiptHandle"]
                    )
                    deleted_count += 1
                    
        return heartbeats_found, deleted_count
                    
    except Exception as e:
        debug(f"Error in crawler heartbeat receiver: {e}")
        if args.log:
            logging.error(f"Error in crawler heartbeat receiver: {e}")
        return 0, 0

def receive_and_process_indexer_heartbeats():
    """Receive and process indexer heartbeats, return count of processed messages"""
    heartbeats_found = 0
    deleted_count = 0
    
    try:
        # Get indexer heartbeats - use long polling
        messages = sqs.receive_message(
            QueueUrl=indexer_queue_url, 
            MaxNumberOfMessages=10,  # Get multiple messages
            WaitTimeSeconds=1,       # Short wait time
            VisibilityTimeout=5      # Short visibility timeout
        )
        
        if "Messages" in messages:
            debug(f"Received {len(messages['Messages'])} indexer heartbeat messages")
            
            for msg in messages["Messages"]:
                try:
                    body = json.loads(msg["Body"])
                    iid = body["indexer_id"]
                    
                    # Debug the raw heartbeat data
                    debug(f"Indexer heartbeat from {iid}: {json.dumps(body)}")
                    
                    # Update or add indexer status
                    indexer_status[iid] = {
                        "timestamp": body["timestamp"],
                        "last_seen": datetime.fromtimestamp(body["timestamp"]).strftime("%H:%M:%S"),
                        "indexed": body["indexed"]
                    }
                    
                    heartbeats_found += 1
                    
                    # Log if enabled
                    if args.log:
                        logging.info(f"Heartbeat from indexer: {iid}")
                except Exception as e:
                    debug(f"Error processing indexer heartbeat: {e}")
                    if args.log:
                        logging.error(f"Error processing indexer heartbeat: {e}")
                finally:
                    # Always delete the message
                    sqs.delete_message(
                        QueueUrl=indexer_queue_url, 
                        ReceiptHandle=msg["ReceiptHandle"]
                    )
                    deleted_count += 1
                    
        return heartbeats_found, deleted_count
                    
    except Exception as e:
        debug(f"Error in indexer heartbeat receiver: {e}")
        if args.log:
            logging.error(f"Error in indexer heartbeat receiver: {e}")
        return 0, 0

def monitor_crawler_heartbeats():
    """Thread to continuously monitor crawler heartbeats"""
    debug(f"Starting crawler heartbeat monitor thread")
    
    while running:
        try:
            # Process crawler heartbeats
            heartbeats_found, deleted_count = receive_and_process_crawler_heartbeats()
            
            if heartbeats_found > 0:
                debug(f"Processed {heartbeats_found} crawler heartbeats (deleted {deleted_count})")
                
            # Clean up old heartbeats if not showing all
            if not args.all:
                now = time.time()
                before_count = len(crawler_status)
                for cid in list(crawler_status.keys()):
                    if now - crawler_status[cid]["timestamp"] > MAX_HEARTBEAT_AGE:
                        del crawler_status[cid]
                after_count = len(crawler_status)
                if before_count != after_count:
                    debug(f"Cleaned up {before_count - after_count} expired crawler heartbeats")
                        
            # Small sleep to avoid high CPU usage
            time.sleep(1)
            
        except Exception as e:
            debug(f"Error in crawler heartbeat monitor: {e}")
            time.sleep(5)  # Back off on errors

def monitor_indexer_heartbeats():
    """Thread to continuously monitor indexer heartbeats"""
    debug(f"Starting indexer heartbeat monitor thread")
    
    while running:
        try:
            # Process indexer heartbeats
            heartbeats_found, deleted_count = receive_and_process_indexer_heartbeats()
            
            if heartbeats_found > 0:
                debug(f"Processed {heartbeats_found} indexer heartbeats (deleted {deleted_count})")
                
            # Clean up old heartbeats if not showing all
            if not args.all:
                now = time.time()
                before_count = len(indexer_status)
                for iid in list(indexer_status.keys()):
                    if now - indexer_status[iid]["timestamp"] > MAX_HEARTBEAT_AGE:
                        del indexer_status[iid]
                after_count = len(indexer_status)
                if before_count != after_count:
                    debug(f"Cleaned up {before_count - after_count} expired indexer heartbeats")
                        
            # Small sleep to avoid high CPU usage
            time.sleep(1)
            
        except Exception as e:
            debug(f"Error in indexer heartbeat monitor: {e}")
            time.sleep(5)  # Back off on errors

def update_node_health():
    """Update node health status based on latest heartbeats"""
    now = time.time()
    active_crawlers = 0
    inactive_crawlers = 0
    
    for cid, info in crawler_status.items():
        if now - info["timestamp"] <= INACTIVE_THRESHOLD:
            active_crawlers += 1
            crawler_status[cid]["status"] = "ACTIVE"
        else:
            inactive_crawlers += 1
            crawler_status[cid]["status"] = "INACTIVE"
        
        # Calculate time since last heartbeat
        seconds_ago = int(now - info["timestamp"])
        crawler_status[cid]["last_seen"] = f"{info['last_seen']} ({seconds_ago}s ago)"
    
    active_indexers = 0
    inactive_indexers = 0
    
    for iid, info in indexer_status.items():
        if now - info["timestamp"] <= INACTIVE_THRESHOLD:
            active_indexers += 1
            indexer_status[iid]["status"] = "ACTIVE"
        else:
            inactive_indexers += 1
            indexer_status[iid]["status"] = "INACTIVE"
        
        # Calculate time since last heartbeat
        seconds_ago = int(now - info["timestamp"])
        indexer_status[iid]["last_seen"] = f"{info['last_seen']} ({seconds_ago}s ago)"
    
    # Update global health stats
    node_health["active_crawlers"] = active_crawlers
    node_health["inactive_crawlers"] = inactive_crawlers
    node_health["active_indexers"] = active_indexers
    node_health["inactive_indexers"] = inactive_indexers

def display_dashboard():
    """Display the dashboard with all system information"""
    global running
    
    try:
        while running:
            # Update node health status
            update_node_health()
            
            # Calculate performance metrics
            crawl_rate, index_rate, error_rate = calculate_rates()
            
            # Get queue and storage stats
            queue_visible, queue_in_flight = fetch_queue_stats()
            content_count, backup_count = fetch_s3_stats()

            # Clear screen 
            os.system('cls' if os.name == 'nt' else 'clear')
            
            # System summary with colors
            print(f"{Back.BLUE}{Fore.WHITE}" + "=" * 70 + Style.RESET_ALL)
            print(f"{Back.BLUE}{Fore.WHITE} 🌐 DISTRIBUTED WEB CRAWLER SYSTEM DASHBOARD {Style.RESET_ALL}")
            print(f"{Back.BLUE}{Fore.WHITE}" + "=" * 70 + Style.RESET_ALL)
            
            print(f"\n📊 {Fore.CYAN}SYSTEM OVERVIEW:{Style.RESET_ALL} {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Show active nodes with color based on status
            if node_health['active_crawlers'] > 0 or node_health['active_indexers'] > 0:
                print(f"  🖥️  {Fore.GREEN}Active Nodes: {node_health['active_crawlers']} crawlers, {node_health['active_indexers']} indexers{Style.RESET_ALL}")
            else:
                print(f"  🖥️  {Fore.YELLOW}Active Nodes: {node_health['active_crawlers']} crawlers, {node_health['active_indexers']} indexers{Style.RESET_ALL}")
            
            if node_health['inactive_crawlers'] > 0 or node_health['inactive_indexers'] > 0:
                print(f"  ⚠️  {Fore.RED}Inactive Nodes: {node_health['inactive_crawlers']} crawlers, {node_health['inactive_indexers']} indexers{Style.RESET_ALL}")
            else:
                print(f"  ⚠️  Inactive Nodes: {node_health['inactive_crawlers']} crawlers, {node_health['inactive_indexers']} indexers")
            
            print(f"  📑 Queue Status: {queue_visible} visible, {queue_in_flight} in processing")
            print(f"  📦 Storage: {content_count} files crawled, {backup_count} index backups")
            
            # Performance metrics with colors for error rate
            print(f"\n📈 {Fore.CYAN}PERFORMANCE METRICS (per minute):{Style.RESET_ALL}")
            print(f"  ⏩ Crawl Rate: {crawl_rate:.2f} pages/min")
            print(f"  🔍 Index Rate: {index_rate:.2f} pages/min")
            
            if error_rate > 0.5:
                print(f"  ❌ {Fore.RED}Error Rate: {error_rate:.2f} errors/min{Style.RESET_ALL}")
            else:
                print(f"  ❌ Error Rate: {error_rate:.2f} errors/min")
            
            # Crawler details with colors
            print(f"\n📊 {Fore.CYAN}CRAWLER NODES{Style.RESET_ALL}")
            print("-" * 70)
            
            if not crawler_status:
                print(f"  {Fore.YELLOW}No crawler nodes detected.{Style.RESET_ALL}")
            else:
                # Sort crawlers by status (active first) then by name
                sorted_crawlers = sorted(
                    crawler_status.items(), 
                    key=lambda x: (0 if x[1].get("status") == "ACTIVE" else 1, x[0])
                )
                
                for cid, info in sorted_crawlers:
                    if info.get("status") == "ACTIVE":
                        status_emoji = "🟢"
                        color = Fore.GREEN
                    else:
                        status_emoji = "🔴"
                        color = Fore.RED
                        
                    print(f"{color}{status_emoji} {cid}{Style.RESET_ALL}")
                    print(f"  ⏱️  Last Seen: {info['last_seen']}")
                    print(f"  ✅ Crawled: {info['crawled']} | ☁️ Uploaded: {info['uploaded']} | ❌ Failed: {info['failed']}")
                    print("-" * 70)

            # Indexer details with colors
            print(f"\n📦 {Fore.CYAN}INDEXER NODES{Style.RESET_ALL}")
            print("-" * 70)
            
            if not indexer_status:
                print(f"  {Fore.YELLOW}No indexer nodes detected.{Style.RESET_ALL}")
            else:
                # Sort indexers by status (active first) then by name
                sorted_indexers = sorted(
                    indexer_status.items(), 
                    key=lambda x: (0 if x[1].get("status") == "ACTIVE" else 1, x[0])
                )
                
                for iid, info in sorted_indexers:
                    if info.get("status") == "ACTIVE":
                        status_emoji = "🟢"
                        color = Fore.GREEN
                    else:
                        status_emoji = "🔴"
                        color = Fore.RED
                        
                    print(f"{color}{status_emoji} {iid}{Style.RESET_ALL}")
                    print(f"  ⏱️  Last Seen: {info['last_seen']}")
                    print(f"  📚 Indexed: {info['indexed']}")
                    print("-" * 70)

            # Footer with dashboard controls
            print(f"\n{Fore.CYAN}Dashboard Options:{Style.RESET_ALL}")
            if args.all:
                print(f"  📌 Showing all heartbeats, even old ones")
            else:
                print(f"  📌 Showing recent heartbeats only (last {MAX_HEARTBEAT_AGE}s)")
            print(f"  🔁 Refreshing every {args.refresh} seconds... (Press Ctrl+C to exit)")
            if debug_mode:
                print(f"  🔧 Debug mode enabled")
            
            # Wait for next refresh
            time.sleep(args.refresh)
            
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}🛑 Exiting dashboard.{Style.RESET_ALL}")
        running = False

if __name__ == "__main__":
    try:
        print(f"{Fore.GREEN}🧭 Monitoring dashboard starting...{Style.RESET_ALL}")
        
        if args.log:
            print(f"📝 Logging enabled - writing to {log_file}")
        
        # Load initial heartbeats
        collect_initial_heartbeats()
            
        # Start heartbeat monitoring threads
        crawler_thread = threading.Thread(target=monitor_crawler_heartbeats)
        indexer_thread = threading.Thread(target=monitor_indexer_heartbeats)
        
        crawler_thread.daemon = True
        indexer_thread.daemon = True
        
        crawler_thread.start()
        indexer_thread.start()
        
        # Start the dashboard display in the main thread
        display_dashboard()
        
    except Exception as e:
        print(f"Dashboard error: {e}")
        running = False
    finally:
        print("Shutting down...")
        running = False
        time.sleep(1)  # Give threads time to clean up
