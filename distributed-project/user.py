import boto3
import json
import argparse
import time
import sys
import os
import subprocess
import colorama
import urllib.parse
from colorama import Fore, Style
import logging
from datetime import datetime
import socket
import importlib.util

# Initialize colorama for cross-platform colored terminal output
colorama.init(autoreset=True)

# Constants - SQS queue for direct submission to master node
CRAWLER_QUEUE_URL = "https://sqs.eu-north-1.amazonaws.com/543442417201/mycrawlerQueue"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("user.log")
    ]
)
logger = logging.getLogger(__name__)

# AWS setup
try:
    # Use standard boto3 credential resolution (environment vars, AWS config files, etc.)
    sqs = boto3.client("sqs", region_name="eu-north-1")
    s3 = boto3.client("s3", region_name="eu-north-1")
except Exception as e:
    logger.error(f"Failed to initialize AWS clients: {e}")
    print(f"{Fore.RED}Error: Failed to initialize AWS. Check your credentials and internet connection.{Style.RESET_ALL}")
    sys.exit(1)

def normalize_url(url):
    """Normalize URL to canonical form"""
    try:
        # Remove any surrounding quotes if present
        url = url.strip('"\'')
        
        # Add scheme if missing
        if not url.startswith('http://') and not url.startswith('https://'):
            url = 'http://' + url

        parsed = urllib.parse.urlparse(url)
        # Remove fragments and standardize scheme
        normalized = parsed._replace(fragment="").geturl()
        # Remove trailing slashes for consistency
        return normalized.rstrip("/")
    except Exception as e:
        logger.error(f"Error normalizing URL {url}: {e}")
        return url  # Return original on error

def send_urls_to_master(urls, depth_limit, restrict_domain=True):
    """Send URLs directly to the SQS queue that the master node monitors"""
    if not urls:
        print(f"{Fore.YELLOW}No URLs provided.{Style.RESET_ALL}")
        return
    
    print(f"{Fore.CYAN}Sending URLs to master node...{Style.RESET_ALL}")
    
    success_count = 0
    failed_urls = []
    
    for url in urls:
        # Normalize URL
        normalized_url = normalize_url(url.strip())
        if not normalized_url:
            continue
            
        try:
            # Format message the same way the master node expects it
            message = json.dumps({
            "url": normalized_url,
            "depth": 0,
            "depth_limit": depth_limit,
            "restrict_domain": restrict_domain
        })
            
            # Send directly to the SQS queue
            sqs.send_message(QueueUrl=CRAWLER_QUEUE_URL, MessageBody=message)
            success_count += 1
            print(f"{Fore.GREEN}✓ URL sent to master: {normalized_url}{Style.RESET_ALL}")
        except Exception as e:
            failed_urls.append(url)
            logger.error(f"Failed to send URL {url}: {e}")
            print(f"{Fore.RED}✗ Failed to send URL: {url} - {e}{Style.RESET_ALL}")
    
    return success_count

def launch_dashboard():
    """Launch the system dashboard in a new process with improved path detection"""
    try:
        print(f"{Fore.CYAN}Starting dashboard...{Style.RESET_ALL}")
        
        # Get the current directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        print(f"Looking for dashboard in: {current_dir}")
        
        # Try with capitalized name first (which we know exists based on your folder structure)
        dashboard_path = os.path.join(current_dir, "Dashboard.py")
        
        if not os.path.exists(dashboard_path):
            # Try lowercase as fallback
            dashboard_path = os.path.join(current_dir, "dashboard.py")
            
        if not os.path.exists(dashboard_path):
            print(f"{Fore.RED}Error: Dashboard.py not found in {current_dir}{Style.RESET_ALL}")
            return False
        
        print(f"Found dashboard at: {dashboard_path}")
        
        # Launch the dashboard in a new process
        cmd = [sys.executable, dashboard_path]
        print(f"Executing: {' '.join(cmd)}")
        subprocess.Popen(cmd)
        print(f"{Fore.GREEN}Dashboard launched. Press Ctrl+C in the dashboard window to exit when done.{Style.RESET_ALL}")
        return True
    except Exception as e:
        print(f"{Fore.RED}Error launching dashboard: {e}{Style.RESET_ALL}")
        return False

def launch_search():
    """Launch the search interface using the indexer's interactive_search function"""
    try:
        # Get the current directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Import indexer using importlib
        indexer_path = os.path.join(current_dir, "indexer.py")
        if not os.path.exists(indexer_path):
            print(f"{Fore.RED}Error: indexer.py not found in {current_dir}{Style.RESET_ALL}")
            return False
            
        spec = importlib.util.spec_from_file_location("indexer", indexer_path)
        indexer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(indexer)
        
        # Launch the interactive search from the indexer module
        print(f"{Fore.CYAN}Starting search interface from indexer module...{Style.RESET_ALL}")
        indexer.interactive_search()
        return True
    except ImportError as e:
        print(f"{Fore.RED}Error importing indexer module: {e}{Style.RESET_ALL}")
        return False
    except Exception as e:
        print(f"{Fore.RED}Error launching search interface: {e}{Style.RESET_ALL}")
        import traceback
        traceback.print_exc()
        return False

def main_menu():
    """Display main menu interface"""
    while True:
        print(f"\n{Fore.CYAN}=== Distributed Web Crawler System ==={Style.RESET_ALL}")
        print(f"1. Submit URLs for crawling")
        print(f"2. Launch dashboard")
        print(f"3. Search indexed content")
        print(f"4. Exit")
        
        try:
            choice = input(f"\n{Fore.GREEN}Choose an option (1-4): {Style.RESET_ALL}")
            
            if choice == '1':
                url_input = input(f"{Fore.GREEN}Enter URLs (comma-separated): {Style.RESET_ALL}")
                urls = [u.strip() for u in url_input.split(',') if u.strip()]
                
                depth_limit = int(input("Enter depth limit (e.g., 2): ") or "2")
                restrict_domain = input("Restrict to same domain? (y/n): ").lower().startswith("y")

                send_urls_to_master(urls, depth_limit, restrict_domain)

                
            elif choice == '2':
                launch_dashboard()
                
            elif choice == '3':
                launch_search()
                
            elif choice == '4':
                print(f"{Fore.CYAN}Exiting. Goodbye!{Style.RESET_ALL}")
                break
                
            else:
                print(f"{Fore.YELLOW}Invalid option. Please choose 1-4.{Style.RESET_ALL}")
                
        except KeyboardInterrupt:
            print(f"\n{Fore.CYAN}Program interrupted. Exiting.{Style.RESET_ALL}")
            break
        except Exception as e:
            print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")

def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description="User interface for Distributed Web Crawler System")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Submit command
    submit_parser = subparsers.add_parser("submit", help="Submit URLs for crawling")
    submit_parser.add_argument("urls", nargs="+", help="URLs to submit for crawling")
    
    # Dashboard command
    subparsers.add_parser("dashboard", help="Launch the system dashboard")
    
    # Search command
    subparsers.add_parser("search", help="Search indexed content")
    
    return parser.parse_args()

if __name__ == "__main__":
    # Check if command line arguments were provided
    if len(sys.argv) > 1:
        args = parse_arguments()
        
        if args.command == "submit":
            send_urls_to_master(args.urls)
        elif args.command == "dashboard":
            launch_dashboard()
        elif args.command == "search":
            launch_search()
    else:
        # No command line args, launch interactive menu
        main_menu()