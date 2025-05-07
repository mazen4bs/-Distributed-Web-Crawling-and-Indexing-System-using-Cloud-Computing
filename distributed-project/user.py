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

# Import the search functionality directly from the indexer
try:
    from indexer import interactive_search
except ImportError:
    pass  # We'll handle this gracefully later

# Initialize colorama for cross-platform colored terminal output
colorama.init(autoreset=True)

# Constants - Use master node directly instead of the queue
MASTER_HOST = "localhost"  # Change this to your master node's hostname/IP if needed
MASTER_PORT = 8000  # This will be used for direct communication with the master

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
    s3 = boto3.client("s3", region_name="eu-north-1")
except Exception as e:
    logger.error(f"Failed to initialize AWS clients: {e}")
    print(f"{Fore.RED}Error: Failed to initialize AWS. Check your credentials and internet connection.{Style.RESET_ALL}")
    sys.exit(1)

def normalize_url(url):
    """Normalize URL to canonical form"""
    try:
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

def send_urls_to_master(urls):
    """Send URLs to the master node for processing"""
    import socket
    import json
    
    if not urls:
        print(f"{Fore.YELLOW}No URLs provided.{Style.RESET_ALL}")
        return
    
    print(f"{Fore.CYAN}Sending URLs to master node...{Style.RESET_ALL}")
    
    success_count = 0
    failed_urls = []
    
    # Import the MasterNode class from master_node module to use its functionality
    try:
        from master_node import MasterNode, normalize_url
        
        # Create a temporary instance of MasterNode to use its add_urls_to_queue method
        master = MasterNode()
        
        # Normalize URLs first
        normalized_urls = [normalize_url(url.strip()) for url in urls if url.strip()]
        
        # Add URLs to master's queue
        master.add_urls_to_queue(normalized_urls)
        
        success_count = len(normalized_urls)
        
        for url in normalized_urls:
            print(f"{Fore.GREEN}✓ URL sent to master: {url}{Style.RESET_ALL}")
            
    except ImportError:
        print(f"{Fore.RED}Error: Could not import MasterNode. Make sure master_node.py is in the same directory.{Style.RESET_ALL}")
        return 0
    except Exception as e:
        failed_urls = urls
        logger.error(f"Failed to send URLs to master: {e}")
        print(f"{Fore.RED}✗ Failed to send URLs to master: {e}{Style.RESET_ALL}")
        
    return success_count

def launch_dashboard():
    """Launch the system dashboard in a new process"""
    try:
        print(f"{Fore.CYAN}Starting dashboard...{Style.RESET_ALL}")
        # Get the path to the dashboard.py file
        dashboard_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.py")
        
        if not os.path.exists(dashboard_path):
            # Try with capitalized name
            dashboard_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Dashboard.py")
            if not os.path.exists(dashboard_path):
                print(f"{Fore.RED}Error: Dashboard.py not found{Style.RESET_ALL}")
                return False
        
        # Launch the dashboard in a new process
        subprocess.Popen([sys.executable, dashboard_path])
        print(f"{Fore.GREEN}Dashboard launched. Press Ctrl+C in the dashboard window to exit when done.{Style.RESET_ALL}")
        return True
    except Exception as e:
        print(f"{Fore.RED}Error launching dashboard: {e}{Style.RESET_ALL}")
        return False

def launch_search():
    """Launch the search interface using the indexer's interactive_search function"""
    try:
        # Check if we successfully imported the function
        if 'interactive_search' not in globals():
            from indexer import interactive_search
            
        # Launch the interactive search from the indexer module
        print(f"{Fore.CYAN}Starting search interface from indexer module...{Style.RESET_ALL}")
        interactive_search()
        return True
    except ImportError:
        print(f"{Fore.RED}Error: Could not import search function from indexer.py. Make sure it's in the same directory.{Style.RESET_ALL}")
        return False
    except Exception as e:
        print(f"{Fore.RED}Error launching search interface: {e}{Style.RESET_ALL}")
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
                send_urls_to_master(urls)
                
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