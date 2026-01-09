#!/usr/bin/env python3
"""
SSRN Paper Batch Download Script
Reads URL list from ruotong.json and downloads all paper PDFs to specified folder
"""

import json
import os
import re
import time
import requests
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup

# Configuration
OUTPUT_DIR = "ssrn_papers"
FAILED_LOG_FILE = "failed_downloads.json"  # Failed download log file
DELAY_BETWEEN_REQUESTS = 1  # Delay between requests (seconds) to avoid rate limiting
MAX_RETRIES = 3  # Maximum retry attempts

# Proxy configuration (uncomment and fill if VPN/proxy is needed)
# Example format:
# PROXIES = {
#     'http': 'http://127.0.0.1:7890',  # HTTP proxy address
#     'https': 'http://127.0.0.1:7890',  # HTTPS proxy address
# }
# Or use SOCKS5 proxy:
# PROXIES = {
#     'http': 'socks5://127.0.0.1:1080',
#     'https': 'socks5://127.0.0.1:1080',
# }
PROXIES = None  # Set to None when not using proxy

def extract_abstract_id(url):
    """Extract abstract_id from SSRN URL"""
    match = re.search(r'abstract_id=(\d+)', url)
    if match:
        return match.group(1)
    return None

def get_download_url(abstract_id):
    """Construct PDF download URL"""
    # Download link format:
    # Delivery.cfm/{abstract_id}.pdf?abstractid={abstract_id}&mirid=1
    base_url = "https://papers.ssrn.com/sol3"
    download_url = f"{base_url}/Delivery.cfm/{abstract_id}.pdf?abstractid={abstract_id}&mirid=1"
    return download_url

def download_pdf(url, output_path, abstract_id):
    """Download PDF file, returns (success_flag, error_message)"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/pdf,application/octet-stream,*/*',
        'Referer': f'https://papers.ssrn.com/sol3/papers.cfm?abstract_id={abstract_id}'
    }
    
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, stream=True, timeout=30, proxies=PROXIES)
            response.raise_for_status()
            
            # Check if response is PDF file
            content_type = response.headers.get('Content-Type', '')
            if 'pdf' not in content_type.lower() and not url.endswith('.pdf'):
                # If not PDF, try to extract download link from page
                print(f"  [WARNING] Direct link is not PDF, attempting to parse page...")
                return download_from_page(abstract_id, output_path)
            
            # Save file
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            file_size = os.path.getsize(output_path)
            if file_size > 0:
                print(f"  [SUCCESS] Download successful ({file_size / 1024:.1f} KB)")
                return (True, None)
            else:
                error_msg = "File size is 0"
                print(f"  [ERROR] {error_msg}")
                return (False, error_msg)
                
        except requests.exceptions.RequestException as e:
            last_error = str(e)
            print(f"  [WARNING] Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
            else:
                # Last attempt, parse from page
                print(f"  [RETRY] Attempting to parse download link from page...")
                result = download_from_page(abstract_id, output_path)
                if not result[0]:
                    return (False, f"Direct download failed: {last_error}; Page parsing also failed: {result[1]}")
                return result
    
    return (False, f"All retry attempts failed: {last_error}")

def download_from_page(abstract_id, output_path):
    """Parse and download PDF from SSRN page, returns (success_flag, error_message)"""
    page_url = f"https://papers.ssrn.com/sol3/papers.cfm?abstract_id={abstract_id}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }
    
    try:
        response = requests.get(page_url, headers=headers, timeout=30, proxies=PROXIES)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find download link
        # <a href="Delivery.cfm/4517697.pdf?abstractid=4517697&amp;mirid=1" class="button-link primary">
        download_link = None
        
        # Method 1: Find link with data-abstract-id attribute
        link = soup.find('a', {'data-abstract-id': abstract_id})
        if link and link.get('href'):
            download_link = link['href']
        else:
            # Method 2: Find link containing "Download This Paper" text
            link = soup.find('a', string=re.compile('Download This Paper', re.I))
            if link and link.get('href'):
                download_link = link['href']
            else:
                # Method 3: Find link with class containing "button-link primary"
                link = soup.find('a', class_=re.compile('button-link.*primary'))
                if link and link.get('href'):
                    download_link = link['href']
        
        if download_link:
            # Handle relative URL
            if download_link.startswith('/'):
                download_url = f"https://papers.ssrn.com{download_link}"
            elif download_link.startswith('Delivery.cfm'):
                download_url = f"https://papers.ssrn.com/sol3/{download_link}"
            else:
                download_url = download_link
            
            # Download PDF
            pdf_headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/pdf,application/octet-stream,*/*',
                'Referer': page_url
            }
            
            response = requests.get(download_url, headers=pdf_headers, stream=True, timeout=30, proxies=PROXIES)
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            file_size = os.path.getsize(output_path)
            if file_size > 0:
                print(f"  [SUCCESS] Download from page successful ({file_size / 1024:.1f} KB)")
                return (True, None)
            else:
                error_msg = "File downloaded from page has size 0"
                print(f"  [ERROR] {error_msg}")
                return (False, error_msg)
        else:
            error_msg = "Unable to find download link in page"
            print(f"  [ERROR] {error_msg}")
            return (False, error_msg)
            
    except requests.exceptions.RequestException as e:
        error_msg = f"Page request failed: {str(e)}"
        print(f"  [ERROR] {error_msg}")
        return (False, error_msg)
    except Exception as e:
        error_msg = f"Download from page failed: {str(e)}"
        print(f"  [ERROR] {error_msg}")
        return (False, error_msg)

def sanitize_filename(filename):
    """Sanitize filename by removing illegal characters"""
    # Remove or replace illegal characters
    illegal_chars = '<>:"/\\|?*'
    for char in illegal_chars:
        filename = filename.replace(char, '_')
    # Limit filename length
    if len(filename) > 200:
        filename = filename[:200]
    return filename

def main():
    # Check proxy configuration
    if PROXIES:
        print(f"[PROXY] Using proxy: {PROXIES.get('https', PROXIES.get('http', 'N/A'))}")
    else:
        print("[INFO] Proxy not configured (configure PROXIES in script if access is blocked)")
    
    # Read JSON file
    json_path = Path("ruotong.json")
    if not json_path.exists():
        print(f"[ERROR] File not found: {json_path}")
        return
    
    print(f"[INFO] Reading {json_path}...")
    with open(json_path, 'r', encoding='utf-8') as f:
        urls = json.load(f)
    
    print(f"[INFO] Found {len(urls)} URLs")
    
    # Create output directory
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(exist_ok=True)
    print(f"[INFO] Output directory: {output_dir.absolute()}\n")
    
    # Statistics
    success_count = 0
    fail_count = 0
    skip_count = 0
    failed_downloads = []  # Record failed downloads
    
    # Download each paper
    for i, url in enumerate(urls, 1):
        abstract_id = extract_abstract_id(url)
        if not abstract_id:
            error_msg = "Unable to extract abstract_id from URL"
            print(f"[{i}/{len(urls)}] [ERROR] {error_msg}: {url}")
            fail_count += 1
            failed_downloads.append({
                'url': url,
                'abstract_id': None,
                'error': error_msg,
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            })
            continue
        
        # Check if file already exists
        output_path = output_dir / f"{abstract_id}.pdf"
        if output_path.exists():
            print(f"[{i}/{len(urls)}] [SKIP] Skipping {abstract_id} (file already exists)")
            skip_count += 1
            continue
        
        print(f"[{i}/{len(urls)}] [DOWNLOAD] Downloading {abstract_id}...")
        
        # Try direct download
        download_url = get_download_url(abstract_id)
        success, error_msg = download_pdf(download_url, output_path, abstract_id)
        
        if success:
            success_count += 1
        else:
            fail_count += 1
            # Delete failed file
            if output_path.exists():
                output_path.unlink()
            # Record failure information
            failed_downloads.append({
                'url': url,
                'abstract_id': abstract_id,
                'error': error_msg or "Unknown error",
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # Delay to avoid rate limiting
        if i < len(urls):
            time.sleep(DELAY_BETWEEN_REQUESTS)
    
    # Save failed downloads to file
    if failed_downloads:
        log_path = Path(FAILED_LOG_FILE)
        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(failed_downloads, f, indent=2, ensure_ascii=False)
        print(f"\n[INFO] Failed downloads log saved to: {log_path.absolute()}")
    
    # Print statistics
    print("\n" + "="*50)
    print("[STATISTICS] Download Statistics:")
    print(f"  [SUCCESS] Successful: {success_count}")
    print(f"  [SKIP] Skipped: {skip_count}")
    print(f"  [FAILED] Failed: {fail_count}")
    print(f"  [OUTPUT] Output directory: {output_dir.absolute()}")
    if failed_downloads:
        print(f"  [LOG] Failed downloads log: {FAILED_LOG_FILE}")
        print(f"\nFailed URL list:")
        for item in failed_downloads[:10]:  # Show only first 10
            print(f"    - {item['url']} ({item['error']})")
        if len(failed_downloads) > 10:
            print(f"    ... {len(failed_downloads) - 10} more failed records, see {FAILED_LOG_FILE}")
    print("="*50)

if __name__ == "__main__":
    main()

