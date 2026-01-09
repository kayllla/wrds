#!/usr/bin/env python3
"""
SSRN 论文批量下载脚本
从 ruotong.json 读取 URL 列表，下载所有论文 PDF 到指定文件夹
"""

import json
import os
import re
import time
import requests
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup

# 配置
OUTPUT_DIR = "ssrn_papers"
FAILED_LOG_FILE = "failed_downloads.json"  # 失败记录文件
DELAY_BETWEEN_REQUESTS = 1  # 请求之间的延迟（秒），避免请求过快
MAX_RETRIES = 3  # 最大重试次数

# 代理配置（如果需要使用 VPN/代理，取消注释并填写）
# 格式示例：
# PROXIES = {
#     'http': 'http://127.0.0.1:7890',  # HTTP 代理地址
#     'https': 'http://127.0.0.1:7890',  # HTTPS 代理地址
# }
# 或者使用 SOCKS5 代理：
# PROXIES = {
#     'http': 'socks5://127.0.0.1:1080',
#     'https': 'socks5://127.0.0.1:1080',
# }
PROXIES = None  # 不使用代理时设为 None

def extract_abstract_id(url):
    """从 SSRN URL 中提取 abstract_id"""
    match = re.search(r'abstract_id=(\d+)', url)
    if match:
        return match.group(1)
    return None

def get_download_url(abstract_id):
    """构造 PDF 下载 URL"""
    # 根据图片中的 HTML 结构，下载链接格式为：
    # Delivery.cfm/{abstract_id}.pdf?abstractid={abstract_id}&mirid=1
    base_url = "https://papers.ssrn.com/sol3"
    download_url = f"{base_url}/Delivery.cfm/{abstract_id}.pdf?abstractid={abstract_id}&mirid=1"
    return download_url

def download_pdf(url, output_path, abstract_id):
    """下载 PDF 文件，返回 (成功标志, 错误信息)"""
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
            
            # 检查是否是 PDF 文件
            content_type = response.headers.get('Content-Type', '')
            if 'pdf' not in content_type.lower() and not url.endswith('.pdf'):
                # 如果不是 PDF，尝试从页面中提取下载链接
                print(f"  [WARNING] 直接链接不是 PDF，尝试解析页面...")
                return download_from_page(abstract_id, output_path)
            
            # 保存文件
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            file_size = os.path.getsize(output_path)
            if file_size > 0:
                print(f"  [SUCCESS] 下载成功 ({file_size / 1024:.1f} KB)")
                return (True, None)
            else:
                error_msg = "文件大小为 0"
                print(f"  [ERROR] {error_msg}")
                return (False, error_msg)
                
        except requests.exceptions.RequestException as e:
            last_error = str(e)
            print(f"  [WARNING] 尝试 {attempt + 1}/{MAX_RETRIES} 失败: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
            else:
                # 最后一次尝试，从页面解析
                print(f"  [RETRY] 尝试从页面解析下载链接...")
                result = download_from_page(abstract_id, output_path)
                if not result[0]:
                    return (False, f"直接下载失败: {last_error}; 页面解析也失败: {result[1]}")
                return result
    
    return (False, f"所有重试均失败: {last_error}")

def download_from_page(abstract_id, output_path):
    """从 SSRN 页面解析并下载 PDF，返回 (成功标志, 错误信息)"""
    page_url = f"https://papers.ssrn.com/sol3/papers.cfm?abstract_id={abstract_id}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }
    
    try:
        response = requests.get(page_url, headers=headers, timeout=30, proxies=PROXIES)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 查找下载链接 - 根据图片中的 HTML 结构
        # <a href="Delivery.cfm/4517697.pdf?abstractid=4517697&amp;mirid=1" class="button-link primary">
        download_link = None
        
        # 方法1: 查找带有 data-abstract-id 属性的链接
        link = soup.find('a', {'data-abstract-id': abstract_id})
        if link and link.get('href'):
            download_link = link['href']
        else:
            # 方法2: 查找包含 "Download This Paper" 文本的链接
            link = soup.find('a', string=re.compile('Download This Paper', re.I))
            if link and link.get('href'):
                download_link = link['href']
            else:
                # 方法3: 查找 class 包含 "button-link primary" 的链接
                link = soup.find('a', class_=re.compile('button-link.*primary'))
                if link and link.get('href'):
                    download_link = link['href']
        
        if download_link:
            # 处理相对 URL
            if download_link.startswith('/'):
                download_url = f"https://papers.ssrn.com{download_link}"
            elif download_link.startswith('Delivery.cfm'):
                download_url = f"https://papers.ssrn.com/sol3/{download_link}"
            else:
                download_url = download_link
            
            # 下载 PDF
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
                print(f"  [SUCCESS] 从页面下载成功 ({file_size / 1024:.1f} KB)")
                return (True, None)
            else:
                error_msg = "从页面下载的文件大小为 0"
                print(f"  [ERROR] {error_msg}")
                return (False, error_msg)
        else:
            error_msg = "无法在页面中找到下载链接"
            print(f"  [ERROR] {error_msg}")
            return (False, error_msg)
            
    except requests.exceptions.RequestException as e:
        error_msg = f"页面请求失败: {str(e)}"
        print(f"  [ERROR] {error_msg}")
        return (False, error_msg)
    except Exception as e:
        error_msg = f"从页面下载失败: {str(e)}"
        print(f"  [ERROR] {error_msg}")
        return (False, error_msg)

def sanitize_filename(filename):
    """清理文件名，移除非法字符"""
    # 移除或替换非法字符
    illegal_chars = '<>:"/\\|?*'
    for char in illegal_chars:
        filename = filename.replace(char, '_')
    # 限制文件名长度
    if len(filename) > 200:
        filename = filename[:200]
    return filename

def main():
    # 检查代理配置
    if PROXIES:
        print(f"[PROXY] 使用代理: {PROXIES.get('https', PROXIES.get('http', 'N/A'))}")
    else:
        print("[INFO] 未配置代理（如果在中国无法访问，请在脚本中配置 PROXIES）")
    
    # 读取 JSON 文件
    json_path = Path("ruotong.json")
    if not json_path.exists():
        print(f"[ERROR] 找不到文件 {json_path}")
        return
    
    print(f"[INFO] 读取 {json_path}...")
    with open(json_path, 'r', encoding='utf-8') as f:
        urls = json.load(f)
    
    print(f"[INFO] 找到 {len(urls)} 个 URL")
    
    # 创建输出目录
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(exist_ok=True)
    print(f"[INFO] 输出目录: {output_dir.absolute()}\n")
    
    # 统计信息
    success_count = 0
    fail_count = 0
    skip_count = 0
    failed_downloads = []  # 记录失败的下载
    
    # 下载每个论文
    for i, url in enumerate(urls, 1):
        abstract_id = extract_abstract_id(url)
        if not abstract_id:
            error_msg = "无法从 URL 提取 abstract_id"
            print(f"[{i}/{len(urls)}] [ERROR] {error_msg}: {url}")
            fail_count += 1
            failed_downloads.append({
                'url': url,
                'abstract_id': None,
                'error': error_msg,
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            })
            continue
        
        # 检查文件是否已存在
        output_path = output_dir / f"{abstract_id}.pdf"
        if output_path.exists():
            print(f"[{i}/{len(urls)}] [SKIP] 跳过 {abstract_id} (文件已存在)")
            skip_count += 1
            continue
        
        print(f"[{i}/{len(urls)}] [DOWNLOAD] 下载 {abstract_id}...")
        
        # 尝试直接下载
        download_url = get_download_url(abstract_id)
        success, error_msg = download_pdf(download_url, output_path, abstract_id)
        
        if success:
            success_count += 1
        else:
            fail_count += 1
            # 删除失败的文件
            if output_path.exists():
                output_path.unlink()
            # 记录失败信息
            failed_downloads.append({
                'url': url,
                'abstract_id': abstract_id,
                'error': error_msg or "未知错误",
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # 延迟，避免请求过快
        if i < len(urls):
            time.sleep(DELAY_BETWEEN_REQUESTS)
    
    # 保存失败记录到文件
    if failed_downloads:
        log_path = Path(FAILED_LOG_FILE)
        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(failed_downloads, f, indent=2, ensure_ascii=False)
        print(f"\n[INFO] 失败记录已保存到: {log_path.absolute()}")
    
    # 打印统计信息
    print("\n" + "="*50)
    print("[STATISTICS] 下载统计:")
    print(f"  [SUCCESS] 成功: {success_count}")
    print(f"  [SKIP] 跳过: {skip_count}")
    print(f"  [FAILED] 失败: {fail_count}")
    print(f"  [OUTPUT] 输出目录: {output_dir.absolute()}")
    if failed_downloads:
        print(f"  [LOG] 失败记录: {FAILED_LOG_FILE}")
        print(f"\n失败的 URL 列表:")
        for item in failed_downloads[:10]:  # 只显示前10个
            print(f"    - {item['url']} ({item['error']})")
        if len(failed_downloads) > 10:
            print(f"    ... 还有 {len(failed_downloads) - 10} 个失败记录，请查看 {FAILED_LOG_FILE}")
    print("="*50)

if __name__ == "__main__":
    main()

