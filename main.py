import os
import time
import json
import sqlite3
import hashlib
import requests
import feedparser
import pandas as pd
from pathlib import Path
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration paths (loaded from config.json)
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"

def load_config():
    """Load configuration file"""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found at {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = load_config()
TMP_DIR = Path(CONFIG["tmp_dir"])
DB_PATH = Path(CONFIG["database_path"])

def load_config():
    """Load configuration file"""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found at {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def ensure_dirs():
    """Create required directories"""
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    (BASE_DIR / "database").mkdir(exist_ok=True)
    (BASE_DIR / "public").mkdir(exist_ok=True)

def hash_url(url: str) -> str:
    """Generate MD5 hash for URL"""
    return hashlib.md5(url.encode("utf-8")).hexdigest()

from dateutil import parser

def get_entry_date(entry):
    """解析条目日期，支持多种格式并自动处理时区"""
    timestamp = None
    date_str = None

    def parsed_process(date_str):
        try:
            return time.mktime(date_str)
        except (TypeError, ValueError) as e:
            print(f"Error parsing time tuple: {e}")
            return None

    def normal_process(date_str):
        try:
            dt = parser.parse(
                date_str,
                ignoretz=False,  # 保留时区信息
                tzinfos={"CST": 8*3600}  # 支持中国标准时区
            )
            return dt.timestamp()
        except Exception as e:
            print(f"Date parsing failed [{date_str}]: {str(e)}")
            return None

    process_map = {
        "published_parsed": parsed_process,
        "updated_parsed": parsed_process,
        "published": normal_process,
        "updated": normal_process,
    }

    for key in ["published_parsed", "published", "updated_parsed", "updated"]:
        if key in process_map.keys():
            date_str = entry.get(key)
            if date_str:
                process_func = process_map[key]
                timestamp = process_func(date_str)
                if timestamp:
                    return timestamp
    return None

def parse_rss_feed(rss_url: str):
    """解析单个RSS源"""
    try:
        # 请求并解析RSS
        response = requests.get(rss_url, timeout=15)
        response.raise_for_status()
        feed = feedparser.parse(BytesIO(response.content))

        # 提取条目数据
        entries = []
        for entry in feed.entries:
            timestamp = get_entry_date(entry)
            if not timestamp:
                print(f"Entry missing date info: {entry.get('link', 'Unknown link')}")
                continue
            entry_data = {
                "title": entry.get("title", "").replace(",", "，"),
                "author": feed.feed.get("title", feed.feed.get("link", "")),
                "link": entry.get("link", ""),
                "home": feed.feed.get("link", ""),
                "rss": rss_url,
                "date": time.strftime("%Y-%m-%d", time.localtime(timestamp)),
                "timestamp": timestamp,
                "test": "",
            }
            entries.append(entry_data)
        if entries:
            print(f"First entry sample: {entries[0]}")

        return entries
    except Exception as e:
        print(f"Failed to parse RSS feed [{rss_url}]: {str(e)}")
        return []

def fetch_and_store_data(config: dict):
    """Fetch and store RSS data"""
    rss_source_list = config["rss_source_list"]
    rss_sources = []
    seen_urls = set()
    for source in rss_source_list:
        try:
            response = requests.get(source, verify=False, timeout=10)
            response.raise_for_status()
            source_list = json.loads(response.text)
            for key, url in source_list.items():
                try:
                    resp = requests.get(url, verify=False, timeout=10)
                    resp.raise_for_status()
                    urls = json.loads(resp.text)
                    for clean_url in (u.strip("/") for u in urls if u.strip("/") not in seen_urls):
                        seen_urls.add(clean_url)
                        rss_sources.append(clean_url)
                except Exception as e:
                    print(f"Failed to process source URL {url}: {str(e)}")
        except Exception as e:
            print(f"Failed to fetch source list {source}: {str(e)}")
    all_entries = []

    print(f"Starting to fetch {len(rss_sources)} RSS sources...")

    # 并行抓取所有RSS源
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(parse_rss_feed, url): url for url in rss_sources}
        for future in as_completed(futures):
            url = futures[future]
            try:
                entries = future.result()
                if entries:
                    all_entries.extend(entries)
                    print(f"Successfully fetched [{url}] with {len(entries)} entries")
            except Exception as e:
                print(f"Fetch failed [{url}]: {str(e)}")

    # 保存到临时文件
    if all_entries:
        df = pd.DataFrame(all_entries)
        df.sort_values("timestamp", ascending=False, inplace=True)
        tmp_file = TMP_DIR / "1.csv"
        # clear tmp_file if exists
        if tmp_file.exists():
            tmp_file.unlink()
        # save to tmp_file
        tmp_file.parent.mkdir(parents=True, exist_ok=True)
        # save to csv
        df.drop_duplicates(subset=["link"], keep="last", inplace=True)
        df.reset_index(drop=True, inplace=True)
        # save to csv
        df.to_csv(tmp_file, index=False, encoding="utf-8")
        print(f"Saved {len(df)} entries to {tmp_file}")
    else:
        print("No new data received")

def run_adaptor():
    """Execute adaptor data processing"""
    print("\n启动adaptor数据处理...")
    from adaptor import adaptor
    class Args:
        def __init__(self, command, csv_dir=None):
            self.command = command
            self.csv_dir = csv_dir

    # 初始化参数并调用adaptor
    args = Args(command='import', csv_dir=str(TMP_DIR.absolute()))
    try:
        if not Path(CONFIG["export"]["version"]).exists():
            adaptor.main(Args(command='import'))
            Path(CONFIG["export"]["version"]).touch()
        adaptor.main(args)
    except Exception as e:
        print(f"Adaptor processing failed: {str(e)}")
        raise
    print("Adaptor processing completed")

def main():
    ensure_dirs()
    config = load_config()

    try:
        # 数据抓取阶段
        fetch_and_store_data(config)

        # 数据处理阶段
        run_adaptor()

        print("\nProcess execution completed")
    except Exception as e:
        print(f"\nProcess execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()