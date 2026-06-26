# coding=UTF-8

import os
import pandas
from fetch_utils import *

FEEDS_CSV = "./public/feeds.csv"

# 所有的rss源
rss = []
# 按rss提供者分类的rss
rss_fetch_source_dir = "./__tmp__/source/"
# 举例member
rss_fetch_member_dir = "./__tmp__/member/"
# 所有的rss
rss_fetch_all_dir = "./__tmp__/all/"
# 按时间年月分类的rss
rss_fetch_date_dir = "./__tmp__/date/"


def load_feed_urls(path):
    """从 public/feeds.csv 加载 status=active 的 URL 列表"""
    urls = []
    if not os.path.exists(path):
        print("feeds.csv not found, skip fetching")
        return urls

    try:
        df = pandas.read_csv(path, encoding="utf-8")
        if "url" not in df.columns or "status" not in df.columns:
            print("feeds.csv missing required columns")
            return urls
        active = df[df["status"].astype(str).str.lower() == "active"]
        urls = [normalize_url(u) for u in active["url"].astype(str).tolist() if u]
        urls = list({u: u for u in urls}.values())
    except Exception as e:
        print("load feeds.csv error", e)

    return urls


def fetch():
    global rss
    rss = load_feed_urls(FEEDS_CSV)

    for r in rss:
        print(r)

    fetch_source(rss_fetch_source_dir, rss)
    combine_source(rss_fetch_all_dir, rss_fetch_source_dir)
    combine_member(rss_fetch_member_dir, rss_fetch_all_dir)
    split_date(rss_fetch_date_dir, rss_fetch_all_dir)
