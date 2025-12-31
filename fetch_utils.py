# coding=UTF-8

import os
import json
import time
import requests
import feedparser
import pandas
import hashlib
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO


def hash_url(url):
    md5 = hashlib.md5()
    md5.update(url.encode("utf-8"))
    return md5.hexdigest()


def get_entry_date(entry):
    if "published_parsed" in entry.keys():
        return entry["published_parsed"]
    elif "updated_parsed" in entry.keys():
        return entry["updated_parsed"]
    else:
        return time.localtime()


def fetch_source(rss_fetch_source_dir, rss):
    """按照来源抓取rss，放到每个源对应的new.csv中"""
    print("fetch new rss ...")
    if not os.path.isdir(rss_fetch_source_dir):
        os.makedirs(rss_fetch_source_dir)

    def parse_rss(r):
        try:
            content = requests.get(r, timeout=10.0).content
            rp = feedparser.parse(BytesIO(content))
        except Exception as e:
            print("parse", r, "error", e)
            return

        print("parse", r)
        try:
            rss_link = [
                {
                    "title": et["title"].replace(",", "，"),
                    # 如果源相同, 但是author名字不同, 这里就会使用最后一个
                    "author": (
                        rp["feed"]["link"]
                        if "title" not in rp["feed"].keys()
                        else rp["feed"]["title"]
                    ),
                    "link": et["link"],
                    "home": rp["feed"]["link"],
                    "rss": r,
                    "date": time.strftime("%Y-%m-%d", get_entry_date(et)),
                    "timestamp": time.mktime(get_entry_date(et)),
                }
                for et in rp["entries"]
            ]
        except Exception as e:
            print("un-support rss", r, e)
            return

        url_hash = hash_url(r)
        rss_dir = rss_fetch_source_dir + url_hash + "/"
        # 不管能不能获取到，先创建一个目录
        # 好处是保证完整性，不管怎么样都有所有的source
        # 坏处是，后续merge操作可能需要判断路径空
        if not os.path.isdir(rss_dir):
            os.makedirs(rss_dir)

        df = pandas.json_normalize(rss_link)
        if len(df) <= 0:
            print("fetching skip", r, "to", url_hash, "size", len(rss_link), len(df))
            return
        df.to_csv(rss_dir + "new.csv", index=False, sep=",", encoding="utf-8")

    # pool = ThreadPoolExecutor(max_workers=10)
    # futures = []
    # for r in rss:
    #     futures.append(pool.submit(parse_rss, r))

    # for future in futures:
    #     future.result()

    for r in rss:
        parse_rss(r)

    print("fetch new rss done")


def combine_source(rss_fetch_all_dir, rss_fetch_source_dir):
    """合并所有的来源，把每个源的new.csv合并到all/new.csv中"""
    print("combin all page ...")
    if not os.path.isdir(rss_fetch_all_dir):
        os.makedirs(rss_fetch_all_dir)
    source_dirs = os.listdir(rss_fetch_source_dir)
    dfs = []
    for file in source_dirs:
        source_file_path = rss_fetch_source_dir + file + "/new.csv"
        if not os.path.isfile(source_file_path):
            continue
        try:
            df = pandas.read_csv(source_file_path, encoding="utf-8")
            dfs.append(df)
        except:
            print("combining skip", file)
    df = pandas.concat(dfs)
    df = df.sort_values("timestamp", ascending=False)
    df.to_csv(rss_fetch_all_dir + "new.csv", index=False, sep=",", encoding="utf-8")
    print("combin all page done")


def combine_member(rss_fetch_member_dir, rss_fetch_all_dir):
    """通过all/new.csv中的author去重，得到member/new.csv，这里面包含了所有的author（更新了的）"""
    print("combin member rss ...")
    if not os.path.isdir(rss_fetch_member_dir):
        os.makedirs(rss_fetch_member_dir)
    df = pandas.read_csv(rss_fetch_all_dir + "/new.csv", encoding="utf-8")
    df = df.sort_values(by="author", kind="mergesort")  # 保留前后顺序
    df = df.drop_duplicates(subset=["author"], keep="first")
    df = df.sort_values(by="timestamp", ascending=False)
    df.to_csv(rss_fetch_member_dir + "new.csv", index=False, sep=",", encoding="utf-8")
    print("combin member rss done")


def split_date(rss_fetch_date_dir, rss_fetch_all_dir):
    """按照年月分类，把all/new.csv中的数据按照年月分类，放到date/年月/new.csv中"""
    print("split date page ...")
    if not os.path.isdir(rss_fetch_date_dir):
        os.makedirs(rss_fetch_date_dir)
    df = pandas.read_csv(rss_fetch_all_dir + "/new.csv", encoding="utf-8")
    dfd = {}
    for i, d in df.iterrows():
        # Validate date format before slicing
        date_str = str(d["date"])
        if len(date_str) < 10 or date_str[4] != "-" or date_str[7] != "-":
            print(f"Warning: Invalid date format '{date_str}' at row {i}, skipping")
            continue
        # Extract year and month directly
        year_str = date_str[0:4]
        month_str = date_str[5:7]
        # Validate year and month are numeric and reasonable
        try:
            year = int(year_str)
            month = int(month_str)
            if year < 1970 or year > 2100 or month < 1 or month > 12:
                print(f"Warning: Invalid year/month '{year_str}-{month_str}' from date '{date_str}' at row {i}, skipping")
                continue
        except ValueError:
            print(f"Warning: Non-numeric year/month '{year_str}-{month_str}' from date '{date_str}' at row {i}, skipping")
            continue
        year_month = year_str + month_str
        if year_month not in dfd.keys():
            dfd[year_month] = []
        dfd[year_month].append(i)
    for key, i in dfd.items():
        date_dir = rss_fetch_date_dir + key
        if not os.path.isdir(date_dir):
            os.makedirs(date_dir)
        df.iloc[i].to_csv(date_dir + "/new.csv", index=False, sep=",", encoding="utf-8")
    print("split date page done")


def split_user(rss_fetch_user_dir, rss_user, rss_fetch_source_dir):
    """按照用户分类，把all/new.csv中的数据按照用户分类，每个用户下又有date、member、all目录"""
    print("combin user rss ...")
    if not os.path.isdir(rss_fetch_user_dir):
        os.makedirs(rss_fetch_user_dir)
    for user, user_rss in rss_user.items():
        dfs = []
        for r in user_rss:
            url_hash = hash_url(r)
            # 每个源一定有一个source
            try:
                ldf = pandas.read_csv(
                    rss_fetch_source_dir + url_hash + "/new.csv", encoding="utf-8"
                )
                dfs.append(ldf)
            except:
                print("combin user skip", url_hash)
        df = pandas.concat(dfs)
        df = df.sort_values("timestamp", ascending=False)
        rss_dir = rss_fetch_user_dir + user + "/all/"
        if not os.path.isdir(rss_dir):
            os.makedirs(rss_dir)
        df.to_csv(rss_dir + "new.csv", index=False, sep=",", encoding="utf-8")
        date_dir = rss_fetch_user_dir + user + "/date/"
        split_date(date_dir, rss_dir)  # user date
        member_dir = rss_fetch_user_dir + user + "/member/"
        combine_member(member_dir, rss_dir)  # user member
    print("combin user rss done")
