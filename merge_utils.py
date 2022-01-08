# coding=UTF-8

import os
import json
import time
import requests
import feedparser
import pandas
import hashlib
import math
import datetime
import PyRSS2Gen

SPLIT = 50
URL = {}


def cut(out, df, batch):
    size = len(df)
    starts = [s for s in range(0, size, batch)]
    for idx, start in enumerate(starts):
        batch_file = out + str(idx + 1) + '.csv'
        df_batch = df[start: start + batch]
        df_batch.to_csv(batch_file, index=False, sep=",", encoding="utf-8")

    batch_num = math.ceil(size / batch)
    return batch_num


def merge(out, fetch, dupset=["link"]):
    df = pandas.read_csv(fetch + "new.csv", encoding="utf-8")
    dfs = [df]
    idx = 1
    while True:
        # 为什么之前是idx + 1 ????
        batch_file = out + str(idx) + '.csv'
        idx += 1
        if not os.path.isfile(batch_file):
            break
        df_batch = pandas.read_csv(batch_file, encoding="utf-8")
        dfs.append(df_batch)
    df = pandas.concat(dfs)
    # 先排序
    df = df.sort_values(by="timestamp", ascending=False)
    # 再去重，保证去重保留的是最新的，当dupset=["home"]时需要这样考虑
    df = df.drop_duplicates(subset=dupset, keep="first")
    batch_num = cut(out, df, SPLIT)
    return batch_num


def generator_rss(rss_out, rss_in):
    batch_file = rss_in + '1.csv'
    df = pandas.read_csv(batch_file, encoding="utf-8")
    df_dict = json.loads(df.to_json(orient="records"))
    rss = PyRSS2Gen.RSS2(
        title="RSSBlog",
        link="https://rssblog.cn/",
        description="A Site for Blog RSS.",
        lastBuildDate=datetime.datetime.now(),

        items=[PyRSS2Gen.RSSItem(
            title=r['title'],
            link=r['link'],
            author=r['author'],
            pubDate=datetime.datetime.fromtimestamp(r['timestamp']),
        ) for r in df_dict],
    )
    rss.write_xml(open(rss_out + "rss.xml", "w"))


def merge_source(rss_out_source_dir, rss_fetch_source_dir, url=URL):
    print("merge source ...")
    url["source"] = []
    if not os.path.isdir(rss_out_source_dir):
        os.makedirs(rss_out_source_dir)
    fetch_source_dirs = os.listdir(rss_fetch_source_dir)
    for source_dir in fetch_source_dirs:
        fetch = rss_fetch_source_dir + source_dir + '/'
        out = rss_out_source_dir + source_dir + '/'
        if not os.path.isdir(fetch):
            continue
        if not os.path.isdir(out):
            os.makedirs(out)
        batch_num = merge(out, fetch)
        url["source"].append((source_dir, batch_num))
    print("merge source done")


def merge_all(rss_out_all_dir, rss_fetch_all_dir, url=URL):
    print("merge all ...")
    url["all"] = 0
    if not os.path.isdir(rss_out_all_dir):
        os.makedirs(rss_out_all_dir)
    fetch = rss_fetch_all_dir
    out = rss_out_all_dir
    batch_num = merge(out, fetch)
    generator_rss(out, out)
    url["all"] = batch_num
    print("merge all done")


def merge_member(rss_out_member_dir, rss_fetch_member_dir, url=URL):
    print("merge member ...")
    url["member"] = 0
    if not os.path.isdir(rss_out_member_dir):
        os.makedirs(rss_out_member_dir)
    fetch = rss_fetch_member_dir
    out = rss_out_member_dir
    batch_num = merge(out, fetch, dupset=["home"])
    url["member"] = batch_num
    print("merge member done")


def merge_date(rss_out_date_dir, rss_fetch_date_dir, url=URL):
    print("merge date ...")
    url["date"] = []
    if not os.path.isdir(rss_out_date_dir):
        os.makedirs(rss_out_date_dir)
    fetch_date_dirs = os.listdir(rss_fetch_date_dir)
    date = {}
    for date_dir in fetch_date_dirs:
        fetch = rss_fetch_date_dir + date_dir + '/'
        out = rss_out_date_dir + date_dir + '/'
        if not os.path.isdir(fetch):
            continue
        if not os.path.isdir(out):
            os.makedirs(out)
        batch_num = merge(out, fetch)
        year = date_dir[0:4]
        month = date_dir[4:6]
        if year not in date.keys():
            date[year] = []
        date[year].append((month, batch_num))
    for (year, month) in date.items():
        url["date"].append((year, month))
    print("merge date done")


def merge_user(rss_out_user_dir, rss_fetch_user_dir):
    print("merge user ...")
    global URL
    URL["user"] = []
    if not os.path.isdir(rss_out_user_dir):
        os.makedirs(rss_out_user_dir)
    fetch_user_dirs = os.listdir(rss_fetch_user_dir)
    user_partion = {("all", merge_all), ("date", merge_date),
                    ("member", merge_member)}
    for user_dir in fetch_user_dirs:
        fetch = rss_fetch_user_dir + user_dir + '/'
        out = rss_out_user_dir + user_dir + '/'
        url = {
            "user": user_dir
        }
        for partion in user_partion:
            fetch_dir = fetch + partion[0] + '/'
            out_dir = out + partion[0] + '/'
            if not os.path.isdir(fetch_dir):
                continue
            if not os.path.isdir(out_dir):
                os.makedirs(out_dir)
            partion[1](out_dir, fetch_dir, url)
        URL["user"].append(url)
    print("merge user done")
