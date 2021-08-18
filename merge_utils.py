# coding=UTF-8

import os
import json
import time
import requests
import feedparser
import pandas
import hashlib
import math

SPLIT = 50
URL = {}


def cut(out, df, batch):
    size = len(df)
    start, end = 0, batch
    batch_num = math.ceil(size / batch)
    for idx in range(batch_num):
        batch_file = out + str(idx + 1) + '.csv'
        end = size if end > size else end
        df_batch = df[start: end]
        start, end = start + batch, end + batch
        df_batch.to_csv(batch_file, index=False, sep=",", encoding="utf-8")
    return batch_num


def merge(out, fetch):
    df = pandas.read_csv(fetch + "new.csv", encoding="utf-8")
    dfs = [df]
    idx = 1
    while True:
        batch_file = out + str(idx + 1) + '.csv'
        idx += 1
        if not os.path.isfile(batch_file):
            break
        df_batch = pandas.read_csv(batch_file, encoding="utf-8")
        dfs.append(df_batch)
    df = pandas.concat(dfs)
    df = df.drop_duplicates(subset=["link"], keep="first")
    df = df.sort_values(by="timestamp", ascending=False)
    batch_num = cut(out, df, SPLIT)
    return batch_num


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
    url["all"] = batch_num
    print("merge all done")


def merge_member(rss_out_member_dir, rss_fetch_member_dir, url=URL):
    print("merge member ...")
    url["member"] = 0
    if not os.path.isdir(rss_out_member_dir):
        os.makedirs(rss_out_member_dir)
    fetch = rss_fetch_member_dir
    out = rss_out_member_dir
    batch_num = merge(out, fetch)
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
