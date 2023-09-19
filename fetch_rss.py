# coding=UTF-8

import os
import json
import time
import requests
import feedparser
import pandas
from fetch_utils import *

requests.packages.urllib3.disable_warnings()
fetch_list_source = "https://gist.githubusercontent.com/caibingcheng/adf8f300dc50a61a965bdcc6ef0aecb3/raw/rssblog-source-list.json"
fetch_list = json.loads(requests.get(fetch_list_source, verify=False).text)

# 所有的rss源
rss = []
# 根据不同用户得到的rss源
rss_user = {}
# 按rss提供者分类的rss
rss_fetch_source_dir = "./__tmp__/source/"
# 举例member
rss_fetch_member_dir = "./__tmp__/member/"
# 按用户分类的rss
rss_fetch_user_dir = "./__tmp__/user/"
# 所有的rss
rss_fetch_all_dir = "./__tmp__/all/"
# 按时间年月分类的rss
rss_fetch_date_dir = "./__tmp__/date/"


def fetch():
    global rss
    for (key, link) in fetch_list.items():
        rss_list = []
        try:
            rss_list = json.loads(requests.get(link, verify=False).text)
            for r in rss_list:
                r = r.strip("/")
                print(r)
        except:
            pass
        rss = rss + rss_list
        rss_user[key] = rss_list

    # 所有源根据url去重
    rss = list({r: r for r in rss}.values())
    # 个人源不去重, 依赖于个人维护
    # for test
    # rss = ["https://xxxx/feed/",]
    # rss_user["test"] = rss

    fetch_source(rss_fetch_source_dir, rss)
    combin_source(rss_fetch_all_dir, rss_fetch_source_dir)
    combin_member(rss_fetch_member_dir, rss_fetch_all_dir)
    split_date(rss_fetch_date_dir, rss_fetch_all_dir)
    split_user(rss_fetch_user_dir, rss_user, rss_fetch_source_dir)
