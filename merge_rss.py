from fetch_rss import rss_fetch_source_dir, rss_fetch_member_dir, rss_fetch_user_dir, rss_fetch_all_dir, rss_fetch_date_dir
from merge_utils import *
import json

rss_out_source_dir = "./public/source/"
rss_out_member_dir = "./public/member/"
rss_out_user_dir = "./public/user/"
rss_out_all_dir = "./public/all/"
rss_out_date_dir = "./public/date/"
rss_out_stats_dir = "./public/"


def merge():
    merge_source(rss_out_source_dir, rss_fetch_source_dir)
    merge_all(rss_out_all_dir, rss_fetch_all_dir)
    merge_member(rss_out_member_dir, rss_fetch_member_dir)
    merge_date(rss_out_date_dir, rss_fetch_date_dir)
    merge_user(rss_out_user_dir, rss_fetch_user_dir)
    dumps = {
        "batch": SPLIT,
        "urls": URL,
    }
    with open(rss_out_stats_dir + "stats.json", "w") as f:
        json.dump(dumps, f, indent=2)
    with open(rss_out_stats_dir + "stats.min.json", "w") as f:
        json.dump(dumps, f)
