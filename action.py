# grequests requires import first
from backup_all import backup
from fetch_rss import fetch
from merge_rss import merge
import sys

OPS = {
    "fetch": fetch,
    "merge": merge,
    "backup": backup,
}

if __name__ == '__main__':
    args = sys.argv
    args_len = len(args)
    if args_len == 1:
        fetch()
        merge()
    else:
        OPS[args[1]]()