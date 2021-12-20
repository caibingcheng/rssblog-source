import grequests
import hashlib
import pandas
import os
from functools import wraps


# 因此backup是需要数据准备好之后, 这样也比较合理
BACKUP_SOURCE = "./public/all/"
BACKUP_PATH = "./public/backup/"
BACKUP_STATS = "./public/backup.csv"


def repeat(times=1):
    def repeat_func(func):
        print("backup", func.__name__, "repeat", times, "times")

        @wraps(func)
        def wrap(*args, **kwargs):
            for _ in range(times):
                func(*args, **kwargs)
        return wrap
    return repeat_func


def backup_key(url, timestamp):
    md5 = hashlib.md5()
    md5.update((url + str(timestamp)).encode('utf-8'))
    return md5.hexdigest()


def get_backup_stats():
    backup_stats = pandas.DataFrame(
        columns=['key', 'title', 'author', 'home', 'rss', 'date', 'link', 'timestamp', 'path'])
    try:
        if os.path.exists(BACKUP_STATS):
            backup_stats = pandas.read_csv(BACKUP_STATS, encoding="utf-8")
    except:
        pass
    return backup_stats


def dump_backup_stats(backup_stats):
    backup_stats.to_csv(BACKUP_STATS, index=False,
                        sep=",", encoding="utf-8")


@repeat(3)
def download_article(backup_stats):
    keys = backup_stats.loc[backup_stats['path']
                            == '-', ['key', 'link']].to_numpy()
    if len(keys) == 0:
        return
    print("backup", len(keys), "links waiting for download")

    def failed_backup(requests, exceptions):
        print("backup failed", requests, exceptions)
        return None

    if not os.path.exists(BACKUP_PATH):
        os.makedirs(BACKUP_PATH)

    lens = len(keys)
    batch = 100
    for s in range(0, lens, batch):
        keys_batch = keys[s: s + batch]
        reqs = [grequests.get(key[1], timeout=5.0)
                for key in keys_batch]
        print("backup request batch size: %d" % len(reqs))
        resp = grequests.map(reqs, exception_handler=failed_backup)

        for i, response in enumerate(resp):
            if not response:
                continue
            path = os.path.join(BACKUP_PATH, keys_batch[i][0] + ".html")
            with open(path, 'w') as f:
                f.write(response.text)
                backup_stats.loc[(backup_stats['key'] ==
                                  keys_batch[i][0]), 'path'] = '+'
                f.close()


BACKUP_STAGE_QUEUE = ['-']
BACKUP_STAGE = {
    '-': download_article,
    # '+':,
}


def backup():
    index = 1
    backup_stats = get_backup_stats()
    while True:
        source_path = BACKUP_SOURCE + str(index) + ".csv"
        if not os.path.exists(source_path):
            break

        source = pandas.read_csv(source_path, encoding="utf-8")
        for title, author, date, link, timestamp, home, rss in zip(source['title'],
                                                                   source['author'],
                                                                   source['date'],
                                                                   source['link'],
                                                                   source['timestamp'],
                                                                   source['home'],
                                                                   source['rss']):
            key = backup_key(link, timestamp)
            if not backup_stats['key'].isin([key]).any():
                print("backup append", key)
                backup_stats = backup_stats.append({
                    'key': key,
                    'title': title,
                    'author': author,
                    'date': date,
                    'link': link,
                    'home': home,
                    'rss': rss,
                    'timestamp': timestamp,
                    'path': "-",
                }, ignore_index=True)

        index += 1
    for stage in BACKUP_STAGE_QUEUE:
        if stage in BACKUP_STAGE.keys():
            BACKUP_STAGE[stage](backup_stats)
    dump_backup_stats(backup_stats)


if __name__ == '__main__':
    def failed_backup(requests, exceptions):
        print("backup failed", requests, exceptions)
        return None

    url = 'https://baidu.com'
    reqs = [grequests.get(url, timeout=5.0) for _ in range(100)]
    resp = grequests.map(reqs, exception_handler=failed_backup)
    print(resp)
