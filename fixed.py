import os, sys
import pandas, hashlib


def hash_url(url):
    md5 = hashlib.md5()
    md5.update(url.encode('utf-8'))
    return md5.hexdigest()


def fix_source():
    all_dir = "./public/all"
    source_dir = "./public/source"
    index = 1
    source_dirs = os.listdir(source_dir)
    for sdir in source_dirs:
        real_path = os.path.join(source_dir, sdir)
        if not os.path.exists(real_path):
            break
        print("fixing source", sdir, "...")

        source_path = os.path.join(source_dir, sdir, '1.csv')
        has_source = os.path.exists(source_path)
        def filter(rss, target):
            if has_source:
                return rss == target
            else:
                return hash_url(rss) == target

        target = sdir
        if has_source:
            source = pandas.read_csv(source_path, encoding="utf-8")
            target = source.loc[0, 'rss']

        while True:
            all_dir_path = all_dir + str(index) + ".csv"
            if not os.path.exists(all_dir_path):
                break
            index += 1

            all_content = pandas.read_csv(all_dir_path, encoding="utf-8")
            cands = all_content.loc[filter(all_content['rss'],target)]
            source = pandas.concat([source, cands])
        source = source.drop_duplicates(subset=["link"], keep="first")
        source = source.sort_values(by="timestamp", ascending=False)
        source.to_csv(source_path, index=False, sep=",", encoding="utf-8")
        print("fixing source", sdir, "done")

fix_map = {
    'source': fix_source
}

if __name__ == "__main__":
    args = sys.argv
    for ftype in args[1].split(','):
        if ftype in fix_map.keys():
            print("fixing type %s" % ftype)
            fix_map[ftype]()