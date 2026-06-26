# coding=UTF-8

import hashlib
import os
import re
import sys
from datetime import datetime, timezone

import feedparser
import pandas
import requests

FEEDS_CSV = "./public/feeds.csv"
REPLACED_BY_PATTERN = re.compile(r"^\s*(\S+)\s+replaced\s+by\s+(\S+)\s*$", re.IGNORECASE)


def hash_url(url):
    md5 = hashlib.md5()
    md5.update(url.encode("utf-8"))
    return md5.hexdigest()


def normalize_url(url):
    url = url.strip()
    url = url.strip('""''\u201c\u201d\u2018\u2019')
    url = url.lower()
    url = url.rstrip("/")
    idx = url.find("#")
    if idx != -1:
        url = url[:idx]
    return url


def parse_issue_body(body):
    """解析 issue body，返回 (普通url列表, replaced_by列表)"""
    plain_urls = []
    replaced_by = []
    if not body:
        return plain_urls, replaced_by

    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        match = REPLACED_BY_PATTERN.match(line)
        if match:
            old_url = normalize_url(match.group(1))
            new_url = normalize_url(match.group(2))
            replaced_by.append((old_url, new_url))
        else:
            plain_urls.append(normalize_url(line))

    return plain_urls, replaced_by


def validate_feed(url, timeout=20):
    """验证 URL 是否为有效 RSS/Atom feed"""
    try:
        response = requests.get(url, timeout=timeout, headers={
            "User-Agent": "Mozilla/5.0 (RSSBlog Source Validator)"
        })
        response.raise_for_status()
        parsed = feedparser.parse(response.content)
    except Exception as e:
        return False, str(e), None, None

    if parsed.bozo and not parsed.entries:
        return False, f"parse error: {parsed.bozo_exception}", None, None

    feed_title = parsed.feed.get("title", "").strip()
    if not feed_title:
        return False, "missing feed title", None, None

    if not parsed.entries:
        return False, "no entries found", None, None

    home = parsed.feed.get("link", "").strip()
    return True, "", feed_title, home


def load_feeds(path):
    """加载 feeds.csv，若不存在则返回空 DataFrame"""
    columns = ["url", "status", "last_checked", "author", "home", "id", "note"]
    if os.path.exists(path):
        try:
            df = pandas.read_csv(path, encoding="utf-8")
            for col in columns:
                if col not in df.columns:
                    df[col] = ""
            return df[columns]
        except Exception:
            pass
    return pandas.DataFrame(columns=columns)


def save_feeds(df, path):
    """保存 feeds.csv"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, sep=",", encoding="utf-8")


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def add_or_update_feed(df, url, author, home):
    """新增或更新 feed，返回 (df, is_new)"""
    existing = df[df["url"] == url]
    if len(existing) > 0:
        df.loc[existing.index, "status"] = "active"
        df.loc[existing.index, "last_checked"] = now_iso()
        df.loc[existing.index, "author"] = author
        df.loc[existing.index, "home"] = home
        return df, False

    new_row = {
        "url": url,
        "status": "active",
        "last_checked": now_iso(),
        "author": author,
        "home": home,
        "id": hash_url(url),
        "note": "",
    }
    return pandas.concat([df, pandas.DataFrame([new_row])], ignore_index=True), True


def replace_feed(df, old_url, new_url, new_author, new_home):
    """处理 replaced by：新 URL 继承旧 URL 的 id/author/home/note"""
    old_rows = df[df["url"] == old_url]
    new_rows = df[df["url"] == new_url]

    inherited_id = None
    inherited_note = ""

    if len(old_rows) > 0:
        inherited_id = old_rows.iloc[0]["id"]
        inherited_note = str(old_rows.iloc[0]["note"])
        if inherited_note.lower() == "nan":
            inherited_note = ""
        # 旧 URL 标记为 inactive
        df.loc[old_rows.index, "status"] = "inactive"
        df.loc[old_rows.index, "last_checked"] = now_iso()
        migration_note = f"replaced by {new_url} at {now_iso()}"
        df.loc[old_rows.index, "note"] = (
            f"{inherited_note}; {migration_note}" if inherited_note else migration_note
        ).strip("; ")

    if len(new_rows) > 0:
        # 新 URL 已存在，更新并继承 id
        df.loc[new_rows.index, "status"] = "active"
        df.loc[new_rows.index, "last_checked"] = now_iso()
        df.loc[new_rows.index, "author"] = new_author
        df.loc[new_rows.index, "home"] = new_home
        if inherited_id:
            df.loc[new_rows.index, "id"] = inherited_id
        return df, False

    new_row = {
        "url": new_url,
        "status": "active",
        "last_checked": now_iso(),
        "author": new_author,
        "home": new_home,
        "id": inherited_id or hash_url(new_url),
        "note": f"replaces {old_url}" if inherited_id else f"replaces {old_url} (old url not found)",
    }
    return pandas.concat([df, pandas.DataFrame([new_row])], ignore_index=True), True


def main():
    issue_number = os.environ.get("ISSUE_NUMBER", "")
    issue_body = os.environ.get("ISSUE_BODY", "")

    plain_urls, replaced_by = parse_issue_body(issue_body)

    # 先去重，保持顺序
    seen = set()
    unique_plain = []
    for u in plain_urls:
        if u not in seen:
            seen.add(u)
            unique_plain.append(u)

    df = load_feeds(FEEDS_CSV)

    reports = []
    has_changes = False

    # 处理普通 URL
    for url in unique_plain:
        ok, err, author, home = validate_feed(url)
        if not ok:
            reports.append(f"- `{url}`: invalid ({err})")
            continue

        df, is_new = add_or_update_feed(df, url, author, home)
        action = "added" if is_new else "updated"
        reports.append(f"- `{url}`: {action} (author={author}, home={home})")
        has_changes = True

    # 处理 replaced by
    for old_url, new_url in replaced_by:
        ok, err, new_author, new_home = validate_feed(new_url)
        if not ok:
            reports.append(f"- `{old_url} replaced by {new_url}`: invalid new url ({err})")
            continue

        df, is_new = replace_feed(df, old_url, new_url, new_author, new_home)
        action = "replaced" if is_new else "updated"
        reports.append(f"- `{old_url} replaced by {new_url}`: {action}")
        has_changes = True

    # 写入文件
    if has_changes:
        save_feeds(df, FEEDS_CSV)

    # 创建新分支并提交
    if has_changes:
        source_branch = os.environ.get("SOURCE_BRANCH", "master")
        public_branch = os.environ.get("PUBLIC_BRANCH", "public")
        new_branch = os.environ.get("NEW_BRANCH", f"update-feeds-issue-{issue_number}")

        os.system(f"git -C ./public checkout -b {new_branch}")
        os.system("git -C ./public config user.email 'actions@github.com'")
        os.system("git -C ./public config user.name 'GitHub Actions'")
        os.system("git -C ./public add feeds.csv")
        os.system(f"git -C ./public commit -m 'Update feeds from issue #{issue_number}'")
        os.system(f"git -C ./public push origin {new_branch}")

    # 输出给 GitHub Actions
    report_text = "\n".join(reports) if reports else "No URLs processed."
    summary = "\n".join(reports) if reports else "No changes."

    print(f"::set-output name=has_changes::{'true' if has_changes else 'false'}")
    print(f"::set-output name=report::{report_text}")
    print(f"::set-output name=change_summary::{summary}")


if __name__ == "__main__":
    main()
