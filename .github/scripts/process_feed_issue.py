# coding=UTF-8

import hashlib
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

import feedparser
import pandas
import requests

FEEDS_CSV = "./public/feeds.csv"
REPLACED_BY_PATTERN = re.compile(r"^\s*(\S+)\s+replaced\s+by\s+(\S+)\s*$", re.IGNORECASE)
URL_RE = re.compile(
    r'https?://[^\s"\'<>\u201c\u201d\u2018\u2019)\]\},;#]+',
    re.IGNORECASE,
)


def git(args, cwd="./public", check=True):
    """在 ./public 仓库运行 git 命令"""
    print(f"[git] {' '.join(['git', *args])} (cwd={cwd})", flush=True)
    result = subprocess.run(
        ["git", *args], cwd=cwd, check=False, capture_output=True, text=True
    )
    if result.stdout:
        print(result.stdout, end="", flush=True)
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr, flush=True)
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, ["git", *args], output=result.stdout, stderr=result.stderr
        )
    return result


def remote_branch_exists(branch, cwd="./public"):
    """检查远程分支是否存在"""
    result = subprocess.run(
        ["git", "ls-remote", "--heads", "origin", branch],
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0 and branch in result.stdout


def hash_url(url):
    md5 = hashlib.md5()
    md5.update(url.encode("utf-8"))
    return md5.hexdigest()


def normalize_url(url):
    url = url.strip()
    url = url.lower()
    idx = url.find("#")
    if idx != -1:
        url = url[:idx]
    url = url.rstrip("/")
    # 去掉可能残留的末尾标点
    url = url.rstrip(',;"\'\u201c\u201d\u2018\u2019.:)]}>')
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

        urls = URL_RE.findall(line)
        if not urls:
            continue

        lowered = line.lower()
        if "replaced" in lowered and "by" in lowered and len(urls) >= 2:
            replaced_by.append((normalize_url(urls[0]), normalize_url(urls[1])))
        else:
            plain_urls.append(normalize_url(urls[0]))

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
    """新增或更新 feed，返回 (df, action)；若无需修改返回 action=None"""
    existing = df[df["url"] == url]
    if len(existing) > 0:
        row = existing.iloc[0]
        if (
            str(row.get("status", "")) == "active"
            and str(row.get("author", "")) == author
            and str(row.get("home", "")) == home
        ):
            return df, None

        df.loc[existing.index, "status"] = "active"
        df.loc[existing.index, "last_checked"] = now_iso()
        df.loc[existing.index, "author"] = author
        df.loc[existing.index, "home"] = home
        return df, "updated"

    new_row = {
        "url": url,
        "status": "active",
        "last_checked": now_iso(),
        "author": author,
        "home": home,
        "id": hash_url(url),
        "note": "",
    }
    return pandas.concat([df, pandas.DataFrame([new_row])], ignore_index=True), "added"


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

        df, action = add_or_update_feed(df, url, author, home)
        if action:
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

    # 写入文件并提交
    if has_changes:
        save_feeds(df, FEEDS_CSV)

        source_branch = os.environ.get("SOURCE_BRANCH", "master")
        public_branch = os.environ.get("PUBLIC_BRANCH", "public")
        new_branch = os.environ.get("NEW_BRANCH", f"update-feeds-issue-{issue_number}")

        # 设置提交者信息
        git(["config", "user.email", "actions@github.com"])
        git(["config", "user.name", "GitHub Actions"])

        # 如果远程分支已存在，则基于该分支继续提交；否则从当前 public 分支新建
        if remote_branch_exists(new_branch):
            git(["fetch", "origin", f"{new_branch}:refs/remotes/origin/{new_branch}"])
            git(["switch", "--force-create", new_branch, f"origin/{new_branch}"])
        else:
            git(["switch", "--create", new_branch])

        git(["add", "feeds.csv"])

        # 只有真正有变更才提交并推送
        diff_check = git(["diff", "--cached", "--quiet"], check=False)
        if diff_check.returncode != 0:
            git(["commit", "-m", f"Update feeds from issue #{issue_number}"])
            git(["push", "origin", new_branch])
        else:
            has_changes = False

    # 输出给 GitHub Actions
    report_text = "\n".join(reports) if reports else "No URLs processed."
    summary = "\n".join(reports) if reports else "No changes."

    gh_output = os.environ.get("GITHUB_OUTPUT")
    if gh_output:
        with open(gh_output, "a") as f:
            f.write(f'has_changes={"true" if has_changes else "false"}\n')
            f.write(f"report<<EOF\n{report_text}\nEOF\n")
            f.write(f"change_summary<<EOF\n{summary}\nEOF\n")


if __name__ == "__main__":
    main()
