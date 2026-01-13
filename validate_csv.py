#!/usr/bin/env python3
"""validate_csv.py

Scan CSV files for duplicate records and log all duplicate locations.

Checks:
 - `public/all` (non-recursive)
 - `public/member` (non-recursive)
 - `source` (recursive)

Writes findings to `validate_csv.log` in the repo root. Continues scanning until all duplicates are discovered.
"""
from pathlib import Path
from collections import defaultdict
import csv
import sys


LOGFILE = Path("validate_csv.log")


def gather_files_grouped():
    """Return a dict mapping group name -> list of csv Path objects.

    Groups:
      - "public/all": non-recursive *.csv under public/all
      - "public/member": non-recursive *.csv under public/member
      - "source": recursive *.csv under source and subdirs
    """
    repo_root = Path(__file__).resolve().parent
    groups = {"public/all": [], "public/member": [], "public/source": [], "public/date": []}

    p_public_all = repo_root / "public" / "all"
    p_public_member = repo_root / "public" / "member"
    p_source = repo_root / "public" / "source"
    p_date = repo_root / "public" / "date"

    if p_public_all.exists():
        groups["public/all"] = sorted([p for p in p_public_all.glob("*.csv") if p.is_file()])
    if p_public_member.exists():
        groups["public/member"] = sorted([p for p in p_public_member.glob("*.csv") if p.is_file()])
    if p_source.exists():
        groups["public/source"] = sorted([p for p in p_source.rglob("*.csv") if p.is_file()])
    if p_date.exists():
        groups["public/date"] = sorted([p for p in p_date.rglob("*.csv") if p.is_file()])

    return groups


def normalize_line(line: str) -> str:
    # remove trailing newlines/carriage returns, keep other whitespace
    s = line.rstrip("\r\n")
    # strip UTF-8 BOM if present at start
    if s.startswith("\ufeff"):
        s = s.lstrip("\ufeff")
    return s


def scan_files(files):
    occurrences = defaultdict(list)  # record -> list of (file, lineno)

    for fp in files:
        try:
            # open with newline='' as recommended by csv module docs
            with fp.open("r", encoding="utf-8", errors="replace", newline="") as f:
                reader = csv.reader(f)
                repo_root = Path(__file__).resolve().parent
                for i, row in enumerate(reader, start=1):
                    # skip header (first row)
                    if i == 1:
                        # remove BOM from first header field if present to avoid leaking into data rows
                        if row and isinstance(row[0], str) and row[0].startswith("\ufeff"):
                            row[0] = row[0].lstrip("\ufeff")
                        continue

                    # normalize fields: strip whitespace
                    row = [c.strip() if isinstance(c, str) else c for c in row]
                    # remove BOM from first field if present
                    if row and isinstance(row[0], str) and row[0].startswith("\ufeff"):
                        row[0] = row[0].lstrip("\ufeff")

                    # skip empty rows
                    if not any((c for c in row if isinstance(c, str) and c.strip() != "")):
                        continue

                    # use tuple of fields as the record key
                    key = tuple(row)
                    occurrences[key].append((str(fp.relative_to(repo_root)), i))
        except Exception as e:
            print(f"Warning: failed to read {fp}: {e}", file=sys.stderr)

    return occurrences


def write_log_for_group(group_name: str, occurrences, logf):
    """Write duplicates for a single group into an open log file object.

    Returns number of duplicate records found in this group.
    """
    dup_count = 0
    logf.write(f"===== Group: {group_name} =====\n")
    for record, locs in occurrences.items():
        if len(locs) > 1:
            dup_count += 1
            logf.write("--- Duplicate record found ({}) occurrences ---\n".format(len(locs)))
            logf.write(repr(record) + "\n")
            for fp, lineno in locs:
                logf.write(f"  - {fp}:{lineno}\n")
            logf.write("\n")

    if dup_count == 0:
        logf.write("No duplicates found in this group.\n")

    logf.write("\n")
    return dup_count


def main():
    groups = gather_files_grouped()
    total_files = sum(len(lst) for lst in groups.values())
    if total_files == 0:
        print("No CSV files found in the target locations.")
        return 0

    print(f"Scanning {total_files} CSV files across {len(groups)} groups...")

    total_dup_records = 0
    # open log and overwrite
    with LOGFILE.open("w", encoding="utf-8") as logf:
        for gname, flist in groups.items():
            logf.write(f"## Scanning group {gname} ({len(flist)} files)\n")
            if not flist:
                logf.write("No files found in this group.\n\n")
                continue

            occurrences = scan_files(flist)
            dup_count = write_log_for_group(gname, occurrences, logf)
            total_dup_records += dup_count

    if total_dup_records:
        print(f"Found {total_dup_records} duplicated record(s) across groups. See {LOGFILE} for details.")
        return 2
    else:
        print("No duplicates found in any group.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
