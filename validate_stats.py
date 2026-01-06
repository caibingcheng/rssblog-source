# coding=UTF-8
"""
Validate stats.min.json for data quality issues.
This script checks for invalid date formats in the stats file.
"""

import json
import sys
import re


# Pre-compile regex patterns for better performance
YEAR_PATTERN = re.compile(r'^\d{4}$')
MONTH_PATTERN = re.compile(r'^\d{2}$')


def validate_year_format(year):
    """
    Validate that year is a 4-digit number representing a valid year.
    Returns (is_valid, error_message)
    """
    # Year should be exactly 4 digits
    if not YEAR_PATTERN.match(str(year)):
        return False, f"Invalid year format: '{year}' (expected 4-digit year like '2025')"
    
    # Check if year is in reasonable range (1970-2100)
    year_int = int(year)
    if year_int < 1970 or year_int > 2100:
        return False, f"Year '{year}' is out of reasonable range (1970-2100)"
    
    return True, None


def validate_month_format(month):
    """
    Validate that month is a 2-digit number representing a valid month (01-12).
    Returns (is_valid, error_message)
    """
    # Month should be exactly 2 digits
    if not MONTH_PATTERN.match(str(month)):
        return False, f"Invalid month format: '{month}' (expected 2-digit month like '01')"
    
    # Check if month is in valid range (01-12)
    month_int = int(month)
    if month_int < 1 or month_int > 12:
        return False, f"Month '{month}' is out of valid range (01-12)"
    
    return True, None


def validate_stats_file(stats_path):
    """
    Validate the stats.min.json file for data quality issues.
    Returns (is_valid, issues_list)
    """
    issues = []
    
    try:
        with open(stats_path, 'r', encoding='utf-8') as f:
            stats = json.load(f)
    except FileNotFoundError:
        return False, [f"Stats file not found: {stats_path}"]
    except json.JSONDecodeError as e:
        return False, [f"Invalid JSON in stats file: {e}"]
    except (PermissionError, OSError) as e:
        return False, [f"Error reading stats file: {e}"]
    
    # Check if required keys exist
    if "urls" not in stats:
        issues.append("Missing 'urls' key in stats file")
        return False, issues
    
    urls = stats.get("urls", {})
    if "date" not in urls:
        issues.append("Missing 'date' key in stats.urls")
        return False, issues
    
    # Validate date entries
    date_entries = urls.get("date", [])
    for entry in date_entries:
        if not isinstance(entry, (list, tuple)) or len(entry) != 2:
            issues.append(f"Invalid date entry format: {entry}")
            continue
        
        year, months = entry
        
        # Validate year
        is_valid, error_msg = validate_year_format(year)
        if not is_valid:
            issues.append(f"Date entry issue - {error_msg}")
        
        # Validate months
        if not isinstance(months, list):
            issues.append(f"Invalid months format for year '{year}': expected list, got {type(months).__name__}")
            continue
        
        for month_entry in months:
            if not isinstance(month_entry, (list, tuple)) or len(month_entry) < 1:
                issues.append(f"Invalid month entry format for year '{year}': {month_entry}")
                continue
            
            month = month_entry[0]
            
            # Validate month
            is_valid, error_msg = validate_month_format(month)
            if not is_valid:
                issues.append(f"Date entry issue - Year '{year}', {error_msg}")
    
    return len(issues) == 0, issues


def main():
    """Main function to run validation."""
    if len(sys.argv) < 2:
        print("Usage: python validate_stats.py <path_to_stats.min.json>")
        sys.exit(1)
    
    stats_path = sys.argv[1]
    is_valid, issues = validate_stats_file(stats_path)
    
    if is_valid:
        print("✓ Validation passed: No issues found in stats.min.json")
        sys.exit(0)
    else:
        print("✗ Validation failed: Issues found in stats.min.json\n")
        print("Issues detected:")
        for i, issue in enumerate(issues, 1):
            print(f"{i}. {issue}")
        sys.exit(1)


if __name__ == "__main__":
    main()
