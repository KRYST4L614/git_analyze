import csv
import re
import time
from datetime import datetime

def clean_message(message):
    """Clean message from extra characters"""
    if not message:
        return "N/A"

    cleaned = re.sub(r'\s+', ' ', message.strip())
    if len(cleaned) > 200:
        cleaned = cleaned[:197] + "..."

    return cleaned


def format_date(date_string):
    """Format date to readable format"""
    if not date_string:
        return "N/A"

    try:
        dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return date_string


def safe_lower(text):
    """Safely convert text to lowercase"""
    if text is None:
        return ""
    return str(text).lower()


def save_to_csv(data, filename="github_analysis.csv"):
    """Save data to CSV"""
    if not data:
        print("No data to save")
        return

    fieldnames = [
        'repo_id', 'repo_name', 'repo_type', 'stars', 'contributor_login',
        'contributor_location', 'contributions', 'commit_sha',
        'commit_date',
    ]

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"Data saved to: {filename}")
    print(f"Total records: {len(data)}")

    commits_with_data = len([item for item in data if item['commit_sha'] != 'N/A'])
    commits_without_data = len([item for item in data if item['commit_sha'] == 'N/A'])

    print(f"\nCOMMIT STATISTICS:")
    print(f"Records with commit information: {commits_with_data}")
    print(f"Records without commit information: {commits_without_data}")
