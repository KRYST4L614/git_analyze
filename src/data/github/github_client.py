import os
import sys

import requests
import time
import re


def wait_for_rate_limit(reset_time):
    """Wait for rate limit reset"""
    current_time = time.time()
    sleep_time = max(reset_time - current_time, 0) + 5

    if sleep_time > 300:
        print(f"Long wait: {sleep_time / 60:.1f} minutes")
        for i in range(int(sleep_time / 60)):
            remaining = sleep_time - i * 60
            print(f"   Remaining: {remaining / 60:.1f} minutes")
            time.sleep(60)
        time.sleep(sleep_time % 60)
    else:
        print(f"Waiting: {sleep_time:.0f} seconds")
        time.sleep(sleep_time)


class GitHubClient:
    def __init__(self, token=None):
        self.base_url = "https://api.github.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "GitHub-Repo-Analyzer"
        }

        self.token = token
        if token:
            self.headers["Authorization"] = f"token {token}"

        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # Rate limit counters
        self.search_remaining = 30
        self.core_remaining = 5000

    def get_commit_count(self, owner, repo):
        """Get total number of commits in repository"""
        url = f"{self.base_url}/repos/{owner}/{repo}/commits"
        params = {
            "per_page": 1
        }

        response = self.make_request(url, params=params)
        if response.status_code != 200:
            return 0

        link_header = response.headers.get('Link', '')
        if link_header:
            last_match = re.search(r'page=(\d+)>; rel="last"', link_header)
            if last_match:
                return int(last_match.group(1))

        commits = response.json()
        return len(commits) if isinstance(commits, list) else 0

    def make_request(self, url, params=None, is_search=False):
        """Safe request with rate limit checking"""
        try:
            response = self.session.get(url, params=params, timeout=30)

            # Update limits from headers
            if 'X-RateLimit-Remaining' in response.headers:
                remaining = int(response.headers['X-RateLimit-Remaining'])
                if is_search:
                    self.search_remaining = remaining
                else:
                    self.core_remaining = remaining

            if response.status_code == 403:
                # Check if this is really rate limit or other 403 error
                rate_limit_remaining = response.headers.get('X-RateLimit-Remaining')
                reset_time = response.headers.get('X-RateLimit-Reset')

                if rate_limit_remaining == '0' and reset_time:
                    reset_time = int(reset_time)
                    limit_type = "Search API" if is_search else "Core API"
                    print(f"{limit_type} limit exceeded! Remaining requests: {rate_limit_remaining}")
                    wait_for_rate_limit(reset_time)
                    return self.make_request(url, params, is_search)
                else:
                    # This is another 403 error (e.g., no access)
                    print(f"Error 403: No access to {url}")
                    print(f"Response: {response.text[:200]}...")
                    return response

            elif response.status_code == 422:
                print(f"Error 422 (Unprocessable Entity) for {url}")
                return response

            elif response.status_code != 200:
                print(f"Error {response.status_code} for {url}")
                print(f"Response: {response.text[:200]}...")
                return response

            return response

        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            time.sleep(5)
            return self.make_request(url, params, is_search)

    def get_user_info(self, username):
        """Get user information"""
        url = f"{self.base_url}/users/{username}"
        response = self.make_request(url)
        if response.status_code == 200:
            return response.json()
        return {"location": "Unknown"}
