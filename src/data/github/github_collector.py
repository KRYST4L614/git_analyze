import concurrent.futures
import re
import time
from threading import Lock

from .github_client import GitHubClient
from src.utils.utils import format_date, clean_message, safe_lower


class GitHubDatasetCollector:
    def __init__(
            self,
            token=None,
            max_workers=50,
            max_contributors=50,
            min_contributions=100,
            max_repos=30,
            min_commits_per_repo=5,
            max_commits_per_user=1000,
    ):
        self.__client = GitHubClient(token)
        self.__max_workers = max_workers
        self.__max_contributors = max_contributors
        self.__min_contributions = min_contributions
        self.__max_repos = max_repos
        self.__min_commits_per_repo = min_commits_per_repo
        self.__max_commits_per_user = max_commits_per_user
        self.__lock = Lock()

        # Databases for classification
        self.corporate_owners = {
            'google', 'microsoft', 'facebook', 'apple', 'amazon', 'netflix',
            'twitter', 'linkedin', 'uber', 'airbnb', 'spotify', 'docker',
            'mozilla', 'adobe', 'oracle', 'ibm', 'intel', 'nvidia', 'github',
            'apache', 'kubernetes', 'elastic', 'mongodb', 'redis'
        }

        self.educational_keywords = {
            'university', 'college', 'edu', 'academy', 'school', 'course',
            'tutorial', 'learning', 'bootcamp', 'curriculum', 'assignment',
            'homework', 'student', 'coursera', 'udemy', 'udacity', 'edx',
            'lab-', 'project-', 'exercise', 'workshop', 'training'
        }

        # Keywords for identifying non-programming repositories
        self.non_tech_keywords = {
            'book', 'books', 'paper', 'papers', 'article', 'articles',
            'curriculum', 'syllabus', 'lecture', 'lectures', 'notes',
            'resource', 'resources', 'list', 'awesome', 'collection',
            'interview', 'interview-questions', 'cheatsheet', 'cheat-sheet',
            'guide', 'tutorial', 'learning', 'study', 'studying',
            'blog', 'blog-posts', 'writing', 'documentation', 'roadmap', "public API\'s"
        }

    def collect_repos(self):
        """Main collect method with parallel processing"""
        print("Getting popular TECHNICAL repositories...")
        print(
            f"Filtering: skipping repositories without programming language, content collections and with <"
            f" {self.__min_commits_per_repo} commits")

        repositories = self.__get_popular_repositories(self.__max_repos, self.__min_commits_per_repo)

        all_results = []
        total_contributors = 0

        print(f"\nStarting parallel analysis of {len(repositories)} TECHNICAL repositories...")

        all_contributor_data = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            future_to_repo = {executor.submit(self.__analyze_repository_contributors, repo): repo for repo in
                              repositories}

            for future in concurrent.futures.as_completed(future_to_repo):
                try:
                    contributor_data = future.result()
                    all_contributor_data.extend(contributor_data)
                    total_contributors += len(contributor_data)
                except Exception as e:
                    print(f"Error analyzing repository: {e}")

        print(f"\nTotal contributors collected for processing: {total_contributors}")

        print("Parallel processing of contributors and getting commits...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.__max_workers) as executor:
            futures = [executor.submit(self.__process_single_contributor, data) for data in all_contributor_data]

            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    results = future.result()
                    if not results:
                        continue
                    all_results.extend(results)

                    if i % 5 == 0:  # Less frequent progress updates due to longer processing time
                        print(f"Processed contributors: {i + 1}/{len(futures)}")

                except Exception as e:
                    print(f"Error processing contributor: {e}")

        return all_results

    def is_technical_repository(self, repo_data):
        """Check if repository is technical (has programming language)"""
        language = repo_data.get('language')
        if not language or language in ["Markdown", "HTML"]:
            print(f"Skipped: no programming language - {repo_data['full_name']}")
            return False

        description = safe_lower(repo_data.get('description', ''))
        repo_name = safe_lower(repo_data.get('name', ''))

        repo_text = f"{repo_name} {description}"

        non_tech_indicators = sum(1 for keyword in self.non_tech_keywords if keyword in repo_text)
        if non_tech_indicators >= 2:
            print(f"Skipped: non-technical content - {repo_data['full_name']}")
            return False

        if self.__is_likely_non_tech(repo_data):
            print(f"Skipped: likely non-technical - {repo_data['full_name']}")
            return False

        return True

    def __is_likely_non_tech(self, repo_data):
        """Check if repository is likely non-technical"""
        full_name = safe_lower(repo_data.get('full_name', ''))
        description = safe_lower(repo_data.get('description', ''))

        known_non_tech_repos = {
            'awesome', 'awesome-list', 'interview', 'books', 'paper',
            'curriculum', 'syllabus', 'lecture-notes', 'javascript-algorithms'
        }

        for non_tech_repo in known_non_tech_repos:
            if non_tech_repo in full_name:
                return True

        non_tech_patterns = [
            r'awesome.*list',
            r'curriculum',
            r'syllabus',
            r'lecture.*notes',
            r'interview.*questions',
            r'book.*collection'
        ]

        text_to_check = f"{full_name} {description}"
        for pattern in non_tech_patterns:
            if re.search(pattern, text_to_check):
                return True

        return False

    def __determine_repo_type(self, repo_data):
        """Determine repository type"""
        owner = repo_data.get('owner', {})
        owner_login = safe_lower(owner.get('login', ''))
        organization = safe_lower(repo_data.get('organization', ''))
        repo_name = safe_lower(repo_data.get('name', ''))
        description = safe_lower(repo_data.get('description', ''))

        if owner_login in self.corporate_owners or (organization is not None and len(organization) > 0):
            return "corporate"

        repo_text = f"{repo_name} {description}"
        edu_indicators = sum(1 for keyword in self.educational_keywords if keyword in repo_text)
        if edu_indicators >= 2:
            return "educational"

        topics = repo_data.get('topics', [])
        if topics:
            topics_lower = [safe_lower(topic) for topic in topics]
            edu_topics = {'education', 'learning', 'tutorial', 'course', 'students', 'labs'}
            if any(topic in topics_lower for topic in edu_topics):
                return "educational"

        return "open_source"

    def __get_all_contributors(self, owner, repo):
        """Get all contributors with pagination and minimum contributions filtering"""
        contributors = []
        page = 1

        while len(contributors) < self.__max_contributors:
            url = f"{self.__client.base_url}/repos/{owner}/{repo}/contributors"
            params = {
                "page": page,
                "per_page": 100,
                "anon": "0"
            }

            response = self.__client.make_request(url, params=params)
            if response.status_code != 200:
                break

            page_contributors = response.json()
            if not page_contributors:
                break

            # Filter contributors by minimum commit count
            filtered_contributors = [
                contributor for contributor in page_contributors
                if contributor.get('contributions', 0) >= self.__min_contributions
            ]

            contributors.extend(filtered_contributors)

            if len(page_contributors) < 100:
                break

            page += 1
            time.sleep(0.1)

        print(f"Contributors after filtering: {len(contributors)} (minimum {self.__min_contributions} commits)")
        return contributors[:self.__max_contributors]

    def __get_user_commits(self, owner, repo, username, max_commits=5):
        """Get commits by specific user in repository"""
        commits = []
        page = 1

        while len(commits) < max_commits:
            url = f"{self.__client.base_url}/repos/{owner}/{repo}/commits"
            params = {
                "author": username,
                "page": page,
                "per_page": 10
            }

            response = self.__client.make_request(url, params=params)
            if response.status_code != 200:
                break

            page_commits = response.json()
            if not page_commits:
                break

            for commit in page_commits:
                if isinstance(commit, dict) and 'commit' in commit:
                    commit_info = commit['commit']
                    author_info = commit_info.get('author', {})

                    raw_date = author_info.get('date', '')
                    formatted_date = format_date(raw_date)

                    commits.append({
                        'sha': commit.get('sha', '')[:8],  # Take only first 8 characters
                        'date': formatted_date,
                        'message': clean_message(commit_info.get('message', ''))
                    })

                    if len(commits) >= max_commits:
                        break

            if len(page_commits) < 10:
                break

            page += 1
            time.sleep(0.2)

        return commits

    def __get_popular_repositories(self, count=30, min_commits=1000):
        """Get most popular repositories (only technical with minimum commit count)"""
        repos = []
        page = 1
        per_page = min(50, count * 3)  # Take more because we'll filter

        while len(repos) < count:
            url = f"{self.__client.base_url}/search/repositories"
            params = {
                "q": "stars:1000..66739 -language:HTML -language:TypeScript -language:Markdown",
                "sort": "stars",
                "order": "desc",
                "page": page,
                "per_page": per_page
            }

            response = self.__client.make_request(url, params=params, is_search=True)
            if response.status_code != 200:
                break

            data = response.json()
            if "items" not in data:
                break

            for repo in data["items"]:
                if not self.is_technical_repository(repo):
                    continue

                owner = repo['owner']['login']
                repo_name = repo['name']
                print(f"Checking commits for {owner}/{repo_name}...")

                commit_count = self.__client.get_commit_count(owner, repo_name)
                repo['commit_count'] = commit_count

                if commit_count >= min_commits:
                    repos.append(repo)
                    print(
                        f"Added repository: {repo['full_name']} ({commit_count} commits,"
                        f" {repo.get('language', 'No language')}, stars: {repo.get('stargazers_count', 0)})")
                else:
                    print(f"Skipped: too few commits ({commit_count} < {min_commits}) - {repo['full_name']}")

                if len(repos) >= count:
                    break

            if len(data["items"]) < per_page:
                break

            page += 1
            time.sleep(1)

            print(f"Found suitable repositories: {len(repos)}/{count}")

        return repos[:count]

    def __process_single_contributor(self, contributor_data):
        """Process single contributor in separate thread"""
        repo_info, contributor = contributor_data
        results = []

        if isinstance(contributor, dict) and 'login' in contributor:
            username = contributor['login']
            owner = repo_info['owner_login']
            repo_name = repo_info['repo_name'].split('/')[1] if '/' in repo_info['repo_name'] else repo_info[
                'repo_name']

            user_info = self.__client.get_user_info(username)
            location = user_info.get('location', 'Unknown')
            if not location:
                return []

            print(f"    Getting commits for {username}...")
            commits = self.__get_user_commits(owner, repo_name, username, self.__max_commits_per_user)

            if commits:
                for commit in commits:
                    result = {
                        'repo_id': repo_info['id'],
                        'repo_name': repo_info['repo_name'],
                        'repo_type': repo_info['repo_type'],
                        'stars': repo_info['stargazers_count'],
                        'contributor_login': username,
                        'contributor_location': location,
                        'contributions': contributor.get('contributions', 0),
                        'commit_sha': commit['sha'],
                        'commit_date': commit['date'],
                    }
                    results.append(result)
            else:
                result = {
                    'repo_id': repo_info['id'],
                    'repo_name': repo_info['repo_name'],
                    'repo_type': repo_info['repo_type'],
                    'stars': repo_info['stargazers_count'],
                    'contributor_login': username,
                    'contributor_location': location,
                    'contributions': contributor.get('contributions', 0),
                    'commit_sha': 'N/A',
                    'commit_date': 'N/A',
                }
                results.append(result)

            with self.__lock:
                print(f"    Processed contributor: {username} ({len(commits)} commits)")

        return results

    def __analyze_repository_contributors(self, repo):
        """Analyze single repository and get all its contributors"""
        repo_id = repo['id']
        repo_name = repo['full_name']
        owner = repo['owner']['login']
        commit_count = repo.get('commit_count', 0)

        print(f"Analyzing repository: {repo_name} ({repo.get('language', 'No language')}, {commit_count} commits)")

        repo_type = self.__determine_repo_type(repo)
        print(f"  Type: {repo_type}")

        stars = repo['stargazers_count']
        print(f"  Stars: {stars}")

        contributors = self.__get_all_contributors(owner, repo['name'])
        print(f"  Found contributors: {len(contributors)}")

        repo_info = {
            'id': repo_id,
            'repo_name': repo_name,
            'repo_type': repo_type,
            'stars': stars,
            'owner_login': owner,
            'stargazers_count': repo.get('stargazers_count', 0),
            'commit_count': commit_count
        }

        contributor_data = [(repo_info, contributor) for contributor in contributors]

        return contributor_data
