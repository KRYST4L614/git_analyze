import requests
import csv
import time
from datetime import datetime
import os
import re
import concurrent.futures
from threading import Lock


class GitHubAnalyzer:
    def __init__(self, token=None, max_workers=5):
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
        self.max_workers = max_workers
        self.lock = Lock()

        # Базы данных для классификации
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

        # Ключевые слова для идентификации не-программерских репозиториев
        self.non_tech_keywords = {
            'book', 'books', 'paper', 'papers', 'article', 'articles',
            'curriculum', 'syllabus', 'lecture', 'lectures', 'notes',
            'resource', 'resources', 'list', 'awesome', 'collection',
            'interview', 'interview-questions', 'cheatsheet', 'cheat-sheet',
            'guide', 'tutorial', 'learning', 'study', 'studying',
            'blog', 'blog-posts', 'writing', 'documentation', 'roadmap', "public API\'s"
        }

        # Счетчики лимитов
        self.search_remaining = 30
        self.core_remaining = 5000

    def get_commit_count(self, owner, repo):
        """Получить общее количество коммитов в репозитории"""
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

    def is_technical_repository(self, repo_data):
        """Проверить, является ли репозиторий техническим (имеет язык программирования)"""
        language = repo_data.get('language')
        if not language or language in ["Markdown", "HTML"]:
            print(f"Пропуск: нет языка программирования - {repo_data['full_name']}")
            return False

        description = self.safe_lower(repo_data.get('description', ''))
        repo_name = self.safe_lower(repo_data.get('name', ''))

        repo_text = f"{repo_name} {description}"

        non_tech_indicators = sum(1 for keyword in self.non_tech_keywords if keyword in repo_text)
        if non_tech_indicators >= 2:
            print(f"Пропуск: не-технический контент - {repo_data['full_name']}")
            return False

        if self.is_likely_non_tech(repo_data):
            print(f"Пропуск: вероятно не-технический - {repo_data['full_name']}")
            return False

        return True

    def is_likely_non_tech(self, repo_data):
        """Проверить, является ли репозиторий вероятно не-техническим"""
        full_name = self.safe_lower(repo_data.get('full_name', ''))
        description = self.safe_lower(repo_data.get('description', ''))

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

    def safe_lower(self, text):
        """Безопасное преобразование в нижний регистр"""
        if text is None:
            return ""
        return str(text).lower()

    def determine_repo_type(self, repo_data):
        """Определить тип репозитория"""
        owner = repo_data.get('owner', {})
        owner_login = self.safe_lower(owner.get('login', ''))
        organization = self.safe_lower(repo_data.get('organization', ''))
        repo_name = self.safe_lower(repo_data.get('name', ''))
        description = self.safe_lower(repo_data.get('description', ''))

        if owner_login in self.corporate_owners or (organization is not None and len(organization) > 0):
            return "corporate"

        repo_text = f"{repo_name} {description}"
        edu_indicators = sum(1 for keyword in self.educational_keywords if keyword in repo_text)
        if edu_indicators >= 2:
            return "educational"

        topics = repo_data.get('topics', [])
        if topics:
            topics_lower = [self.safe_lower(topic) for topic in topics]
            edu_topics = {'education', 'learning', 'tutorial', 'course', 'students'}
            if any(topic in topics_lower for topic in edu_topics):
                return "educational"

        return "individual"

    def wait_for_rate_limit(self, reset_time):
        """Ожидание сброса лимита"""
        current_time = time.time()
        sleep_time = max(reset_time - current_time, 0) + 10  # +10 секунд для надежности

        if sleep_time > 300:  # Если ожидание больше 5 минут
            print(f"⏳ Длительное ожидание: {sleep_time / 60:.1f} минут")
            # Разбиваем ожидание на части с прогрессом
            for i in range(int(sleep_time / 60)):
                remaining = sleep_time - i * 60
                print(f"   Осталось: {remaining / 60:.1f} минут")
                time.sleep(60)
            time.sleep(sleep_time % 60)
        else:
            print(f"⏳ Ожидание: {sleep_time:.0f} секунд")
            time.sleep(sleep_time)

    def make_request(self, url, params=None, is_search=False):
        """Безопасный запрос с проверкой rate limit"""
        try:
            response = self.session.get(url, params=params, timeout=30)

            # Обновляем лимиты из заголовков
            if 'X-RateLimit-Remaining' in response.headers:
                remaining = int(response.headers['X-RateLimit-Remaining'])
                if is_search:
                    self.search_remaining = remaining
                else:
                    self.core_remaining = remaining

            if response.status_code == 403:
                # Проверяем, действительно ли это rate limit, а не другая ошибка 403
                rate_limit_remaining = response.headers.get('X-RateLimit-Remaining')
                reset_time = response.headers.get('X-RateLimit-Reset')

                if rate_limit_remaining == '0' and reset_time:
                    # Это действительно превышение rate limit
                    reset_time = int(reset_time)
                    limit_type = "Search API" if is_search else "Core API"
                    print(f"🚫 {limit_type} limit превышен! Осталось запросов: {rate_limit_remaining}")
                    self.wait_for_rate_limit(reset_time)
                    return self.make_request(url, params, is_search)
                else:
                    # Это другая ошибка 403 (например, нет доступа)
                    print(f"🚫 Ошибка 403: Нет доступа к {url}")
                    print(f"   Response: {response.text[:200]}...")
                    return response

            elif response.status_code == 422:
                print(f"⚠️  Ошибка 422 (Unprocessable Entity) для {url}")
                return response

            elif response.status_code != 200:
                print(f"⚠️  Ошибка {response.status_code} для {url}")
                print(f"   Response: {response.text[:200]}...")
                return response

            return response

        except requests.exceptions.RequestException as e:
            print(f"Ошибка запроса: {e}")
            time.sleep(5)
            return self.make_request(url, params, is_search)

    def get_all_contributors(self, owner, repo, max_contributors=50, min_contributions=100):
        """Получить всех контрибьюторов с пагинацией и фильтрацией по минимальному количеству коммитов"""
        contributors = []
        page = 1

        while len(contributors) < max_contributors:
            url = f"{self.base_url}/repos/{owner}/{repo}/contributors"
            params = {
                "page": page,
                "per_page": 100,
                "anon": "0"
            }

            response = self.make_request(url, params=params)
            if response.status_code != 200:
                break

            page_contributors = response.json()
            if not page_contributors:
                break

            # Фильтруем контрибьюторов по минимальному количеству коммитов
            filtered_contributors = [
                contributor for contributor in page_contributors
                if contributor.get('contributions', 0) >= min_contributions
            ]

            contributors.extend(filtered_contributors)

            if len(page_contributors) < 100:
                break

            page += 1
            time.sleep(0.1)

        print(
            f"После фильтрации осталось контрибьюторов: {len(contributors)} (минимум {min_contributions} коммитов)")
        return contributors[:max_contributors]
    def get_user_commits(self, owner, repo, username, max_commits=5):
        """Получить коммиты конкретного пользователя в репозитории"""
        commits = []
        page = 1

        while len(commits) < max_commits:
            url = f"{self.base_url}/repos/{owner}/{repo}/commits"
            params = {
                "author": username,
                "page": page,
                "per_page": 10
            }

            response = self.make_request(url, params=params)
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
                    formatted_date = self.format_date(raw_date)

                    commits.append({
                        'sha': commit.get('sha', '')[:8],  # Берем только первые 8 символов
                        'date': formatted_date,
                        'message': self.clean_commit_message(commit_info.get('message', ''))
                    })

                    if len(commits) >= max_commits:
                        break

            if len(page_commits) < 10:
                break

            page += 1
            time.sleep(0.2)

        return commits

    def format_date(self, date_string):
        """Форматировать дату в читаемый вид"""
        if not date_string:
            return "N/A"

        try:
            dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return date_string

    def clean_commit_message(self, message):
        """Очистить сообщение коммита от лишних символов"""
        if not message:
            return "N/A"

        cleaned = re.sub(r'\s+', ' ', message.strip())
        if len(cleaned) > 200:
            cleaned = cleaned[:197] + "..."

        return cleaned

    def get_popular_repositories(self, count=30, min_commits=1000):
        """Получить самые популярные репозитории (только технические с минимальным количеством коммитов)"""
        repos = []
        page = 1
        per_page = min(50, count * 3)  # Берем больше, т.к. будем фильтровать

        while len(repos) < count:
            url = f"{self.base_url}/search/repositories"
            params = {
                "q": "stars:>1000",
                "sort": "stars",
                "order": "desc",
                "page": page,
                "per_page": per_page
            }

            response = self.make_request(url, params=params)
            if response.status_code != 200:
                break

            data = response.json()
            if "items" not in data:
                break

            for repo in data["items"]:
                # Проверяем технический репозиторий
                if not self.is_technical_repository(repo):
                    continue

                # Проверяем количество коммитов
                owner = repo['owner']['login']
                repo_name = repo['name']
                print(f"  🔍 Проверка коммитов для {owner}/{repo_name}...")

                commit_count = self.get_commit_count(owner, repo_name)
                repo['commit_count'] = commit_count

                if commit_count >= min_commits:
                    repos.append(repo)
                    print(
                        f"Добавлен репозиторий: {repo['full_name']} ({commit_count} коммитов, {repo.get('language', 'No language')})")
                else:
                    print(f"Пропущен: мало коммитов ({commit_count} < {min_commits}) - {repo['full_name']}")

                if len(repos) >= count:
                    break

            if len(data["items"]) < per_page:
                break

            page += 1
            time.sleep(1)

            print(f"Найдено подходящих репозиториев: {len(repos)}/{count}")

        return repos[:count]

    def process_single_contributor(self, contributor_data):
        """Обработать одного контрибьютора в отдельном потоке"""
        repo_info, contributor = contributor_data
        results = []

        if isinstance(contributor, dict) and 'login' in contributor:
            username = contributor['login']
            owner = repo_info['owner_login']
            repo_name = repo_info['repo_name'].split('/')[1] if '/' in repo_info['repo_name'] else repo_info[
                'repo_name']

            user_info = self.get_user_info(username)
            location = user_info.get('location', 'Unknown')
            if not location:
                return []

            print(f"    Получение коммитов для {username}...")
            commits = self.get_user_commits(owner, repo_name, username, max_commits=1000)

            if commits:
                for commit in commits:
                    result = {
                        'repo_id': repo_info['id'],
                        'repo_name': repo_info['repo_name'],
                        'repo_type': repo_info['repo_type'],
                        'contributor_login': username,
                        'contributor_location': location,
                        'contributions': contributor.get('contributions', 0),
                        'commit_sha': commit['sha'],
                        'commit_date': commit['date'],
                        'commit_message': commit['message']
                    }
                    results.append(result)
            else:
                # Если коммитов нет, создаем одну запись без информации о коммитах
                result = {
                    'repo_id': repo_info['id'],
                    'repo_name': repo_info['repo_name'],
                    'repo_type': repo_info['repo_type'],
                    'contributor_login': username,
                    'contributor_location': location,
                    'contributions': contributor.get('contributions', 0),
                    'commit_sha': 'N/A',
                    'commit_date': 'N/A',
                    'commit_message': 'N/A'
                }
                results.append(result)

            with self.lock:
                print(f"    Обработан контрибьютор: {username} ({len(commits)} коммитов)")

        return results

    def get_user_info(self, username):
        """Получить информацию о пользователе"""
        url = f"{self.base_url}/users/{username}"
        response = self.make_request(url)
        if response.status_code == 200:
            return response.json()
        return {"location": "Unknown"}

    def analyze_repository_contributors(self, repo):
        """Проанализировать один репозиторий и получить всех его контрибьюторов"""
        repo_id = repo['id']
        repo_name = repo['full_name']
        owner = repo['owner']['login']
        commit_count = repo.get('commit_count', 0)

        print(f"Анализ репозитория: {repo_name} ({repo.get('language', 'No language')}, {commit_count} коммитов)")

        repo_type = self.determine_repo_type(repo)
        print(f"  Тип: {repo_type}")

        contributors = self.get_all_contributors(owner, repo['name'], max_contributors=50)
        print(f"  Найдено контрибьюторов: {len(contributors)}")

        repo_info = {
            'id': repo_id,
            'repo_name': repo_name,
            'repo_type': repo_type,
            'owner_login': owner,
            'stargazers_count': repo.get('stargazers_count', 0),
            'commit_count': commit_count
        }

        contributor_data = [(repo_info, contributor) for contributor in contributors]

        return contributor_data

    def analyze_repositories_parallel(self, repo_count=30, min_commits=1000):
        """Основной метод анализа с распараллеливанием"""
        print("Получение популярных ТЕХНИЧЕСКИХ репозиториев...")
        print(
            f"Фильтрация: пропускаем репозитории без языка программирования, сборники контента и с < {min_commits} коммитов")

        repositories = self.get_popular_repositories(repo_count, min_commits)

        all_results = []
        total_contributors = 0

        print(f"\nНачинаем параллельный анализ {len(repositories)} ТЕХНИЧЕСКИХ репозиториев...")

        all_contributor_data = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_repo = {executor.submit(self.analyze_repository_contributors, repo): repo for repo in
                              repositories}

            for future in concurrent.futures.as_completed(future_to_repo):
                try:
                    contributor_data = future.result()
                    all_contributor_data.extend(contributor_data)
                    total_contributors += len(contributor_data)
                except Exception as e:
                    print(f"Ошибка при анализе репозитория: {e}")

        print(f"\nВсего собрано контрибьюторов для обработки: {total_contributors}")

        print("Параллельная обработка контрибьюторов и получение коммитов...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.process_single_contributor, data) for data in all_contributor_data]

            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    results = future.result()
                    if not results:
                        continue
                    all_results.extend(results)

                    if i % 5 == 0:  # Реже выводим прогресс из-за большего времени обработки
                        print(f"Обработано контрибьюторов: {i + 1}/{len(futures)}")

                except Exception as e:
                    print(f"Ошибка при обработке контрибьютора: {e}")

        return all_results

    def save_to_csv(self, data, filename="github_analysis.csv"):
        """Сохранить данные в CSV"""
        if not data:
            print("Нет данных для сохранения")
            return

        fieldnames = [
            'repo_id', 'repo_name', 'repo_type', 'contributor_login',
            'contributor_location', 'contributions', 'commit_sha',
            'commit_date', 'commit_message'
        ]

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        print(f"Данные сохранены в: {filename}")
        print(f"Всего записей: {len(data)}")

        commits_with_data = len([item for item in data if item['commit_sha'] != 'N/A'])
        commits_without_data = len([item for item in data if item['commit_sha'] == 'N/A'])

        print(f"\nСТАТИСТИКА КОММИТОВ:")
        print(f"Записей с информацией о коммитах: {commits_with_data}")
        print(f"Записей без информации о коммитах: {commits_without_data}")


def main():
    token = (
            os.getenv('GITHUB_TOKEN') or
            os.getenv('GH_TOKEN') or
            input("Введите GitHub Token: ").strip()
    )

    if not token:
        print("Токен не ввндён.")
        return
    else:
        repo_count = 10
        max_workers = 5

    # Минимальное количество коммитов для фильтрации
    min_commits = 1000

    print(f"Используется {max_workers} потоков")
    print("Фильтрация: только репозитории с языками программирования")
    print(f"Дополнительная фильтрация: только репозитории с ≥ {min_commits} коммитами")
    print("Сбор данных: коммиты для каждого контрибьютора")

    analyzer = GitHubAnalyzer(token, max_workers=max_workers)

    try:
        start_time = time.time()
        print("Начинаем параллельный анализ...")

        data = analyzer.analyze_repositories_parallel(repo_count, min_commits)

        end_time = time.time()
        print(f"Анализ завершен за {end_time - start_time:.2f} секунд")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"github_commits_analysis_{timestamp}.csv"
        analyzer.save_to_csv(data, filename)

        # Общая статистика
        if data:
            unique_repos = len(set(item['repo_id'] for item in data))
            unique_contributors = len(set(item['contributor_login'] for item in data))

            print(f"\nОБЩАЯ СТАТИСТИКА:")
            print(f"Уникальных технических репозиториев: {unique_repos}")
            print(f"Уникальных контрибьюторов: {unique_contributors}")
            print(f"Всего записей: {len(data)}")

    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()