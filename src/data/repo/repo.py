from contextlib import contextmanager
from sqlalchemy.dialects.postgresql import insert
from src.storage.unit_of_work import UnitOfWork
from src.data.models.models import Repository, Contributor, Commit, RepositoryContributor


class BaseRepository:
    def __init__(self, database_url: str = None):
        self.database_url = database_url
        self._uow = None

    @property
    def uow(self):
        if self._uow is None:
            self._uow = UnitOfWork(self.database_url)
        return self._uow

    @uow.setter
    def uow(self, value):
        self._uow = value

    @contextmanager
    def session_scope(self):
        """Контекстный менеджер сессии с общим UnitOfWork"""
        session = self.uow.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


class RepositoryRepository(BaseRepository):
    def __init__(self, database_url: str = None):
        super().__init__(database_url)

    def upsert_repository(self, repo_data):
        with self.session_scope() as session:
            stmt = insert(Repository).values(
                id=repo_data['id'],
                name=repo_data['name'],
                full_name=repo_data['full_name'],
                description=repo_data.get('description'),
                language=repo_data.get('language'),
                stargazers_count=repo_data.get('stargazers_count', 0),
                forks_count=repo_data.get('forks_count', 0),
                watchers_count=repo_data.get('watchers_count', 0),
                open_issues_count=repo_data.get('open_issues_count', 0),
                size=repo_data.get('size', 0),
                default_branch=repo_data.get('default_branch'),
                created_at=repo_data.get('created_at'),
                updated_at=repo_data.get('updated_at'),
                pushed_at=repo_data.get('pushed_at'),
                homepage=repo_data.get('homepage'),
                topics=repo_data.get('topics', []),
                owner_id=repo_data.get('owner_id'),
                owner_login=repo_data.get('owner_login'),
                owner_type=repo_data.get('owner_type'),
                is_fork=repo_data.get('is_fork', False),
                has_issues=repo_data.get('has_issues', False),
                has_projects=repo_data.get('has_projects', False),
                has_downloads=repo_data.get('has_downloads', False),
                has_wiki=repo_data.get('has_wiki', False),
                has_pages=repo_data.get('has_pages', False),
                archived=repo_data.get('archived', False),
                disabled=repo_data.get('disabled', False)
            ).on_conflict_do_update(
                index_elements=['id'],
                set_={
                    'stargazers_count': repo_data.get('stargazers_count', 0),
                    'forks_count': repo_data.get('forks_count', 0),
                    'updated_at': repo_data.get('updated_at'),
                    'pushed_at': repo_data.get('pushed_at'),
                }
            )
            session.execute(stmt)


class ContributorRepository(BaseRepository):
    def __init__(self, database_url: str = None):
        super().__init__(database_url)

    def exists(self, contributor_id: int) -> bool:
        with self.session_scope() as session:
            count = session.query(Contributor).filter(Contributor.id == contributor_id).count()
            return count > 0

    def get_by_login(self, contributor_id: int) -> Contributor | None:
        with self.session_scope() as session:
            return session.query(Contributor).filter(Contributor.id == contributor_id).first()

    def upsert_contributor(self, user_data, repo_id, contributions):
        with self.session_scope() as session:
            # UPSERT contributor
            stmt = insert(Contributor).values(
                id=user_data['id'],
                login=user_data.get('login'),
                type=user_data.get('type'),
                site_admin=user_data.get('site_admin', False),
                name=user_data.get('name'),
                company=user_data.get('company'),
                blog=user_data.get('blog'),
                location=user_data.get('location'),
                email=user_data.get('email'),
                hireable=user_data.get('hireable', False),
                bio=user_data.get('bio'),
                twitter_username=user_data.get('twitter_username'),
                public_repos=user_data.get('public_repos', 0),
                public_gists=user_data.get('public_gists', 0),
                followers=user_data.get('followers', 0),
                following=user_data.get('following', 0),
                created_at=user_data.get('created_at'),
                updated_at=user_data.get('updated_at')
            ).on_conflict_do_update(
                index_elements=['id'],
                set_={
                    'public_repos': user_data.get('public_repos', 0),
                    'followers': user_data.get('followers', 0),
                    'following': user_data.get('following', 0),
                    'updated_at': user_data.get('updated_at'),
                }
            )
            session.execute(stmt)

            # UPSERT relationship
            stmt = insert(RepositoryContributor).values(
                repo_id=repo_id,
                contributor_id=user_data['id'],
                contributions=contributions
            ).on_conflict_do_update(
                index_elements=['repo_id', 'contributor_id'],
                set_={'contributions': contributions}
            )
            session.execute(stmt)


class CommitRepository(BaseRepository):
    def __init__(self, database_url: str = None):
        super().__init__(database_url)

    def upsert_commit(self, commit_data, repo_id, author_id):
        with self.session_scope() as session:
            stmt = insert(Commit).values(
                sha=commit_data.get('sha'),
                repo_id=repo_id,
                author_id=author_id,
                author_login=commit_data.get('author_login'),
                committer_id=commit_data.get('committer_id'),
                committer_login=commit_data.get('committer_login'),
                message=commit_data.get('message', '')[:5000],
                author_date=commit_data.get('author_date'),
                commit_date=commit_data.get('commit_date'),
                url=commit_data.get('url'),
                comment_count=commit_data.get('comment_count', 0),
                verification_verified=commit_data.get('verification_verified', False),
                verification_reason=commit_data.get('verification_reason', ''),
                additions=commit_data.get('additions', 0),
                deletions=commit_data.get('deletions', 0),
                total_changes=commit_data.get('total_changes', 0),
                files_changed=commit_data.get('files_changed', []),
                parent_shas=commit_data.get('parent_shas', [])
            ).on_conflict_do_update(
                index_elements=['sha'],
                set_={
                    'additions': commit_data.get('additions', 0),
                    'deletions': commit_data.get('deletions', 0),
                    'total_changes': commit_data.get('total_changes', 0),
                    'files_changed': commit_data.get('files_changed', []),
                }
            )
            session.execute(stmt)