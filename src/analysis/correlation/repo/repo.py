# src/analysis/correlation/repo/repo.py
from contextlib import contextmanager

from sqlalchemy import text
import pandas as pd

from src.storage.unit_of_work import UnitOfWork
from src.analysis.correlation.models.models import CommitCorrelationResult


class CorrelationRepository:
    def __init__(self, database_url: str | None = None):
        self.database_url = database_url
        # Reuse one UnitOfWork / engine
        self._uow = UnitOfWork(self.database_url)

    @contextmanager
    def session_scope(self):
        session = self._uow.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ----------- LOADERS -----------

    def load_commits(self) -> pd.DataFrame:
        """
        Load minimal commit info:
        repo_id, commit_date, message.
        """
        with self.session_scope() as session:
            query = text("""
                SELECT
                    repo_id,
                    commit_date,
                    message
                FROM commits
                WHERE commit_date IS NOT NULL
            """)
            df = pd.read_sql(query, session.connection())
            print(f"[CorrelationRepository] Loaded {len(df)} commits")
            return df

    def load_pull_requests(self) -> pd.DataFrame:
        """
        Load minimal PR info needed for lead time:
        - id
        - repo_id
        - number
        - created_at (for start of lead time)
        - merged_at (optional; we’ll use commit_date as end)
        """
        with self.session_scope() as session:
            query = text("""
                SELECT
                    id AS pr_id,
                    repo_id,
                    number AS pr_number,
                    author_id AS pr_author_id,
                    created_at AS pr_created_at,
                    merged_at AS pr_merged_at
                FROM pull_requests
                WHERE created_at IS NOT NULL
            """)
            df = pd.read_sql(query, session.connection())
            print(f"[CorrelationRepository] Loaded {len(df)} pull_requests")
            return df

    # ----------- SAVER -----------

    def save_correlation_result(self, result_dict: dict) -> None:
        """
        Save a correlation result row for a single repository.
        We KEEP history; no deletes.
        """
        with self.session_scope() as session:
            row = CommitCorrelationResult(
                repo_id=result_dict.get("repo_id"),
                commit_corr_week_of_year=result_dict.get("commit_corr_week_of_year"),
                commit_corr_week_index=result_dict.get("commit_corr_week_index"),
                pr_corr_week_of_year=result_dict.get("pr_corr_week_of_year"),
                pr_corr_week_index=result_dict.get("pr_corr_week_index"),
                n_weeks_commits=result_dict.get("n_weeks_commits"),
                total_commits=result_dict.get("total_commits"),
                n_weeks_pr_lead=result_dict.get("n_weeks_pr_lead"),
                n_pr_merges_used=result_dict.get("n_pr_merges_used"),
            )

            session.add(row)
            print(
                f"[CorrelationRepository] Saved correlation result "
                f"to database for repo_id={row.repo_id}"
            )
