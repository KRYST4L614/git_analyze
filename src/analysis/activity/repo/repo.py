# src/analysis/activity/repo/repo.py
from contextlib import contextmanager
from typing import List, Dict, Any

import pandas as pd
from sqlalchemy import text

from src.storage.unit_of_work import UnitOfWork
from src.analysis.activity.models.models import (
    RepositoryYearActivityStats,
    RepositoryWeeklyActivityAnomaly,
)


class ActivityRepository:
    """
    Repository for loading commit data and persisting activity analysis results.
    """

    def __init__(self, database_url: str | None = None):
        self.database_url = database_url
        # Reuse the same engine / session factory via a UnitOfWork instance
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

    # ----------------------- LOADERS -----------------------

    def load_corporate_commits(self) -> pd.DataFrame:
        """
        Load commit_date and repo_id for repositories classified as 'corporate'.

        Uses repository_clustering_results + repository_types to filter by type.
        Returns a pandas DataFrame with columns: repo_id, commit_date.
        """
        with self.session_scope() as session:
            # We join commits with repository_clustering_results and repository_types
            # and filter for corporate repositories.
            query = text(
                """
                SELECT
                    c.repo_id,
                    c.commit_date
                FROM commits c
                JOIN repository_clustering_results rcr
                    ON rcr.repo_id = c.repo_id
                JOIN repository_types rt
                    ON rt.id = rcr.repo_type_id
                WHERE
                    rt.name = 'corporate'
                    AND c.commit_date IS NOT NULL
                """
            )

            df = pd.read_sql(query, session.connection())
            print(f"[ActivityRepository] Loaded {len(df)} corporate commits")
            return df

    # ----------------------- SAVERS -----------------------

    def save_year_stats(self, stats_list: List[Dict[str, Any]]) -> None:
        """
        Save a list of yearly stats for repositories.
        """
        if not stats_list:
            return

        with self.session_scope() as session:
            for stats in stats_list:
                row = RepositoryYearActivityStats(
                    repo_id=stats["repo_id"],
                    year=stats["year"],
                    total_commits=stats["total_commits"],
                    active_weeks=stats["active_weeks"],
                    first_week=stats["first_week"],
                    last_week=stats["last_week"],
                    weeks_in_range=stats["weeks_in_range"],
                    active_weeks_ratio=stats["active_weeks_ratio"],
                    is_alive=stats["is_alive"],
                )
                session.add(row)

            print(
                f"[ActivityRepository] Saved {len(stats_list)} "
                "RepositoryYearActivityStats rows"
            )

    def save_weekly_anomalies(self, anomalies: List[Dict[str, Any]]) -> None:
        """
        Save a list of weekly anomalies.
        """
        if not anomalies:
            return

        with self.session_scope() as session:
            for a in anomalies:
                row = RepositoryWeeklyActivityAnomaly(
                    repo_id=a["repo_id"],
                    year=a["year"],
                    iso_week=a["iso_week"],
                    commit_count=a["commit_count"],
                    z_score=a["z_score"],
                    direction=a["direction"],
                    total_commits_year=a["total_commits_year"],
                    active_weeks_year=a["active_weeks_year"],
                )
                session.add(row)

            print(
                f"[ActivityRepository] Saved {len(anomalies)} "
                "RepositoryWeeklyActivityAnomaly rows"
            )
