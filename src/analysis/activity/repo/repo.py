from contextlib import contextmanager

from sqlalchemy import text
import pandas as pd

from src.storage.unit_of_work import UnitOfWork
from src.analysis.activity.models.models import RepoActivityForecast


class ActivityRepository:
    """
    Data access for activity forecasting & anomaly detection.
    """

    def __init__(self, database_url: str | None = None):
        self.database_url = database_url

    @contextmanager
    def session_scope(self):
        uow = UnitOfWork(self.database_url)
        session = uow.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ----------- LOADERS -----------

    def load_daily_commit_data(self) -> pd.DataFrame:
        """
        Load aggregated commit activity per day for all repos.

        Output columns:
          - repo_id
          - activity_date (date)
          - commit_count
        """
        with self.session_scope() as session:
            query = text("""
                SELECT
                    repo_id,
                    date_trunc('day', commit_date)::date AS activity_date,
                    COUNT(*) AS commit_count
                FROM commits
                WHERE commit_date IS NOT NULL
                GROUP BY repo_id, activity_date
                ORDER BY repo_id, activity_date
            """)

            df = pd.read_sql(query, session.connection())
            print(f"[ActivityRepository] Loaded {len(df)} daily rows")
            return df

    # ----------- SAVER -----------

    def save_forecasts(self, df: pd.DataFrame):
        """
        Save DOW+MAD v3 forecast/anomaly results to DB.

        Expects df with columns:
          - repo_id
          - activity_date
          - actual
          - predicted
          - residual
          - z_score
          - is_anomaly
        """
        with self.session_scope() as session:
            # Clear previous run for simplicity.
            # If you want history, drop this delete and add a run_id column.
            session.query(RepoActivityForecast).delete()

            rows = []
            for _, row in df.iterrows():
                rows.append(
                    RepoActivityForecast(
                        repo_id=int(row["repo_id"]),
                        activity_date=row["activity_date"],
                        actual_commits=int(row["actual"]),
                        predicted_commits=float(row["predicted"])
                        if row["predicted"] is not None
                        else None,
                        residual=float(row["residual"])
                        if row["residual"] is not None
                        else None,
                        z_score=float(row["z_score"])
                        if row["z_score"] is not None
                        else None,
                        is_anomaly=bool(row["is_anomaly"]),
                        model_type="dow_mad_v3",
                        seasonal_periods=None,
                    )
                )

            session.add_all(rows)
            print(f"[ActivityRepository] Saved {len(rows)} DOW+MAD v3 rows")
