from sqlalchemy import Column, Integer, Float, Date, DateTime, Boolean, String
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class RepoActivityForecast(Base):
    """
    Daily activity forecast + anomaly flags (DOW+MAD v3).

    Semantics:
      - activity_date: date (per day, per repo)
      - actual_commits: observed commit count that day
      - predicted_commits: DOW-based baseline (expected commits for that day)
      - residual: actual_commits - predicted_commits
      - z_score: robust modified z-score (based on log1p counts + MAD)
      - is_anomaly: True if |z_score| >= threshold and |residual| >= min_abs_diff
      - model_type: 'dow_mad_v3'
      - seasonal_periods: unused (kept for backward DB compatibility)
    """

    __tablename__ = "repo_activity_forecast"

    id = Column(Integer, primary_key=True, autoincrement=True)

    repo_id = Column(Integer, nullable=False, index=True)
    activity_date = Column(Date, nullable=False, index=True)

    actual_commits = Column(Integer, nullable=False)
    predicted_commits = Column(Float, nullable=True)
    residual = Column(Float, nullable=True)
    z_score = Column(Float)  # MAD-based modified z-score

    is_anomaly = Column(Boolean, default=False)

    # For backward compatibility; now always 'dow_mad_v3'
    model_type = Column(String(50), default="dow_mad_v3")
    seasonal_periods = Column(Integer, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self) -> str:
        return (
            f"<RepoActivityForecast(repo_id={self.repo_id}, "
            f"activity_date={self.activity_date}, "
            f"actual_commits={self.actual_commits}, "
            f"predicted_commits={self.predicted_commits}, "
            f"z_score={self.z_score}, "
            f"is_anomaly={self.is_anomaly})>"
        )
