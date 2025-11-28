# src/analysis/correlation/models/models.py

from sqlalchemy import Column, Integer, Float, DateTime, ForeignKey
from datetime import datetime
from src.data.models.models import Base  # shared Base

class CommitCorrelationResult(Base):
    __tablename__ = "commit_correlation_result"

    id = Column(Integer, primary_key=True, autoincrement=True)
    repo_id = Column(Integer, ForeignKey("repositories.id"), nullable=True, index=True)

    # correlations
    commit_corr_week_of_year = Column(Float)
    commit_corr_week_index = Column(Float)
    pr_corr_week_of_year = Column(Float)
    pr_corr_week_index = Column(Float)

    # NEW: sample sizes / volumes
    n_weeks_commits = Column(Integer)      # how many weekly commit points
    total_commits = Column(Integer)       # total commits used

    n_weeks_pr_lead = Column(Integer)     # how many weekly lead-time points
    n_pr_merges_used = Column(Integer)    # how many merged PRs

    computed_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return (
            f"<CommitCorrelationResult("
            f"repo_id={self.repo_id}, "
            f"commit_corr_week_index={self.commit_corr_week_index}, "
            f"pr_corr_week_index={self.pr_corr_week_index}, "
            f"n_weeks_commits={self.n_weeks_commits}, "
            f"n_weeks_pr_lead={self.n_weeks_pr_lead}"
            f")>"
        )
