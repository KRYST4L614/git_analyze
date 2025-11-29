from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    Float,
    String,
    Boolean,
    DateTime,
    ForeignKey,
)

# 👇 Берём общий Base, где уже описана таблица repositories
from src.data.models.models import Base


class RepositoryYearActivityStats(Base):
    """
    Year-level stats for a repository's activity.

    One row per (repo_id, year).
    """
    __tablename__ = "repository_year_activity_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)

    repo_id = Column(Integer, ForeignKey("repositories.id"), index=True, nullable=False)
    year = Column(Integer, index=True, nullable=False)

    total_commits = Column(Integer, nullable=False)
    active_weeks = Column(Integer, nullable=False)
    first_week = Column(Integer, nullable=True)
    last_week = Column(Integer, nullable=True)
    weeks_in_range = Column(Integer, nullable=False)
    active_weeks_ratio = Column(Float, nullable=False)
    is_alive = Column(Boolean, nullable=False, index=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self) -> str:
        return (
            f"<RepositoryYearActivityStats("
            f"repo_id={self.repo_id}, year={self.year}, "
            f"total_commits={self.total_commits}, active_weeks={self.active_weeks}, "
            f"first_week={self.first_week}, last_week={self.last_week}, "
            f"weeks_in_range={self.weeks_in_range}, "
            f"active_weeks_ratio={self.active_weeks_ratio:.3f}, "
            f"is_alive={self.is_alive}"
            f")>"
        )


class RepositoryWeeklyActivityAnomaly(Base):
    """
    Weekly anomalies for a repository's activity.

    One row per anomalous (repo_id, year, iso_week).
    """
    __tablename__ = "repository_weekly_activity_anomalies"

    id = Column(Integer, primary_key=True, autoincrement=True)

    repo_id = Column(Integer, ForeignKey("repositories.id"), index=True, nullable=False)
    year = Column(Integer, index=True, nullable=False)
    iso_week = Column(Integer, index=True, nullable=False)

    commit_count = Column(Integer, nullable=False)
    z_score = Column(Float, nullable=False)
    direction = Column(String(10), nullable=False)  # "high" or "low"

    # Context fields (duplicated for easier querying)
    total_commits_year = Column(Integer, nullable=False)
    active_weeks_year = Column(Integer, nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self) -> str:
        return (
            f"<RepositoryWeeklyActivityAnomaly("
            f"repo_id={self.repo_id}, year={self.year}, week={self.iso_week}, "
            f"commit_count={self.commit_count}, z_score={self.z_score:.3f}, "
            f"direction={self.direction}"
            f")>"
        )
