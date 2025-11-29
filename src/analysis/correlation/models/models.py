from sqlalchemy import Column, Integer, Float, DateTime, ForeignKey, Boolean
from datetime import datetime
from src.data.models.models import Base  # shared Base


class CommitCorrelationResult(Base):
    __tablename__ = "commit_correlation_result"

    id = Column(Integer, primary_key=True, autoincrement=True)
    repo_id = Column(Integer, ForeignKey("repositories.id"), nullable=True, index=True)

    kw_h_stat = Column(Float)          # H-статистика
    kw_p_value = Column(Float)         # p-value (по χ²-распределению)
    eta_squared = Column(Float)        # η² — размер эффекта
    has_week_dependency = Column(Boolean)  # True, если p < alpha (обычно 0.05)

    n_weeks = Column(Integer)          # количество недельных наблюдений (N)
    n_week_groups = Column(Integer)    # количество различных номеров недель (k)
    total_commits = Column(Integer)    # всего коммитов в репозитории (учтённых)

    computed_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return (
            f"<CommitCorrelationResult("
            f"repo_id={self.repo_id}, "
            f"kw_h_stat={self.kw_h_stat}, "
            f"kw_p_value={self.kw_p_value}, "
            f"eta_squared={self.eta_squared}, "
            f"has_week_dependency={self.has_week_dependency}, "
            f"n_weeks={self.n_weeks}, "
            f"n_week_groups={self.n_week_groups}, "
            f"total_commits={self.total_commits}"
            f")>"
        )
