from contextlib import contextmanager

from sqlalchemy import text
import pandas as pd

from src.storage.unit_of_work import UnitOfWork
from src.analysis.correlation.models.models import CommitCorrelationResult


class CorrelationRepository:

    def __init__(self, database_url: str | None = None):
        self.database_url = database_url
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

    def load_commits(self) -> pd.DataFrame:
        """
        Загружаем минимальную информацию о коммитах:
        repo_id, commit_date.

        commit_date нужен, чтобы вычислить неделю (ISO week)
        и агрегировать частоту коммитов по неделям.
        """
        with self.session_scope() as session:
            query = text("""
                SELECT
                    repo_id,
                    commit_date
                FROM commits
                WHERE commit_date IS NOT NULL
            """)
            df = pd.read_sql(query, session.connection())
            print(f"[CorrelationRepository] Loaded {len(df)} commits")
            return df

    def save_correlation_result(self, result_dict: dict) -> None:
        """
        Сохраняем одну строку результата анализа для одного репозитория.
        Историю не затираем: таблица будет накапливать результаты.
        """
        with self.session_scope() as session:
            row = CommitCorrelationResult(
                repo_id=result_dict.get("repo_id"),
                kw_h_stat=result_dict.get("kw_h_stat"),
                kw_p_value=result_dict.get("kw_p_value"),
                eta_squared=result_dict.get("eta_squared"),
                has_week_dependency=result_dict.get("has_week_dependency"),
                n_weeks=result_dict.get("n_weeks"),
                n_week_groups=result_dict.get("n_week_groups"),
                total_commits=result_dict.get("total_commits"),
            )

            session.add(row)
            print(
                "[CorrelationRepository] Saved Kruskal–Wallis result "
                f"to database for repo_id={row.repo_id}"
            )
