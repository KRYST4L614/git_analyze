from contextlib import contextmanager
from sqlalchemy import text
from src.storage.unit_of_work import UnitOfWork
from src.analysis.clustering.type.models.models import (
    RepositoryClusteringResult,
    RepositoryType
)
from typing import List, Dict
import pandas as pd


class BaseAnalysisRepository:
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
        session = self.uow.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


class AnalysisRepository(BaseAnalysisRepository):
    def __init__(self, database_url: str = None):
        super().__init__(database_url)
        self._ensure_repository_types()

    def _ensure_repository_types(self):
        with self.session_scope() as session:
            existing_types = session.query(RepositoryType).count()
            if existing_types == 0:
                types = [
                    RepositoryType(id=1, name='corporate', description='Corporate repositories'),
                    RepositoryType(id=2, name='educational', description='Educational repositories'),
                    RepositoryType(id=3, name='personal', description='Personal repositories')
                ]
                session.add_all(types)

    def _get_type_mapping(self) -> Dict[str, int]:
        with self.session_scope() as session:
            types = session.query(RepositoryType).all()
            return {repo_type.name: repo_type.id for repo_type in types}

    def load_repository_data(self, chunk_size: int = 1000) -> List[pd.DataFrame]:
        chunks = []
        with self.session_scope() as session:
            query = text("""
                SELECT 
                    r.id as repo_id,
                    r.full_name,
                    r.description,
                    r.topics,
                    r.owner_login,
                    r.language,
                    r.stargazers_count as stars,
                    r.forks_count as forks
                FROM repositories r
                WHERE r.description IS NOT NULL OR r.full_name IS NOT NULL
            """)

            for chunk in pd.read_sql(query, session.connection(), chunksize=chunk_size):
                chunks.append(chunk)

        return chunks

    def save_clustering_results(self, results_df: pd.DataFrame):
        with self.session_scope() as session:
            session.query(RepositoryClusteringResult).delete()

            type_mapping = self._get_type_mapping()

            for _, row in results_df.iterrows():
                repo_type_name = row.get('final_type', 'personal')
                repo_type_id = type_mapping.get(repo_type_name, 3)

                result = RepositoryClusteringResult(
                    repo_id=row['repo_id'],
                    repo_name=row.get('full_name', ''),
                    repo_type_id=repo_type_id,
                    corporate_weight=row.get('corporate_weight', 0.0),
                    educational_weight=row.get('educational_weight', 0.0),
                    personal_weight=row.get('personal_weight', 0.0),
                    confidence_score=row.get('confidence_score', 0.0),
                    features={
                        'topics': row.get('topics', []),
                        'owner_login': row.get('owner_login', ''),
                        'org_name': row.get('org_name', '')
                    }
                )
                session.add(result)

            print(f"Saved {len(results_df)} clustering results to database")