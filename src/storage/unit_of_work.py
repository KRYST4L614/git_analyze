from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager


class UnitOfWork:
    def __init__(self, database_url: str = None):
        if not database_url:
            raise ValueError("Database URL is required")

        self.engine = create_engine(
            database_url,
            pool_size=50,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=1800,
            future=True
        )
        self.session_factory = sessionmaker(
            bind=self.engine,
            expire_on_commit=False
        )

    @contextmanager
    def session_scope(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_session(self):
        return self.session_factory()

    def dispose(self):
        self.engine.dispose()

    def create_location_tables(self):
        from src.analysis.clustering.place.models.models import Base
        Base.metadata.create_all(self.engine)

    def create_type_clustering_tables(self):
        from src.analysis.clustering.type.models.models import Base
        Base.metadata.create_all(self.engine)

    def create_tables(self):
        from src.data.models.models import Base
        Base.metadata.create_all(self.engine)