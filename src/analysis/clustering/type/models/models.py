from sqlalchemy import Column, Integer, String, Text, DateTime, Float, JSON, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()


class RepositoryType(Base):
    __tablename__ = 'repository_types'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)  # 'corporate', 'educational', 'personal'
    description = Column(Text)

    # Relationship
    clustering_results = relationship("RepositoryClusteringResult", back_populates="repo_type_rel")

    def __repr__(self):
        return f"<RepositoryType(id={self.id}, name={self.name})>"


class RepositoryClusteringResult(Base):
    __tablename__ = 'repository_clustering_results'

    id = Column(Integer, primary_key=True, autoincrement=True)
    repo_id = Column(Integer, nullable=False, index=True)
    repo_name = Column(String(255))
    repo_type_id = Column(Integer, ForeignKey('repository_types.id'), nullable=False)
    corporate_weight = Column(Float)
    educational_weight = Column(Float)
    personal_weight = Column(Float)
    confidence_score = Column(Float)
    features = Column(JSON)
    analysis_timestamp = Column(DateTime, default=datetime.utcnow)

    # Relationships
    repo_type_rel = relationship("RepositoryType", back_populates="clustering_results")

    def __repr__(self):
        return f"<RepositoryClusteringResult(repo_id={self.repo_id}, type_id={self.repo_type_id})>"