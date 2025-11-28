from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()


class Country(Base):
    __tablename__ = 'countries'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    code = Column(String(2), unique=True)  # ISO code: US, DE, etc.

    # Relationships
    cities = relationship("City", back_populates="country")
    contributor_locations = relationship("ContributorLocation", back_populates="country")


class City(Base):
    __tablename__ = 'cities'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    country_id = Column(Integer, ForeignKey('countries.id'), nullable=False)
    latitude = Column(Float)
    longitude = Column(Float)
    timezone = Column(String(50))

    # Relationships
    country = relationship("Country", back_populates="cities")
    contributor_locations = relationship("ContributorLocation", back_populates="city")


class ContributorLocation(Base):
    __tablename__ = 'contributor_locations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    contributor_id = Column(Integer, nullable=False, unique=True, index=True)
    original_location = Column(String(255), nullable=False)
    country_id = Column(Integer, ForeignKey('countries.id'), nullable=False)
    city_id = Column(Integer, ForeignKey('cities.id'))
    latitude = Column(Float)
    longitude = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    country = relationship("Country", back_populates="contributor_locations")
    city = relationship("City", back_populates="contributor_locations")

    def __repr__(self):
        return f"<ContributorLocation(contributor_id={self.contributor_id}, country_id={self.country_id})>"