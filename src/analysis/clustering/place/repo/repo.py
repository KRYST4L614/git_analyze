# src/analysis/clustering/place/repo/repo.py
from contextlib import contextmanager
from sqlalchemy import text
from src.storage.unit_of_work import UnitOfWork
from src.analysis.clustering.place.models.models import (
    Country,
    City,
    ContributorLocation
)
from typing import Tuple
import pandas as pd


class BaseLocationRepository:
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


class LocationRepository(BaseLocationRepository):
    def __init__(self, database_url: str = None):
        super().__init__(database_url)

    def load_contributor_location_data(self) -> pd.DataFrame:
        with self.session_scope() as session:
            query = text("""
                SELECT 
                    c.id as contributor_id,
                    c.login as contributor_login,
                    c.location,
                    c.company,
                    c.email,
                    COUNT(DISTINCT cm.repo_id) as repo_count,
                    COUNT(cm.sha) as commit_count,
                    COUNT(DISTINCT DATE(cm.author_date)) as active_days
                FROM contributors c
                LEFT JOIN commits cm ON c.id = cm.author_id
                WHERE c.location IS NOT NULL 
                    AND c.location != ''
                GROUP BY c.id, c.login, c.location, c.company, c.email
                HAVING COUNT(cm.sha) >= 1
            """)

            df = pd.read_sql(query, session.connection())
            print(f"Loaded {len(df)} contributors with location data")
            return df

    def get_or_create_country(self, country_name: str) -> Tuple[bool, int]:
        with self.session_scope() as session:
            country = session.query(Country).filter(
                Country.name == country_name
            ).first()

            if country:
                return False, country.id

            new_country = Country(name=country_name)
            session.add(new_country)
            session.flush()

            print(f"Created new country: {country_name} (ID: {new_country.id})")
            return True, new_country.id

    def get_or_create_city(self, city_name: str, country_id: int,
                           latitude: float = None, longitude: float = None) -> Tuple[bool, int]:
        with self.session_scope() as session:
            city = session.query(City).filter(
                City.name == city_name,
                City.country_id == country_id
            ).first()

            if city:
                return False, city.id

            new_city = City(
                name=city_name,
                country_id=country_id,
                latitude=latitude,
                longitude=longitude
            )
            session.add(new_city)
            session.flush()

            print(f"Created new city: {city_name} (Country ID: {country_id})")
            return True, new_city.id

    def save_contributor_location(self, contributor_id: int, original_location: str,
                                  country_name: str, city_name: str = None,
                                  latitude: float = None, longitude: float = None) -> bool:
        try:
            with self.session_scope() as session:
                _, country_id = self.get_or_create_country(country_name)

                city_id = None
                if city_name and city_name != 'unknown':
                    _, city_id = self.get_or_create_city(
                        city_name, country_id, latitude, longitude
                    )

                existing_location = session.query(ContributorLocation).filter_by(
                    contributor_id=contributor_id
                ).first()

                if existing_location:
                    existing_location.country_id = country_id
                    existing_location.city_id = city_id
                    existing_location.latitude = latitude
                    existing_location.longitude = longitude
                    existing_location.original_location = original_location
                else:
                    new_location = ContributorLocation(
                        contributor_id=contributor_id,
                        original_location=original_location,
                        country_id=country_id,
                        city_id=city_id,
                        latitude=latitude,
                        longitude=longitude
                    )
                    session.add(new_location)

                return True

        except Exception as e:
            print(f"Error saving location for contributor {contributor_id}: {e}")
            return False

    def get_contributor_locations(self) -> pd.DataFrame:
        with self.session_scope() as session:
            query = text("""
                SELECT 
                    cl.contributor_id,
                    cl.original_location,
                    c.name as country_name,
                    city.name as city_name,
                    cl.latitude,
                    cl.longitude,
                    cl.confidence,
                    cl.created_at
                FROM contributor_locations cl
                JOIN countries c ON cl.country_id = c.id
                LEFT JOIN cities city ON cl.city_id = city.id
                ORDER BY cl.contributor_id
            """)

            df = pd.read_sql(query, session.connection())
            return df