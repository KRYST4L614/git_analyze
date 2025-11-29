from contextlib import contextmanager
from sqlalchemy import text
from src.storage.unit_of_work import UnitOfWork
from src.analysis.clustering.place.models.models import (
    Country,
    City,
    ContributorLocation
)
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd


class LocationRepository:
    def __init__(self, database_url: str = None):
        self.database_url = database_url

    @contextmanager
    def session_scope(self):
        uow = UnitOfWork(self.database_url)
        session = uow.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def load_contributor_location_data(self) -> pd.DataFrame:
        """
        Загрузка данных о контрибьютерах и их локациях
        """
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
                    AND c.id > 20000000
                GROUP BY c.id, c.login, c.location, c.company, c.email
                HAVING COUNT(cm.sha) >= 1
            """)

            df = pd.read_sql(query, session.connection())
            print(f"Loaded {len(df)} contributors with location data")
            return df

    def get_or_create_country(self, country_name: str) -> Tuple[bool, int]:
        """
        Получает или создает страну в базе данных
        Возвращает (created, country_id)
        """
        with self.session_scope() as session:
            # Ищем существующую страну
            country = session.query(Country).filter(
                Country.name == country_name
            ).first()

            if country:
                return False, country.id

            # Создаем новую страну
            new_country = Country(name=country_name)
            session.add(new_country)
            session.flush()  # Получаем ID

            print(f"Created new country: {country_name} (ID: {new_country.id})")
            return True, new_country.id

    def get_or_create_city(self, city_name: str, country_id: int,
                           latitude: float = None, longitude: float = None) -> Tuple[bool, int]:
        """
        Получает или создает город в базе данных
        Возвращает (created, city_id)
        """
        with self.session_scope() as session:
            # Ищем существующий город
            city = session.query(City).filter(
                City.name == city_name,
                City.country_id == country_id
            ).first()

            if city:
                return False, city.id

            # Создаем новый город
            new_city = City(
                name=city_name,
                country_id=country_id,
                latitude=latitude,
                longitude=longitude
            )
            session.add(new_city)
            session.flush()  # Получаем ID

            print(f"Created new city: {city_name} (Country ID: {country_id})")
            return True, new_city.id

    def save_contributor_location(self, contributor_id: int, original_location: str,
                                  country_name: str, city_name: str = None,
                                  latitude: float = None, longitude: float = None) -> bool:
        """
        Сохранение локации контрибьютера с использованием словарей
        """
        try:
            with self.session_scope() as session:
                # Получаем или создаем страну
                _, country_id = self.get_or_create_country(country_name)

                city_id = None
                if city_name and city_name != 'unknown':
                    # Получаем или создаем город
                    _, city_id = self.get_or_create_city(
                        city_name, country_id, latitude, longitude
                    )

                # Проверяем, есть ли уже запись для этого контрибьютера
                existing_location = session.query(ContributorLocation).filter_by(
                    contributor_id=contributor_id
                ).first()

                if existing_location:
                    # Обновляем существующую запись
                    existing_location.country_id = country_id
                    existing_location.city_id = city_id
                    existing_location.latitude = latitude
                    existing_location.longitude = longitude
                    existing_location.original_location = original_location
                else:
                    # Создаем новую запись
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
        """
        Получение всех локаций контрибьютеров с JOIN на словари
        """
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