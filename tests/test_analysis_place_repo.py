import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime
from sqlalchemy import text

from src.analysis.clustering.place.repo.repo import (
    BaseLocationRepository,
    LocationRepository
)
from src.analysis.clustering.place.models.models import (
    Country,
    City,
    ContributorLocation
)


# =================== FIXTURES ===================
@pytest.fixture
def mock_session():
    """Mock SQLAlchemy session"""
    session = Mock()
    session.query = Mock()
    session.add = Mock()
    session.commit = Mock()
    session.rollback = Mock()
    session.close = Mock()
    session.flush = Mock()
    session.connection = Mock()
    return session


@pytest.fixture
def mock_uow(mock_session):
    """Mock UnitOfWork"""
    uow = Mock()
    uow.get_session = Mock(return_value=mock_session)
    return uow


@pytest.fixture
def location_repository(mock_uow):
    """LocationRepository with mocked UoW"""
    repo = LocationRepository("sqlite:///:memory:")
    repo.uow = mock_uow
    return repo


@pytest.fixture
def sample_location_data():
    """Sample data for contributor locations"""
    return pd.DataFrame({
        'contributor_id': [1, 2, 3],
        'contributor_login': ['user1', 'user2', 'user3'],
        'location': ['New York, USA', 'Berlin, Germany', 'Tokyo, Japan'],
        'company': ['Company A', 'Company B', 'Company C'],
        'email': ['user1@email.com', 'user2@email.com', 'user3@email.com'],
        'repo_count': [5, 3, 7],
        'commit_count': [100, 50, 200],
        'active_days': [30, 20, 60]
    })


# =================== BASE REPOSITORY TESTS ===================
class TestBaseLocationRepository:
    """Tests for BaseLocationRepository"""

    def test_uow_property_initialization(self, mock_uow):
        """Test UoW property lazy initialization"""
        repo = BaseLocationRepository("sqlite:///:memory:")

        # Mock UnitOfWork constructor
        with patch('src.analysis.clustering.place.repo.repo.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value = mock_uow

            # First access should create UoW
            uow = repo.uow
            mock_uow_class.assert_called_once_with("sqlite:///:memory:")
            assert uow == mock_uow

            # Second access should return cached UoW
            mock_uow_class.reset_mock()
            uow2 = repo.uow
            mock_uow_class.assert_not_called()
            assert uow2 == mock_uow

    def test_uow_setter(self):
        """Test UoW setter"""
        repo = BaseLocationRepository()
        mock_uow = Mock()

        repo.uow = mock_uow
        assert repo._uow == mock_uow

    def test_session_scope_success(self, mock_uow, mock_session):
        """Test session_scope context manager with successful commit"""
        repo = BaseLocationRepository()
        repo.uow = mock_uow

        with repo.session_scope() as session:
            assert session == mock_session
            session.execute(text("SELECT 1"))

        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()

    def test_session_scope_exception(self, mock_uow, mock_session):
        """Test session_scope context manager with exception"""
        repo = BaseLocationRepository()
        repo.uow = mock_uow

        mock_session.execute.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            with repo.session_scope() as session:
                session.execute(text("SELECT 1"))

        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()
        mock_session.commit.assert_not_called()


# =================== LOCATION REPOSITORY TESTS ===================
class TestLocationRepository:
    """Tests for LocationRepository"""

    # ---------- Data Loading Tests ----------
    def test_load_contributor_location_data_success(self, location_repository, mock_session, sample_location_data):
        """Test successful loading of contributor location data"""
        # Setup mock
        mock_conn = Mock()
        mock_session.connection.return_value = mock_conn

        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.return_value = sample_location_data

            # Execute
            result = location_repository.load_contributor_location_data()

            # Verify
            mock_read_sql.assert_called_once()
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3
            assert 'contributor_id' in result.columns
            assert 'location' in result.columns
            pd.testing.assert_frame_equal(result, sample_location_data)

    def test_load_contributor_location_data_empty(self, location_repository, mock_session):
        """Test loading when no data available"""
        mock_conn = Mock()
        mock_session.connection.return_value = mock_conn

        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.return_value = pd.DataFrame()

            result = location_repository.load_contributor_location_data()

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0

    # ---------- Country CRUD Tests ----------
    @pytest.mark.parametrize("country_name,should_create", [
        ("United States", True),
        ("Germany", True),
        ("", True),
    ])
    def test_get_or_create_country_new(self, location_repository, mock_session,
                                       country_name, should_create):
        """Test creating new country - returns (True, id)"""
        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = None
        mock_query.filter.return_value = mock_filter
        mock_session.query.return_value = mock_query

        mock_new_country = Mock()
        mock_new_country.id = 42

        with patch('src.analysis.clustering.place.repo.repo.Country') as mock_country_class:
            mock_country_class.return_value = mock_new_country

            exists, country_id = location_repository.get_or_create_country(country_name)

            assert exists == should_create
            assert country_id == 42

            mock_session.add.assert_called_once_with(mock_new_country)
            mock_session.flush.assert_called_once()

            mock_country_class.assert_called_once_with(name=country_name)

    def test_get_or_create_country_existing(self, location_repository, mock_session):
        """Test retrieving existing country"""
        country_name = "United States"
        mock_country = Mock(spec=Country)
        mock_country.id = 1

        # Mock query to return existing country
        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = mock_country
        mock_query.filter.return_value = mock_filter
        mock_session.query.return_value = mock_query

        exists, country_id = location_repository.get_or_create_country(country_name)

        assert exists == False  # Not created (already exists)
        assert country_id == 1
        mock_session.add.assert_not_called()

    # ---------- City CRUD Tests ----------
    @pytest.mark.parametrize("city_name,country_id,lat,lng,expected_exists", [
        ("New York", 1, 40.7128, -74.0060, True),
        ("Berlin", 2, 52.5200, 13.4050, True),
        ("Tokyo", 3, 35.6762, 139.6503, True),
    ])
    def test_get_or_create_city_new(self, location_repository, mock_session,
                                    city_name, country_id, lat, lng, expected_exists):
        """Test creating new city - returns (True, id)"""
        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = None
        mock_query.filter.return_value = mock_filter
        mock_session.query.return_value = mock_query

        mock_city = Mock()
        mock_city.id = 100
        with patch('src.analysis.clustering.place.repo.repo.City') as mock_city_class:
            mock_city_class.return_value = mock_city

            exists, city_id = location_repository.get_or_create_city(
                city_name, country_id, lat, lng
            )

            assert exists == expected_exists
            assert city_id == 100
            mock_session.add.assert_called_once()
            mock_session.flush.assert_called_once()

            mock_city_class.assert_called_once_with(
                name=city_name,
                country_id=country_id,
                latitude=lat,
                longitude=lng
            )

    def test_get_or_create_city_existing(self, location_repository, mock_session):
        """Test retrieving existing city"""
        mock_city = Mock(spec=City)
        mock_city.id = 1

        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = mock_city
        mock_query.filter.return_value = mock_filter
        mock_session.query.return_value = mock_query

        exists, city_id = location_repository.get_or_create_city("New York", 1)

        assert exists == False
        assert city_id == 1
        mock_session.add.assert_not_called()

    # ---------- Contributor Location Tests ----------
    def test_save_contributor_location_new(self, location_repository, mock_session):
        """Test saving new contributor location"""
        # Mock get_or_create_country and get_or_create_city
        location_repository.get_or_create_country = Mock(return_value=(True, 1))
        location_repository.get_or_create_city = Mock(return_value=(True, 2))

        # Mock query for existing location
        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = None  # No existing location
        mock_query.filter_by.return_value = mock_filter
        mock_session.query.return_value = mock_query

        result = location_repository.save_contributor_location(
            contributor_id=1,
            original_location="New York, USA",
            country_name="United States",
            city_name="New York",
            latitude=40.7128,
            longitude=-74.0060
        )

        assert result == True
        location_repository.get_or_create_country.assert_called_once_with("United States")
        location_repository.get_or_create_city.assert_called_once_with(
            "New York", 1, 40.7128, -74.0060
        )
        mock_session.add.assert_called_once()
        mock_session.query.assert_called()

    def test_save_contributor_location_update(self, location_repository, mock_session):
        """Test updating existing contributor location"""
        # Mock existing ContributorLocation
        mock_existing = Mock(spec=ContributorLocation)
        mock_existing.contributor_id = 1

        location_repository.get_or_create_country = Mock(return_value=(False, 1))
        location_repository.get_or_create_city = Mock(return_value=(False, 2))

        # Mock query to return existing location
        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = mock_existing
        mock_query.filter_by.return_value = mock_filter
        mock_session.query.return_value = mock_query

        result = location_repository.save_contributor_location(
            contributor_id=1,
            original_location="New York, USA",
            country_name="United States",
            city_name="New York"
        )

        assert result == True
        assert mock_existing.country_id == 1
        assert mock_existing.city_id == 2
        assert mock_existing.original_location == "New York, USA"
        mock_session.add.assert_not_called()  # Should not add new, should update existing

    def test_save_contributor_location_no_city(self, location_repository, mock_session):
        """Test saving location without city"""
        location_repository.get_or_create_country = Mock(return_value=(True, 1))

        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = None
        mock_query.filter_by.return_value = mock_filter
        mock_session.query.return_value = mock_query

        result = location_repository.save_contributor_location(
            contributor_id=1,
            original_location="USA",
            country_name="United States"
        )

        assert result == True
        location_repository.get_or_create_country.assert_called_once_with("United States")

    def test_save_contributor_location_exception(self, location_repository, mock_session):
        """Test error handling when saving location"""
        mock_session.query.side_effect = Exception("Database error")

        result = location_repository.save_contributor_location(
            contributor_id=1,
            original_location="New York, USA",
            country_name="United States"
        )

        assert result == False

    # ---------- Data Retrieval Tests ----------
    def test_get_contributor_locations(self, location_repository, mock_session):
        """Test retrieving contributor locations"""
        expected_data = pd.DataFrame({
            'contributor_id': [1, 2],
            'original_location': ['New York, USA', 'Berlin, Germany'],
            'country_name': ['United States', 'Germany'],
            'city_name': ['New York', 'Berlin'],
            'latitude': [40.7128, 52.5200],
            'longitude': [-74.0060, 13.4050],
            'confidence': [0.9, 0.8],
            'created_at': [datetime.now(), datetime.now()]
        })

        mock_conn = Mock()
        mock_session.connection.return_value = mock_conn

        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.return_value = expected_data

            result = location_repository.get_contributor_locations()

            assert isinstance(result, pd.DataFrame)
            pd.testing.assert_frame_equal(result, expected_data)
            mock_read_sql.assert_called_once()

    def test_get_contributor_locations_empty(self, location_repository, mock_session):
        """Test retrieving contributor locations when none exist"""
        mock_conn = Mock()
        mock_session.connection.return_value = mock_conn

        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.return_value = pd.DataFrame()

            result = location_repository.get_contributor_locations()

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0

    # ---------- Integration-style Tests ----------
    def test_complete_workflow(self, location_repository, mock_session):
        """Test complete workflow: load → process → save"""
        # Setup mocks
        sample_data = pd.DataFrame({
            'contributor_id': [1, 2],
            'contributor_login': ['user1', 'user2'],
            'location': ['New York, USA', 'Berlin, Germany'],
            'company': ['A', 'B'],
            'email': ['a@a.com', 'b@b.com'],
            'repo_count': [1, 2],
            'commit_count': [10, 20],
            'active_days': [5, 10]
        })

        mock_conn = Mock()
        mock_session.connection.return_value = mock_conn

        # Mock pandas.read_sql for load
        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.return_value = sample_data

            # Mock the CRUD operations
            location_repository.get_or_create_country = Mock(side_effect=[
                (True, 1),  # USA
                (True, 2)  # Germany
            ])
            location_repository.get_or_create_city = Mock(side_effect=[
                (True, 1),  # New York
                (True, 2)  # Berlin
            ])

            # Mock query for save operations
            mock_query = Mock()
            mock_filter = Mock()
            mock_filter.first.return_value = None  # No existing locations
            mock_query.filter_by.return_value = mock_filter
            mock_session.query.return_value = mock_query

            # Execute workflow
            df = location_repository.load_contributor_location_data()
            assert len(df) == 2

            # Process first row
            result1 = location_repository.save_contributor_location(
                contributor_id=1,
                original_location="New York, USA",
                country_name="United States",
                city_name="New York"
            )
            assert result1 == True

            # Process second row
            result2 = location_repository.save_contributor_location(
                contributor_id=2,
                original_location="Berlin, Germany",
                country_name="Germany",
                city_name="Berlin"
            )
            assert result2 == True


# =================== ERROR HANDLING TESTS ===================
class TestErrorHandling:
    """Tests for error handling scenarios"""

    def test_database_connection_error_on_load(self, location_repository, mock_session):
        """Test handling database connection errors"""
        mock_session.connection.side_effect = Exception("Connection failed")

        with pytest.raises(Exception, match="Connection failed"):
            location_repository.load_contributor_location_data()

    def test_invalid_sql_query(self, location_repository, mock_session):
        """Test handling invalid SQL queries"""
        mock_conn = Mock()
        mock_session.connection.return_value = mock_conn

        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.side_effect = Exception("Invalid SQL")

            with pytest.raises(Exception, match="Invalid SQL"):
                location_repository.load_contributor_location_data()

    def test_duplicate_country_handling(self, location_repository, mock_session):
        """Test handling of duplicate country insertion"""
        # Simulate unique constraint violation on flush
        mock_session.flush.side_effect = Exception("UNIQUE constraint failed")

        # Mock query to return None initially
        mock_query = Mock()
        mock_filter = Mock()
        mock_filter.first.return_value = None
        mock_query.filter.return_value = mock_filter
        mock_session.query.return_value = mock_query

        with pytest.raises(Exception, match="UNIQUE constraint failed"):
            location_repository.get_or_create_country("United States")


# =================== CONCURRENCY TESTS ===================
class TestConcurrency:
    """Tests for concurrent access scenarios"""

    @pytest.mark.parametrize("thread_count", [2, 5, 10])
    def test_concurrent_country_creation(self, location_repository, mock_session, thread_count):
        """Test concurrent creation of same country"""
        import threading
        from concurrent.futures import ThreadPoolExecutor

        results = []
        errors = []

        def create_country():
            try:
                # Mock to simulate race condition
                mock_query = Mock()
                mock_filter = Mock()

                # First call returns None, subsequent calls return country
                if not hasattr(create_country, 'call_count'):
                    create_country.call_count = 0

                if create_country.call_count == 0:
                    mock_filter.first.return_value = None
                else:
                    mock_country = Mock()
                    mock_country.id = 1
                    mock_filter.first.return_value = mock_country

                create_country.call_count += 1
                mock_query.filter.return_value = mock_filter
                mock_session.query.return_value = mock_query

                exists, country_id = location_repository.get_or_create_country("United States")
                results.append((exists, country_id))
            except Exception as e:
                errors.append(str(e))

        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = [executor.submit(create_country) for _ in range(thread_count)]
            for future in futures:
                future.result()

        # Should handle race conditions gracefully
        assert len(errors) == 0
        assert len(results) == thread_count