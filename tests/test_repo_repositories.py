import pytest
from unittest.mock import patch, MagicMock
from src.data.repo.repo import RepositoryRepository, ContributorRepository, CommitRepository


@pytest.fixture
def mock_session():
    return MagicMock()


@patch("src.data.repo.repo.UnitOfWork", autospec=True)
def test_upsert_repository(mock_uow_class, mock_session):
    # Make UnitOfWork().get_session() return our mock session
    mock_uow_instance = mock_uow_class.return_value
    mock_uow_instance.get_session.return_value = mock_session

    repo_repo = RepositoryRepository("sqlite://")
    data = {"id": 1, "name": "test", "full_name": "owner/test"}

    repo_repo.upsert_repository(data)

    mock_session.execute.assert_called_once()
    mock_session.commit.assert_called_once()


@patch("src.data.repo.repo.UnitOfWork", autospec=True)
def test_contributor_exists(mock_uow_class, mock_session):
    mock_uow_instance = mock_uow_class.return_value
    mock_session.query().filter().count.return_value = 1
    mock_uow_instance.get_session.return_value = mock_session

    contrib_repo = ContributorRepository("sqlite://")
    assert contrib_repo.exists(1) is True


@patch("src.data.repo.repo.UnitOfWork", autospec=True)
def test_commit_upsert(mock_uow_class, mock_session):
    mock_uow_instance = mock_uow_class.return_value
    mock_uow_instance.get_session.return_value = mock_session

    commit_repo = CommitRepository("sqlite://")
    commit_repo.upsert_commit({"sha": "abc123"}, 1, 2)

    mock_session.execute.assert_called_once()
    mock_session.commit.assert_called_once()
