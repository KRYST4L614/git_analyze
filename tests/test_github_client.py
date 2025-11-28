import pytest
from unittest.mock import MagicMock, patch
from src.data.github.github_client import GitHubClient


@pytest.fixture
def client():
    return GitHubClient(token="fake")


def test_init_headers(client):
    assert "Authorization" in client.session.headers
    assert client.headers["User-Agent"] == "GitHub-Repo-Analyzer"


@patch("src.data.github.github_client.requests.Session.get")
def test_make_request_success(mock_get, client):
    mock_resp = MagicMock(status_code=200)
    mock_resp.headers = {'X-RateLimit-Remaining': '10'}
    mock_get.return_value = mock_resp
    response = client.make_request("https://api.github.com/test")
    assert response.status_code == 200
    assert client.core_remaining == 10


@patch("src.data.github.github_client.requests.Session.get")
def test_make_request_rate_limit(mock_get, client):
    mock_resp = MagicMock(status_code=403)
    mock_resp.headers = {
        'X-RateLimit-Remaining': '0',
        'X-RateLimit-Reset': '123456789'
    }
    mock_get.side_effect = [mock_resp, MagicMock(status_code=200)]
    with patch("src.data.github.github_client.wait_for_rate_limit") as wait_mock:
        response = client.make_request("url")
        wait_mock.assert_called_once()
        assert response.status_code == 200


@patch("src.data.github.github_client.requests.Session.get")
def test_get_commit_count_last_page(mock_get, client):
    mock_resp = MagicMock(status_code=200)
    mock_resp.headers = {'Link': '<https://api.github.com?page=10>; rel="last"'}
    mock_resp.json.return_value = [{}]
    mock_get.return_value = mock_resp
    result = client.get_commit_count("user", "repo")
    assert result == 10


@patch("src.data.github.github_client.requests.Session.get")
def test_get_commit_count_no_link(mock_get, client):
    mock_resp = MagicMock(status_code=200)
    mock_resp.headers = {}
    mock_resp.json.return_value = [{} for _ in range(5)]
    mock_get.return_value = mock_resp
    assert client.get_commit_count("user", "repo") == 5


@patch("src.data.github.github_client.requests.Session.get")
def test_get_user_info_success(mock_get, client):
    mock_resp = MagicMock(status_code=200)
    mock_resp.json.return_value = {"location": "Earth"}
    mock_get.return_value = mock_resp
    assert client.get_user_info("user")["location"] == "Earth"


@patch("src.data.github.github_client.requests.Session.get")
def test_get_user_info_fail(mock_get, client):
    mock_resp = MagicMock(status_code=404)
    mock_get.return_value = mock_resp
    assert client.get_user_info("unknown") == {"location": "Unknown"}
