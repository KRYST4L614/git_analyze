from src.data.github.github_collector import _extract_commit_details


def test_extract_commit_details_valid():
    commit_data = {
        "sha": "abc",
        "commit": {
            "author": {"name": "John", "email": "a@b.com", "date": "2023-05-05T12:00:00Z"},
            "committer": {"name": "Jane", "email": "x@y.com", "date": "2023-05-05T12:01:00Z"},
            "message": "Initial commit",
            "comment_count": 1,
            "verification": {"verified": True, "reason": "valid"}
        },
        "stats": {"additions": 1, "deletions": 0, "total": 1},
        "files": [{"filename": "main.py"}]
    }

    result = _extract_commit_details(commit_data, "owner", "repo", "user")
    assert result["sha"] == "abc"
    assert result["author_name"] == "John"
    assert "main.py" in result["files_changed"]


def test_extract_commit_details_error():
    bad_commit = {"commit": "not-a-dict"}
    result = _extract_commit_details(bad_commit, "o", "r", "u")
    assert result is None
