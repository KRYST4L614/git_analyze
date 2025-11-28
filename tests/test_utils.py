from src.utils.utils import clean_message, format_date, safe_lower


def test_clean_message_short():
    assert clean_message(" Hello   world ") == "Hello world"


def test_clean_message_long():
    msg = "a" * 300
    cleaned = clean_message(msg)
    assert cleaned.endswith("...")
    assert len(cleaned) == 200


def test_clean_message_empty():
    assert clean_message(None) == "N/A"


def test_format_date_valid():
    date_str = "2024-05-15T12:30:00Z"
    assert format_date(date_str) == "2024-05-15 12:30:00"


def test_format_date_invalid():
    assert format_date("not-a-date") == "not-a-date"


def test_format_date_none():
    assert format_date(None) == "N/A"


def test_safe_lower_normal():
    assert safe_lower("Hello") == "hello"


def test_safe_lower_none():
    assert safe_lower(None) == ""
