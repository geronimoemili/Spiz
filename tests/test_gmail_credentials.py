import main


def test_get_gmail_credentials_from_primary_env(monkeypatch):
    monkeypatch.setenv("GMAIL_USER", "primary@example.com")
    monkeypatch.setenv("GMAIL_APP_PASSWORD", "app-pass")
    monkeypatch.delenv("GMAIL_EMAIL", raising=False)
    monkeypatch.delenv("GMAIL_PASSWORD", raising=False)
    monkeypatch.delenv("GOOGLE_APP_PASSWORD", raising=False)
    monkeypatch.delenv("EMAIL_USER", raising=False)
    monkeypatch.delenv("EMAIL_PASSWORD", raising=False)

    user, password = main._get_gmail_credentials()
    assert user == "primary@example.com"
    assert password == "app-pass"


def test_get_gmail_credentials_from_alias_env(monkeypatch):
    monkeypatch.delenv("GMAIL_USER", raising=False)
    monkeypatch.delenv("GMAIL_APP_PASSWORD", raising=False)
    monkeypatch.setenv("GMAIL_EMAIL", "alias@example.com")
    monkeypatch.setenv("GMAIL_PASSWORD", "alias-pass")

    user, password = main._get_gmail_credentials()
    assert user == "alias@example.com"
    assert password == "alias-pass"
