from fastapi.testclient import TestClient

import main


def _client_with_auth_env(monkeypatch):
    monkeypatch.setenv("APP_USERNAME", "tester")
    monkeypatch.setenv("APP_PASSWORD", "secret123")
    main._AUTH_SESSIONS.clear()
    return TestClient(main.app)


def test_protected_pages_redirect_to_login(monkeypatch):
    client = _client_with_auth_env(monkeypatch)
    resp = client.get("/home", follow_redirects=False)
    assert resp.status_code == 307
    assert resp.headers["location"] == "/login"


def test_protected_api_returns_401_when_not_authenticated(monkeypatch):
    client = _client_with_auth_env(monkeypatch)
    resp = client.get("/api/events")
    assert resp.status_code == 401
    assert resp.json()["error"] == "UNAUTHORIZED"


def test_login_sets_cookie_and_grants_access(monkeypatch):
    client = _client_with_auth_env(monkeypatch)

    login = client.post("/api/login", json={"username": "tester", "password": "secret123"})
    assert login.status_code == 200
    assert login.json()["success"] is True
    assert "spiz_session" in client.cookies

    home = client.get("/home")
    assert home.status_code == 200
