import importlib
import pytest


def test_missing_supabase_env_uses_fallback(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_KEY", raising=False)

    db = importlib.import_module("services.database")
    db = importlib.reload(db)

    with pytest.raises(RuntimeError):
        db.supabase.table("articles")


def test_generate_embedding_returns_none_without_openai_client():
    ingestion = importlib.import_module("api.ingestion")
    ingestion.ai = None
    assert ingestion.generate_embedding("ciao") is None
