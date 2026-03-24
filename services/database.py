import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()


class _MissingSupabaseClient:
    """Fallback client che evita crash a import-time se mancano le env."""

    def table(self, _name):
        raise RuntimeError(
            "Supabase non configurato: imposta SUPABASE_URL e SUPABASE_KEY."
        )


def _build_supabase_client():
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        print(
            "⚠️ Supabase non configurato: imposta SUPABASE_URL e SUPABASE_KEY "
            "per abilitare DB/API."
        )
        return _MissingSupabaseClient()
    return create_client(supabase_url, supabase_key)


supabase = _build_supabase_client()


def upsert_article(data):
    # On_conflict usa l'hash per evitare doppioni se ricarichi lo stesso file
    return supabase.table("articles").upsert(data, on_conflict="content_hash").execute()
