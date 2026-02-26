import os
import pandas as pd
import hashlib
import datetime
from services.database import supabase
from openai import OpenAI

ai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def clean_text(s):
    return ' '.join(str(s).strip().lower().split())

def generate_content_hash(row: dict) -> str:
    key_fields = [
        clean_text(row.get('titolo', '')),
        clean_text(row.get('data', '')),
        clean_text(row.get('testata', '')),
        clean_text(row.get('giornalista', '')),
        clean_text(row.get('testo_completo', ''))[:500],
    ]
    return hashlib.sha256('|'.join(key_fields).encode('utf-8')).hexdigest()

def parse_date(date_val) -> str:
    if pd.isna(date_val) or str(date_val).strip() == '':
        return datetime.date.today().isoformat()
    try:
        return pd.to_datetime(date_val, dayfirst=True).date().isoformat()
    except Exception:
        return str(date_val)

def parse_ave(value) -> float:
    if pd.isna(value) or str(value).strip() == '':
        return 0.0
    try:
        return float(str(value).replace(',', '.').replace(' ', ''))
    except Exception:
        return 0.0

def normalize_macrosettori(value) -> str:
    if pd.isna(value) or str(value).strip() == '':
        return ''
    tags = [t.strip() for t in str(value).replace(';', ',').split(',') if t.strip()]
    seen, unique = set(), []
    for t in tags:
        if t.lower() not in seen:
            seen.add(t.lower())
            unique.append(t)
    return ', '.join(unique)

def generate_embedding(text: str):
    try:
        if not text or len(text.strip()) == 0:
            text = "nessun contenuto"
        resp = ai.embeddings.create(model="text-embedding-ada-002", input=text[:8000])
        return resp.data[0].embedding
    except Exception as e:
        print(f"Embedding error: {e}")
        return None

def embed_articles(article_ids: list):
    if not article_ids:
        return
    try:
        res = supabase.table("articles").select("id, titolo, occhiello, sottotitolo, testo_completo, macrosettori, dominant_topic").in_("id", article_ids).is_("embedding", "null").execute()
        articles = res.data or []
        print(f"[EMBED] Generazione embedding per {len(articles)} nuovi articoli...")
        for art in articles:
            text = " ".join(filter(None, [art.get('titolo',''), art.get('occhiello',''), art.get('sottotitolo',''), art.get('testo_completo',''), art.get('macrosettori',''), art.get('dominant_topic','')]))
            embedding = generate_embedding(text)
            if embedding:
                supabase.table("articles").update({"embedding": embedding}).eq("id", art['id']).execute()
        print(f"[EMBED] Done.")
    except Exception as e:
        print(f"[EMBED] Errore: {e}")

COLUMN_MAPPING = {
    'testata': 'testata', 'data_testata': 'data', 'pagina_testata': 'pagina_testata',
    'distribuzione_testata': 'distribuzione_testata', 'cadenza_testata': 'cadenza_testata',
    'autore': 'giornalista', 'occhiello': 'occhiello', 'titolo': 'titolo',
    'sottotitolo': 'sottotitolo', 'testo': 'testo_completo', 'macrosettori': 'macrosettori',
    'tipologia_articolo': 'tipologia_articolo', 'ave': 'ave', 'tipo_fonte': 'tipo_fonte',
}

def process_csv(file_path: str) -> dict:
    try:
        try:
            df = pd.read_csv(file_path, sep=None, engine='python', encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, sep=None, engine='python', encoding='latin-1')
        df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
        print(f"--- DEBUG: Colonne CSV rilevate: {df.columns.tolist()} ---")
        missing = set(COLUMN_MAPPING.keys()) - set(df.columns)
        if missing:
            print(f"ATTENZIONE: Colonne mancanti: {missing}.")
        records = []
        for _, row in df.iterrows():
            record = {
                'testata': 'N.D.', 'data': datetime.date.today().isoformat(),
                'pagina_testata': '', 'distribuzione_testata': '', 'cadenza_testata': '',
                'giornalista': 'N.D.', 'occhiello': '', 'titolo': 'Senza Titolo',
                'sottotitolo': '', 'testo_completo': '', 'macrosettori': '',
                'tipologia_articolo': '', 'ave': 0.0, 'tipo_fonte': '',
                'tone': 'Neutral', 'dominant_topic': '', 'reputational_risk': 'None',
                'political_risk': 'None', 'metadata': {}, 'embedding': None,
            }
            for csv_col, db_col in COLUMN_MAPPING.items():
                if csv_col not in df.columns:
                    continue
                value = row.get(csv_col)
                if pd.isna(value) or str(value).strip() == '':
                    continue
                if csv_col == 'data_testata':
                    record['data'] = parse_date(value)
                elif csv_col == 'ave':
                    record['ave'] = parse_ave(value)
                elif csv_col == 'macrosettori':
                    record['macrosettori'] = normalize_macrosettori(value)
                else:
                    record[db_col] = str(value).strip()
            record['content_hash'] = generate_content_hash(record)
            records.append(record)
        if not records:
            return {'status': 'error', 'message': 'CSV vuoto o nessun record valido.'}
        seen_hashes = {}
        for r in records:
            if r['content_hash'] not in seen_hashes:
                seen_hashes[r['content_hash']] = r
        records_deduped = list(seen_hashes.values())
        dup_csv = len(records) - len(records_deduped)
        result = supabase.table('articles').upsert(records_deduped, on_conflict='content_hash').execute()
        inserted_data = result.data or []
        inserted = len(inserted_data)
        skipped = len(records_deduped) - inserted
        new_ids = [r['id'] for r in inserted_data if r.get('id')]
        if new_ids:
            embed_articles(new_ids)
        return {'status': 'success', 'message': f"Elaborati {len(records)} articoli ({dup_csv} duplicati). Inseriti: {inserted}. Presenti: {skipped}. Embedding: {len(new_ids)}."}
    except Exception as e:
        print(f"ERRORE INGESTION: {e}")
        return {'status': 'error', 'message': str(e)}
