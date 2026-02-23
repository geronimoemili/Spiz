import pandas as pd
import hashlib
from services.database import supabase
import datetime

def clean_text(s):
    return ' '.join(str(s).strip().lower().split())

def generate_content_hash(row):
    key_fields = [
        clean_text(row.get('titolo', '')),
        clean_text(row.get('data', '')),
        clean_text(row.get('testata', '')),
        clean_text(row.get('autore', '')),
        clean_text(row.get('testo_completo', row.get('testo', '')))[:500]
    ]
    key_string = '|'.join(key_fields)
    return hashlib.sha256(key_string.encode('utf-8')).hexdigest()

def process_csv(file_path):
    try:
        # Carica CSV
        try:
            df = pd.read_csv(file_path, sep=None, engine='python')
        except:
            df = pd.read_csv(file_path)

        df.columns = [c.strip().lower() for c in df.columns]
        print(f"--- DEBUG: Colonne CSV rilevate: {df.columns.tolist()} ---")

        records = []
        for _, row in df.iterrows():
            autore = row.get('autore') if 'autore' in df.columns else "Autore non indicato"
            testo = str(row.get('testo_completo', row.get('testo', "")))
            content_hash = generate_content_hash(row)

            record = {
                "titolo": str(row.get('titolo', "Senza Titolo")),
                "testata": str(row.get('testata', "N.D.")),
                "data": str(row.get('data', datetime.date.today().isoformat())),
                "giornalista": str(autore) if pd.notna(autore) else "N.D.",
                "testo_completo": testo,
                "occhiello": str(row.get('occhiello', "")),
                "sottotitolo": str(row.get('sottotitolo', "")),
                "ave": float(row.get('ave', 0)) if pd.notna(row.get('ave')) else 0,
                "tone": "Neutral",
                "reputational_risk": "None",
                "content_hash": content_hash
            }
            records.append(record)

        if records:
            # UPSERT: sfrutta il vincolo UNIQUE su content_hash
            result = supabase.table("articles").upsert(records, on_conflict="content_hash").execute()
            inserted = len(result.data) if result.data else 0
            return {
                "status": "success",
                "message": f"Elaborati {len(records)} articoli. Inseriti: {inserted}. Duplicati ignorati: {len(records)-inserted}."
            }

        return {"status": "error", "message": "CSV vuoto."}

    except Exception as e:
        print(f"ERRORE INGESTION: {str(e)}")
        return {"status": "error", "message": str(e)}