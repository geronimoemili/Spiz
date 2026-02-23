import os
import shutil
import uvicorn
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import date

# Import moduli interni
try:
    from api.ingestion import process_csv
    from services.database import supabase
    from api.chat import ask_spiz
except ImportError as e:
    print(f"❌ ERRORE IMPORTAZIONE: {e}")

app = FastAPI(title="SPIZ Intelligence Dashboard")

# Configurazione cartelle
os.makedirs("data/raw", exist_ok=True)
os.makedirs("web", exist_ok=True)

# ── MODELLO CHAT AGGIORNATO ───────────────────────────────────────────────────
class ChatRequest(BaseModel):
    message:    str
    session_id: str           = "default"   # il frontend può passare un ID univoco per utente/sessione
    client_id:  Optional[str] = None        # UUID o nome cliente — opzionale

# --- ROTTE NAVIGAZIONE ---
@app.get("/")
async def index():
    return FileResponse('web/index.html')

@app.get("/clients")
async def clients_page():
    return FileResponse('web/clienti.html')

# --- API CORE ---
@app.post("/upload")
async def upload_multiple(files: List[UploadFile] = File(...)):
    """Gestisce il caricamento di uno o più CSV"""
    results = []
    for file in files:
        try:
            path = f"data/raw/{file.filename}"
            with open(path, "wb") as f:
                shutil.copyfileobj(file.file, f)
            res = process_csv(path)
            results.append({"file": file.filename, "status": "success"})
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            results.append({"file": file.filename, "status": "error", "message": str(e)})
    return {"results": results}

# ── ENDPOINT CHAT AGGIORNATO ──────────────────────────────────────────────────
@app.post("/api/chat")
async def chat_endpoint(req: ChatRequest):
    try:
        answer = ask_spiz(
            question   = req.message,
            session_id = req.session_id,
            client_id  = req.client_id,   # None se non passato → nessun filtro cliente
        )
        return {"response": answer}
    except Exception as e:
        return {"response": f"Errore AI: {str(e)}"}

# ── ENDPOINT LISTA CLIENTI (per il frontend) ──────────────────────────────────
@app.get("/api/clients")
async def get_clients():
    """Restituisce la lista clienti — utile per il selettore nel frontend."""
    try:
        res = supabase.table("clients").select("id, name, keywords, semantic_topic").execute()
        return res.data or []
    except Exception as e:
        return []

# --- API DASHBOARD DINAMICA ---
@app.get("/api/dashboard-stats")
async def get_dashboard_stats():
    """Restituisce il totale complessivo degli articoli (non solo oggi)"""
    try:
        # Totale complessivo articoli
        res_total = supabase.table("articles").select("giornalista", count="exact").execute()
        total_all = res_total.count or 0
        articles_all = res_total.data or []
        firmati_all = len([
            a for a in articles_all
            if a.get('giornalista') and a.get('giornalista').strip() not in ["", "Redazione", "N.D."]
        ])
        anonimi_all = total_all - firmati_all

        return {
            "total": total_all,
            "firmati": firmati_all,
            "anonimi": anonimi_all
        }
    except Exception as e:
        print(f"Errore in dashboard-stats: {e}")
        return {"total": 0, "firmati": 0, "anonimi": 0}

@app.get("/api/today-mentions")
async def get_today_mentions():
    try:
        today    = date.today().isoformat()
        clients  = supabase.table("clients").select("*").execute().data or []
        articles = supabase.table("articles").select("titolo, testo_completo").eq("data", today).execute().data or []
        results  = []
        for client in clients:
            keywords = [k.strip().lower() for k in (client.get('keywords') or "").split(',')]
            count    = sum(
                1 for art in articles
                if any(
                    kw in f"{art.get('titolo','')} {art.get('testo_completo','')}".lower()
                    for kw in keywords if kw
                )
            )
            results.append({"name": client['name'], "today": count, "id": client.get('id')})
        return results
    except Exception:
        return []

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)