import os
import shutil
import uvicorn
import json
import uuid
import time

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import date

# Import moduli core
try:
    from api.ingestion import process_csv
    from services.database import supabase
    from services.ai_logic import ask_spiz
    from api.pitch import pitch_advisor
except ImportError as e:
    print(f"❌ ERRORE IMPORTAZIONE CORE: {e}")

# Import monitor + scheduler (opzionale)
run_monitoring = None
try:
    from services.monitor import run_monitoring
    from apscheduler.schedulers.background import BackgroundScheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_monitoring, 'cron', hour=6, minute=0)
    scheduler.start()
    print("✅ Scheduler monitoraggio avviato (ogni giorno alle 06:00)")
except Exception as e:
    print(f"⚠️ Scheduler non avviato: {e}")

app = FastAPI(title="SPIZ Intelligence")

# Configurazione cartelle
os.makedirs("data/raw", exist_ok=True)
os.makedirs("web", exist_ok=True)

# ── STORAGE TEMPORANEO DOCX ───────────────────────────────────────────

_DOCX_STORE: dict = {}  # token → {path, expires}

def _store_docx(path: Optional[str]) -> Optional[str]:
    """Registra il file docx con un token temporaneo (1 ora)."""
    if not path or not os.path.exists(path):
        return None
    token = str(uuid.uuid4())
    _DOCX_STORE[token] = {"path": path, "expires": time.time() + 3600}
    return token


# ── MODELLI ────────────────────────────────────────────────────────────

class ArticleUpdateSimple(BaseModel):
    titolo:             Optional[str]   = None
    testata:            Optional[str]   = None
    data:               Optional[str]   = None
    giornalista:        Optional[str]   = None
    occhiello:          Optional[str]   = None
    sottotitolo:        Optional[str]   = None
    testo_completo:     Optional[str]   = None
    tone:               Optional[str]   = None
    reputational_risk:  Optional[str]   = None
    political_risk:     Optional[str]   = None
    dominant_topic:     Optional[str]   = None
    macrosettori:       Optional[str]   = None
    tipologia_articolo: Optional[str]   = None
    ave:                Optional[float] = None
    tipo_fonte:         Optional[str]   = None


# ── NAVIGAZIONE ────────────────────────────────────────────────────────

@app.get("/")
async def index():
    if os.path.exists("web/index.html"):
        return FileResponse("web/index.html")
    return {"status": "ok"}

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.get("/chat")
async def chat_page():
    return FileResponse('web/chat.html')

@app.get("/clients")
async def clients_page():
    return FileResponse('web/clienti.html')

@app.get("/monitor")
async def monitor_page():
    return FileResponse('web/monitor.html')

@app.get("/pitch")
async def pitch_page():
    return FileResponse('web/pitch.html')


# ── UPLOAD CSV INGESTIONE ─────────────────────────────────────────────

@app.post("/upload")
async def upload_multiple(files: List[UploadFile] = File(...)):
    results = []
    for file in files:
        try:
            path = f"data/raw/{file.filename}"
            with open(path, "wb") as f:
                shutil.copyfileobj(file.file, f)
            res = process_csv(path)
            results.append({"file": file.filename, "status": "success", "detail": res})
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            results.append({"file": file.filename, "status": "error", "message": str(e)})
    return {"results": results}


# ══════════════════════════════════════════════════════════════════════
# ENDPOINT CHAT — SUPPORTO CSV + DOCX
# ══════════════════════════════════════════════════════════════════════

@app.post("/api/chat")
async def chat_endpoint(
    message:  str = Form(...),
    context:  str = Form("general"),
    history:  str = Form("[]"),
    csv_file: Optional[UploadFile] = File(None),
):
    """
    Endpoint chat principale.
    Accetta:
      - message
      - context
      - history (JSON serializzato)
      - csv_file opzionale
    """

    # Deserializza history
    try:
        hist = json.loads(history) if history else []
    except Exception:
        hist = []

    # Leggi CSV se allegato
    csv_content = None
    if csv_file and csv_file.filename:
        raw = await csv_file.read()
        try:
            csv_content = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            csv_content = raw.decode("latin-1", errors="replace")

    try:
        result = ask_spiz(
            message     = message,
            history     = hist,
            context     = context,
            csv_content = csv_content,
        )
    except Exception as e:
        return {"success": False, "error": str(e)}

    if "error" in result:
        return {"success": False, "error": result["error"]}

    docx_token = _store_docx(result.get("docx_path"))

    return {
        "success":       True,
        "response":      result.get("response", ""),
        "is_report":     result.get("is_report", False),
        "articles_used": result.get("articles_used", 0),
        "total_period":  result.get("total_period", 0),
        "source":        result.get("source", "db"),
        "has_docx":      docx_token is not None,
        "docx_token":    docx_token,
    }


@app.get("/api/download-report/{token}")
async def download_report(token: str):
    entry = _DOCX_STORE.get(token)

    if not entry:
        raise HTTPException(status_code=404, detail="File non trovato o scaduto")

    if time.time() > entry["expires"]:
        del _DOCX_STORE[token]
        raise HTTPException(status_code=410, detail="File scaduto")

    path = entry["path"]

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File non trovato sul disco")

    filename = os.path.basename(path)

    return FileResponse(
        path       = path,
        filename   = filename,
        media_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


# ── AVVIO SERVER ──────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))