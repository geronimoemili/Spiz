import os
import shutil
import uvicorn
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import date

# Import moduli core
try:
    from api.ingestion import process_csv
    from services.database import supabase
    from api.chat import ask_spiz
    from api.pitch import pitch_advisor
except ImportError as e:
    print(f"❌ ERRORE IMPORTAZIONE CORE: {e}")

# Import monitor + scheduler (opzionale, non blocca l'avvio)
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

# ── MODELLI ───────────────────────────────────────────────────
class ChatRequest(BaseModel):
    message:    str
    session_id: str           = "default"
    client_id:  Optional[str] = None

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

# --- ROTTE NAVIGAZIONE ---
@app.get("/")
async def index():
    return FileResponse('web/index.html')

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

# --- API CORE ---
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

@app.post("/api/chat")
async def chat_endpoint(req: ChatRequest):
    try:
        answer = ask_spiz(
            question   = req.message,
            session_id = req.session_id,
            client_id  = req.client_id,
        )
        return {"response": answer}
    except Exception as e:
        return {"response": f"Errore AI: {str(e)}"}

@app.get("/api/clients")
async def get_clients():
    try:
        res = supabase.table("clients").select("id, name, keywords, semantic_topic").execute()
        return res.data or []
    except Exception as e:
        return []

# --- DASHBOARD QUANTITATIVA ---

@app.get("/api/dashboard-stats")
async def get_dashboard_stats():
    try:
        res_total    = supabase.table("articles").select("giornalista", count="exact").execute()
        total_all    = res_total.count or 0
        articles_all = res_total.data or []
        firmati_all  = len([
            a for a in articles_all
            if a.get('giornalista') and a.get('giornalista').strip() not in ["", "Redazione", "N.D."]
        ])
        return {"total": total_all, "firmati": firmati_all, "anonimi": total_all - firmati_all}
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
            keywords = [k.strip().lower() for k in (client.get('keywords') or "").split(',') if k.strip()]
            count    = sum(
                1 for art in articles
                if any(kw in f"{art.get('titolo','')} {art.get('testo_completo','')}".lower() for kw in keywords)
            )
            results.append({"name": client['name'], "today": count, "id": client.get('id'), "keywords": client.get('keywords')})
        return results
    except Exception:
        return []

@app.get("/api/today-stats")
async def get_today_stats():
    today = date.today().isoformat()
    try:
        res            = supabase.table("articles").select("id, giornalista, testata, titolo").eq("data", today).execute()
        articles_today = res.data or []
        total_today    = len(articles_today)

        giornalisti_oggi = {}
        for a in articles_today:
            g = a.get('giornalista') or ''
            if g and g.strip() not in ("", "N.D.", "Redazione", "Autore non indicato"):
                giornalisti_oggi[g] = giornalisti_oggi.get(g, 0) + 1

        testate_oggi = {}
        for a in articles_today:
            t = a.get('testata', 'N/D')
            testate_oggi[t] = testate_oggi.get(t, 0) + 1

        return {
            "total_today":     total_today,
            "giornalisti":     [{"nome": k, "articoli": v} for k, v in sorted(giornalisti_oggi.items(), key=lambda x: -x[1])],
            "testate":         [{"nome": k, "articoli": v} for k, v in sorted(testate_oggi.items(), key=lambda x: -x[1])],
            "ultimi_articoli": sorted(articles_today, key=lambda x: x.get('data', ''), reverse=True)[:5]
        }
    except Exception as e:
        print(f"Errore today-stats: {e}")
        return {"total_today": 0, "giornalisti": [], "testate": [], "ultimi_articoli": []}

@app.get("/api/last-upload")
async def get_last_upload():
    try:
        res = supabase.table("articles").select("data").order("data", desc=True).limit(1).execute()
        return {"last_upload": res.data[0]['data'] if res.data else None}
    except Exception:
        return {"last_upload": None}

# --- DASHBOARD CLIENTI ---

@app.get("/api/client-articles")
async def get_client_articles(client_id: str, from_date: str, to_date: str):
    try:
        client_res = supabase.table("clients").select("*").eq("id", client_id).execute()
        if not client_res.data:
            return {"error": "Cliente non trovato"}
        client   = client_res.data[0]
        keywords = [k.strip().lower() for k in (client.get('keywords') or "").split(',') if k.strip()]
        if not keywords:
            return []

        articles = supabase.table("articles").select(
            "id, testata, data, titolo, giornalista, occhiello, sottotitolo, testo_completo"
        ).gte("data", from_date).lte("data", to_date).execute().data or []

        results = []
        for art in articles:
            text = f"{art.get('titolo','')} {art.get('occhiello','')} {art.get('sottotitolo','')} {art.get('testo_completo','')}".lower()
            if any(kw in text for kw in keywords):
                art.pop('testo_completo', None)
                results.append(art)
        return results
    except Exception as e:
        print(f"Errore client-articles: {e}")
        return {"error": str(e)}

@app.get("/api/article/{article_id}")
async def get_article(article_id: str):
    try:
        res = supabase.table("articles").select("*").eq("id", article_id).execute()
        return res.data[0] if res.data else {"error": "Articolo non trovato"}
    except Exception as e:
        return {"error": str(e)}

@app.put("/api/article/{article_id}")
async def update_article(article_id: str, updates: ArticleUpdateSimple):
    try:
        update_data = {k: v for k, v in updates.dict().items() if v is not None}
        if not update_data:
            return {"error": "Nessun campo da aggiornare"}
        res = supabase.table("articles").update(update_data).eq("id", article_id).execute()
        return res.data[0] if res.data else {"error": "Aggiornamento fallito"}
    except Exception as e:
        return {"error": str(e)}

@app.delete("/api/article/{article_id}")
async def delete_article(article_id: str):
    try:
        res = supabase.table("articles").delete().eq("id", article_id).execute()
        return {"success": True} if res.data else {"error": "Articolo non trovato"}
    except Exception as e:
        return {"error": str(e)}

# --- GESTIONE CLIENTI (CRUD) ---

@app.post("/api/clients")
async def create_client(client: dict):
    try:
        res = supabase.table("clients").insert({
            "name":           client.get("name"),
            "keywords":       client.get("keywords", ""),
            "semantic_topic": client.get("semantic_topic", "")
        }).execute()
        return res.data[0] if res.data else {"error": "Inserimento fallito"}
    except Exception as e:
        return {"error": str(e)}

@app.put("/api/clients/{client_id}")
async def update_client(client_id: str, client: dict):
    try:
        data = {k: client[k] for k in ("name", "keywords", "semantic_topic") if k in client}
        if not data:
            return {"error": "Nessun campo da aggiornare"}
        res = supabase.table("clients").update(data).eq("id", client_id).execute()
        return res.data[0] if res.data else {"error": "Aggiornamento fallito"}
    except Exception as e:
        return {"error": str(e)}

@app.delete("/api/clients/{client_id}")
async def delete_client(client_id: str):
    try:
        res = supabase.table("clients").delete().eq("id", client_id).execute()
        return {"success": True} if res.data else {"error": "Cliente non trovato"}
    except Exception as e:
        return {"error": str(e)}

# --- MONITORAGGIO WEB ---

@app.get("/api/sources")
async def get_sources():
    try:
        res = supabase.table("monitored_sources").select("*").order("name").execute()
        return res.data or []
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/sources")
async def add_source(source: dict):
    try:
        res = supabase.table("monitored_sources").insert({
            "name":   source.get("name"),
            "url":    source.get("url"),
            "type":   source.get("type", "rss"),
            "active": True
        }).execute()
        return res.data[0] if res.data else {"error": "Inserimento fallito"}
    except Exception as e:
        return {"error": str(e)}

@app.delete("/api/sources/{source_id}")
async def delete_source(source_id: str):
    try:
        supabase.table("monitored_sources").delete().eq("id", source_id).execute()
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/monitor/run")
async def trigger_monitor():
    if run_monitoring is None:
        return {"error": "Monitor non disponibile — controlla che services/monitor.py esista"}
    result = run_monitoring()
    return result

@app.get("/api/web-mentions")
async def get_web_mentions(client: str = None, limit: int = 50):
    try:
        query = supabase.table("web_mentions").select("*").order("published_at", desc=True)
        if client:
            query = query.ilike("matched_client", f"%{client}%")
        res = query.limit(limit).execute()
        return res.data or []
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/pitch")
async def api_pitch(req: dict):
    """Analizza comunicato e suggerisce giornalisti"""
    try:
        testo = req.get("testo", "")
        top_n = req.get("top_n", 10)
        result = pitch_advisor(testo, top_n=top_n)
        return result
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/top-giornalisti")
async def get_top_giornalisti(period: str = "30days", limit: int = 20):
    """Giornalisti più prolifici in un periodo"""
    try:
        today = date.today()
        if period == "7days":
            from_date = (today - __import__('datetime').timedelta(days=7)).isoformat()
        elif period == "6months":
            from_date = (today - __import__('datetime').timedelta(days=180)).isoformat()
        else:  # 30days default
            from_date = (today - __import__('datetime').timedelta(days=30)).isoformat()

        to_date = today.isoformat()

        res = supabase.table("articles").select("id, giornalista, testata, titolo, data") \
            .gte("data", from_date).lte("data", to_date).execute()
        articles = res.data or []

        counts = {}
        for a in articles:
            g = (a.get('giornalista') or '').strip()
            if not g or g in ('N.D.', 'N/D', 'Redazione', 'Autore non indicato', ''):
                continue
            if g not in counts:
                counts[g] = 0
            counts[g] += 1

        top = sorted(counts.items(), key=lambda x: -x[1])[:limit]
        return [{"nome": k, "articoli": v} for k, v in top]
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/giornalista-articoli")
async def get_giornalista_articoli(nome: str, period: str = "30days"):
    """Articoli di un giornalista specifico in un periodo"""
    try:
        today = date.today()
        if period == "7days":
            from_date = (today - __import__('datetime').timedelta(days=7)).isoformat()
        elif period == "6months":
            from_date = (today - __import__('datetime').timedelta(days=180)).isoformat()
        else:
            from_date = (today - __import__('datetime').timedelta(days=30)).isoformat()

        res = supabase.table("articles") \
            .select("id, titolo, testata, data, occhiello") \
            .eq("giornalista", nome) \
            .gte("data", from_date) \
            .lte("data", today.isoformat()) \
            .order("data", desc=True) \
            .execute()
        return res.data or []
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)