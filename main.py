import os
import shutil
import uvicorn
import json
import uuid
import time
import threading
import imaplib
import email as _email_lib
import hashlib as _hashlib
from email.header import decode_header as _decode_header
from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query, Request
from fastapi.responses import FileResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from datetime import date, timedelta, datetime, timezone
from collections import Counter

try:
    from api.ingestion import process_csv
    from services.database import supabase
    from api.chat import ask_spiz, generate_digest
    from api.pitch import pitch_advisor
except ImportError as e:
    print(f"❌ ERRORE IMPORTAZIONE CORE: {e}")

# ── SCHEDULER ──────────────────────────────────────────────────────
run_monitoring = None
try:
    from services.monitor import run_monitoring
    from apscheduler.schedulers.background import BackgroundScheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_monitoring, 'cron', hour=6, minute=0)

    def _scheduled_gmail_check():
        now = datetime.now()
        if 7 <= now.hour < 11:
            print(f"[GMAIL] Controllo automatico ore {now.strftime('%H:%M')}")
            _run_gmail_import(auto=True)

    scheduler.add_job(_scheduled_gmail_check, 'interval', minutes=15)
    scheduler.start()
    print("✅ Scheduler avviato (monitor 06:00 + Gmail ogni 15 min)")
except Exception as e:
    print(f"⚠️ Scheduler non avviato: {e}")

# ── APP ─────────────────────────────────────────────────────────────
app = FastAPI(title="MAIM Intelligence")
app.mount("/static", StaticFiles(directory="web"), name="static")
os.makedirs("data/raw", exist_ok=True)
os.makedirs("web", exist_ok=True)

# ── ROUTER GIORNALISTI ──────────────────────────────────────────────
try:
    from api.journalists import router as journalists_router
    app.include_router(journalists_router)
    print("✅ Router giornalisti caricato")
except ImportError as e:
    print(f"⚠️ Router giornalisti non disponibile: {e}")

# ── JOB QUEUE ───────────────────────────────────────────────────────
_JOBS = {}
_JOBS_LOCK = threading.Lock()
_DOCX_STORE = {}

def _set_job(job_id, status, result=None, error=None):
    with _JOBS_LOCK:
        _JOBS[job_id] = {"status": status, "result": result, "error": error, "created": time.time()}

def _get_job(job_id):
    with _JOBS_LOCK:
        return _JOBS.get(job_id)

def _cleanup_old_jobs():
    cutoff = time.time() - 1800
    with _JOBS_LOCK:
        for k in [k for k, v in _JOBS.items() if v["created"] < cutoff]:
            del _JOBS[k]

# ── MODELLI ─────────────────────────────────────────────────────────
class ChatRequest(BaseModel):
    message: Optional[str] = ""; context: Optional[str] = "week"
    history: Optional[list] = []; client_name: Optional[str] = ""; topic_name: Optional[str] = ""

class GenerateReportRequest(BaseModel):
    client_name: Optional[str] = ""; topic_name: Optional[str] = ""
    article_ids: Optional[List[str]] = []; report_type: Optional[str] = "posizionamento_giornalisti"
    refinement: Optional[str] = ""

class ArticleUpdateSimple(BaseModel):
    titolo: Optional[str] = None; testata: Optional[str] = None; data: Optional[str] = None
    giornalista: Optional[str] = None; occhiello: Optional[str] = None; sottotitolo: Optional[str] = None
    testo_completo: Optional[str] = None; tone: Optional[str] = None; reputational_risk: Optional[str] = None
    political_risk: Optional[str] = None; dominant_topic: Optional[str] = None; macrosettori: Optional[str] = None
    tipologia_articolo: Optional[str] = None; ave: Optional[float] = None; tipo_fonte: Optional[str] = None

class ClientModel(BaseModel):
    name: Optional[str] = None; keywords: Optional[str] = None; keywords_web: Optional[str] = None
    sector: Optional[str] = None; description: Optional[str] = None; website: Optional[str] = None
    contact: Optional[str] = None; semantic_topic: Optional[str] = None; macro_strategici: Optional[str] = None

class SourceModel(BaseModel):
    name: str; url: str; type: Optional[str] = "rss"; active: Optional[bool] = True

class HistoricalScanRequest(BaseModel):
    from_date: str; to_date: str

class ShareRequest(BaseModel):
    article_ids: List[str]

class DeleteMentionsRequest(BaseModel):
    ids: List[str]

class DigestAudioRequest(BaseModel):
    text: str

class TestataUpdate(BaseModel):
    testata: str; tier: int; ordine: Optional[int] = None

class TestateUpdateRequest(BaseModel):
    testate: List[TestataUpdate]


# ══════════════════════════════════════════════════════════════════
# NAVIGAZIONE
# ══════════════════════════════════════════════════════════════════

@app.get("/")
async def root(): return FileResponse("web/home.html")
@app.get("/home")
async def home_page(): return FileResponse("web/home.html")
@app.get("/press")
async def press_page(): return FileResponse("web/press.html")
@app.get("/dashboard")
async def dashboard_page(): return FileResponse("web/press.html")
@app.get("/web")
async def web_page(): return FileResponse("web/web.html")
@app.get("/monitor")
async def monitor_page(): return FileResponse("web/web.html")
@app.get("/chat")
async def chat_page(): return FileResponse("web/chat.html")
@app.get("/clients")
async def clients_page(): return FileResponse("web/clienti.html")
@app.get("/pitch")
async def pitch_page(): return FileResponse("web/pitch.html")
@app.get("/web-digest")
async def web_digest_page(): return FileResponse("web/web_digest.html")
@app.get("/webdigest")
async def webdigest_admin_page(): return FileResponse("web/webdigest.html")
@app.get("/giornalisti")
async def giornalisti_page(): return FileResponse("web/giornalisti.html")

@app.get("/intelligence")
async def intelligence_page(): return FileResponse("web/intelligence.html")
@app.get("/testate")
async def testate_page(): return FileResponse("web/testate.html")
@app.get("/health")
async def health_check(): return {"status": "ok"}
@app.get("/healthcheck")
async def healthcheck(): return {"status": "ok"}


# ══════════════════════════════════════════════════════════════════
# UPLOAD
# ══════════════════════════════════════════════════════════════════

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
            if os.path.exists(path): os.remove(path)
        except Exception as e:
            results.append({"file": file.filename, "status": "error", "message": str(e)})
    digest_job_id = None
    if any(r["status"] == "success" for r in results):
        _cleanup_old_jobs()
        digest_job_id = str(uuid.uuid4())[:12]
        _set_job(digest_job_id, "pending")
        threading.Thread(target=_run_digest_job, args=(digest_job_id,), daemon=True).start()
        # Sync automatico giornalisti nel CRM dopo ogni ingestion
        threading.Thread(target=_sync_journalists_auto, daemon=True).start()
    return {"results": results, "digest_job_id": digest_job_id}


def _sync_journalists_auto():
    """Importa automaticamente nel CRM i nuovi giornalisti dopo ogni ingestion."""
    try:
        SKIP = {"", "n.d.", "n/d", "redazione", "autore non indicato"}
        arts = supabase.table("articles").select("giornalista, testata").execute().data or []
        existing = {j["nome"].strip().lower() for j in (supabase.table("journalists").select("nome").execute().data or [])}
        from collections import Counter as _C
        testate = {}
        for a in arts:
            g = (a.get("giornalista") or "").strip()
            if not g or g.lower() in SKIP: continue
            testate.setdefault(g.lower(), _C())[a.get("testata","") or ""] += 1
        inserted = 0
        for gl, tc in testate.items():
            if gl in existing: continue
            tm = tc.most_common(1)[0][0]
            tipo = "agenzia" if any(x in tm.lower() for x in ["ansa","adnkronos","askanews"]) else                    "web" if any(x in tm.lower() for x in [".it","web","online"]) else                    "radio_tv" if any(x in tm.lower() for x in ["radio","tv","rai","tele"]) else "quotidiano"
            try:
                supabase.table("journalists").insert({"nome": gl.title(), "testata_principale": tm, "tipo_testata": tipo}).execute()
                inserted += 1
            except Exception: pass
        print(f"[SYNC] {inserted} nuovi giornalisti aggiunti al CRM")
    except Exception as e:
        print(f"[SYNC] Errore sync giornalisti: {e}")


# ══════════════════════════════════════════════════════════════════
# AI REPORT / CHAT
# ══════════════════════════════════════════════════════════════════

def _run_report_job(job_id, client_name, topic_name, articles, report_type="posizionamento_giornalisti", refinement=""):
    import traceback
    try:
        result = ask_spiz(client_name=client_name, topic_name=topic_name,
                          preloaded_articles=articles, report_type=report_type, refinement=refinement)
        if "error" in result: _set_job(job_id, "error", error=result["error"])
        else: _set_job(job_id, "done", result=result)
    except Exception as e:
        _set_job(job_id, "error", error=str(e) + " | " + traceback.format_exc().splitlines()[-1])

@app.post("/api/generate-report")
async def generate_report_endpoint(req: GenerateReportRequest):
    _cleanup_old_jobs()
    if not req.article_ids: return {"success": False, "error": "Nessun articolo selezionato."}
    DB_COLS = "id, testata, data, giornalista, occhiello, titolo, sottotitolo, testo_completo, macrosettori, tipologia_articolo, tone, dominant_topic, reputational_risk, political_risk, ave, tipo_fonte"
    try:
        res = supabase.table("articles").select(DB_COLS).in_("id", req.article_ids).execute()
        articles = res.data or []
    except Exception as e:
        return {"success": False, "error": f"Errore DB: {e}"}
    if not articles: return {"success": False, "error": "Articoli non trovati."}
    job_id = str(uuid.uuid4())[:12]
    _set_job(job_id, "pending")
    threading.Thread(target=_run_report_job, args=(job_id, req.client_name or "", req.topic_name or "", articles, req.report_type or "posizionamento_giornalisti", req.refinement or ""), daemon=True).start()
    return {"success": True, "job_id": job_id}

@app.get("/api/job/{job_id}")
async def get_job_status(job_id: str):
    job = _get_job(job_id)
    if not job: return {"status": "error", "error": "Job non trovato o scaduto."}
    if job["status"] == "pending": return {"status": "pending"}
    if job["status"] == "error": return {"status": "error", "error": job["error"]}
    result = job["result"] or {}
    return {"status": "done", "response": result.get("response",""), "articles_used": result.get("articles_used",0),
            "period_from": result.get("period_from",""), "period_to": result.get("period_to",""),
            "text": result.get("text",""), "articles_today": result.get("articles_today",0), "client_mentions": result.get("client_mentions",0)}

@app.post("/api/chat")
async def chat_endpoint(req: ChatRequest):
    try:
        result = ask_spiz(message=req.message or "", history=req.history or [], context=req.context or "week",
                          client_name=req.client_name or "", topic_name=req.topic_name or "")
    except Exception as e:
        return {"success": False, "error": str(e)}
    if "error" in result: return {"success": False, "error": result["error"]}
    return {"success": True, "response": result.get("response",""), "is_report": result.get("is_report",False),
            "articles_used": result.get("articles_used",0), "period_from": result.get("period_from",""),
            "period_to": result.get("period_to",""), "articles_list": result.get("articles_list",[])}

@app.get("/api/download-report/{token}")
async def download_report(token: str):
    entry = _DOCX_STORE.get(token)
    if not entry: raise HTTPException(status_code=404, detail="File non trovato o scaduto")
    if time.time() > entry["expires"]: del _DOCX_STORE[token]; raise HTTPException(status_code=410, detail="Scaduto")
    path = entry["path"]
    if not os.path.exists(path): raise HTTPException(status_code=404, detail="File non trovato")
    return FileResponse(path=path, filename=os.path.basename(path),
                        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document")


# ══════════════════════════════════════════════════════════════════
# STATS
# ══════════════════════════════════════════════════════════════════

@app.get("/api/dashboard-stats")
async def dashboard_stats():
    try:
        today = date.today().isoformat()
        week_ago = (date.today() - timedelta(days=7)).isoformat()
        month_ago = (date.today() - timedelta(days=30)).isoformat()
        total = supabase.table("articles").select("id", count="exact").execute()
        oggi = supabase.table("articles").select("id", count="exact").eq("data", today).execute()
        settimana = supabase.table("articles").select("id", count="exact").gte("data", week_ago).execute()
        mese = supabase.table("articles").select("id", count="exact").gte("data", month_ago).execute()
        return {"totale": total.count or 0, "oggi": oggi.count or 0, "settimana": settimana.count or 0, "mese": mese.count or 0}
    except Exception as e:
        return {"totale": 0, "oggi": 0, "settimana": 0, "mese": 0, "error": str(e)}

@app.get("/api/last-upload")
async def last_upload():
    try:
        res = supabase.table("articles").select("data, testata").order("data", desc=True).limit(1).execute()
        if res.data: return {"data": res.data[0].get("data"), "testata": res.data[0].get("testata")}
        return {"data": None, "testata": None}
    except Exception as e: return {"data": None, "error": str(e)}

@app.get("/api/today-stats")
async def today_stats():
    try:
        today = date.today().isoformat()
        articles = supabase.table("articles").select("testata, tone, macrosettori, giornalista").eq("data", today).execute().data or []
        testate_c = Counter(a.get("testata","") for a in articles if a.get("testata"))
        giornalisti_c = Counter(a.get("giornalista","") for a in articles
                                if a.get("giornalista") and a["giornalista"].lower() not in ("redazione","n.d.","n/d",""))
        tones = Counter(a.get("tone","") for a in articles if a.get("tone"))
        tone_tot = sum(tones.values()) or 1
        return {"total_today": len(articles), "totale": len(articles),
                "testate": [{"name": k, "count": v} for k, v in testate_c.most_common(10)],
                "giornalisti": [{"nome": k, "articoli": v} for k, v in giornalisti_c.most_common(20)],
                "sentiment": {k: round(v/tone_tot*100) for k, v in tones.items() if k}}
    except Exception as e:
        return {"total_today": 0, "totale": 0, "testate": [], "giornalisti": [], "sentiment": {}, "error": str(e)}

@app.get("/api/today-mentions")
async def today_mentions():
    try:
        today = date.today().isoformat()
        clients = supabase.table("clients").select("*").execute().data or []
        articles = supabase.table("articles").select("id, titolo, testata, giornalista, tone, dominant_topic, testo_completo, occhiello").eq("data", today).execute().data or []
        result = []
        for cl in clients:
            raw_kw = cl.get("keywords_press") or cl.get("keywords") or ""
            kws = [k.strip().lower() for k in raw_kw.split(",") if k.strip()]
            count = sum(1 for a in articles if kws and any(
                kw in (a.get("testo_completo") or "").lower() or kw in (a.get("titolo") or "").lower() or kw in (a.get("occhiello") or "").lower()
                for kw in kws)) if kws else 0
            result.append({"id": cl["id"], "name": cl.get("name",""), "keywords": raw_kw, "today": count})
        return result
    except Exception: return []

@app.get("/api/macro-groups")
async def get_macro_groups():
    try:
        res = supabase.table("macro_groups").select("id, name").eq("active", True).order("name").execute()
        return {"groups": res.data or []}
    except Exception as e: return {"error": str(e)}

@app.get("/api/macro-group-articles")
async def get_macro_group_articles(macro_group_id: str, from_date: str, to_date: str):
    try:
        links = supabase.table("macro_group_links").select("official_macro_id").eq("macro_group_id", macro_group_id).execute()
        official_ids = [l["official_macro_id"] for l in (links.data or [])]
        if not official_ids: return {"articles": []}
        macro_names = [m["name"] for m in (supabase.table("official_macrosectors").select("name").in_("id", official_ids).execute().data or [])]
        articles = supabase.table("articles").select("id, titolo, testata, data, giornalista, macrosettori").gte("data", from_date).lte("data", to_date).order("data", desc=True).limit(300).execute().data or []
        return {"articles": [a for a in articles if a.get("macrosettori") and any(m.strip() in macro_names for m in a["macrosettori"].split(","))]}
    except Exception as e: return {"error": str(e)}

@app.get("/api/macro-groups-count")
async def macro_groups_count(from_date: Optional[str] = None, to_date: Optional[str] = None):
    try:
        if not from_date: from_date = date.today().isoformat()
        if not to_date:   to_date   = date.today().isoformat()
        groups = supabase.table("macro_groups").select("id, name").eq("active", True).order("name").execute().data or []
        if not groups: return {"groups": []}
        all_links = supabase.table("macro_group_links").select("macro_group_id, official_macro_id").execute()
        links_by_group = {}
        for lnk in (all_links.data or []):
            links_by_group.setdefault(lnk["macro_group_id"], []).append(lnk["official_macro_id"])
        all_oids = list({oid for ids in links_by_group.values() for oid in ids})
        macro_names_map = {}
        if all_oids:
            for m in (supabase.table("official_macrosectors").select("id, name").in_("id", all_oids).execute().data or []):
                macro_names_map[m["id"]] = m["name"]
        articles = supabase.table("articles").select("id, macrosettori").gte("data", from_date).lte("data", to_date).execute().data or []
        result = []
        for g in groups:
            names_set = {macro_names_map[oid] for oid in links_by_group.get(g["id"],[]) if oid in macro_names_map}
            count = sum(1 for a in articles if a.get("macrosettori") and any(m.strip() in names_set for m in a["macrosettori"].split(","))) if names_set else 0
            result.append({"id": g["id"], "name": g["name"], "count": count})
        return {"groups": result}
    except Exception as e: return {"groups": [], "error": str(e)}


# ══════════════════════════════════════════════════════════════════
# ARTICOLI
# ══════════════════════════════════════════════════════════════════

@app.get("/api/articles-filtered")
async def get_articles_filtered(from_date: str, to_date: str, client_id: Optional[str] = None, macro_group_id: Optional[str] = None, topic: Optional[str] = None):
    try:
        articles = supabase.table("articles").select("id, titolo, testata, data, giornalista, macrosettori, testo_completo, occhiello, ave").gte("data", from_date).lte("data", to_date).order("ave", desc=True).limit(500).execute().data or []
        if client_id:
            cl = supabase.table("clients").select("*").eq("id", client_id).execute()
            if cl.data:
                kws = [k.strip().lower() for k in (cl.data[0].get("keywords_press") or cl.data[0].get("keywords") or "").split(",") if k.strip()]
                if kws: articles = [a for a in articles if any(kw in (a.get("testo_completo") or "").lower() or kw in (a.get("titolo") or "").lower() or kw in (a.get("occhiello") or "").lower() for kw in kws)]
        elif topic:
            tl = topic.lower()
            articles = [a for a in articles if tl in (a.get("titolo") or "").lower() or tl in (a.get("testo_completo") or "").lower() or tl in (a.get("occhiello") or "").lower()]
        if macro_group_id:
            links = supabase.table("macro_group_links").select("official_macro_id").eq("macro_group_id", macro_group_id).execute()
            oids = [l["official_macro_id"] for l in (links.data or [])]
            if oids:
                mac_names = [m["name"] for m in (supabase.table("official_macrosectors").select("name").in_("id", oids).execute().data or [])]
                articles = [a for a in articles if a.get("macrosettori") and any(m.strip() in mac_names for m in a["macrosettori"].split(","))]
        return {"articles": articles}
    except Exception as e: return {"error": str(e), "articles": []}

@app.get("/api/articles")
async def get_articles(from_date: Optional[str] = None, to_date: Optional[str] = None, testata: Optional[str] = None, limit: int = 50):
    try:
        q = supabase.table("articles").select("id, titolo, testata, data, occhiello, giornalista, tone, dominant_topic, macrosettori")
        if from_date: q = q.gte("data", from_date)
        if to_date:   q = q.lte("data", to_date)
        if testata:   q = q.eq("testata", testata)
        res = q.order("data", desc=True).limit(limit).execute()
        return {"articles": res.data or [], "total": len(res.data or [])}
    except Exception as e: return {"error": str(e)}

@app.get("/api/article/{article_id}")
async def get_article(article_id: str):
    try:
        res = supabase.table("articles").select("*").eq("id", article_id).execute()
        if not res.data: raise HTTPException(status_code=404, detail="Articolo non trovato")
        return res.data[0]
    except HTTPException: raise
    except Exception as e: return {"error": str(e)}

@app.put("/api/article/{article_id}")
async def update_article(article_id: str, data: ArticleUpdateSimple):
    try:
        update_data = {k: v for k, v in data.dict().items() if v is not None}
        if not update_data: raise HTTPException(status_code=400, detail="Nessun campo da aggiornare")
        res = supabase.table("articles").update(update_data).eq("id", article_id).execute()
        return res.data[0] if res.data else {"success": True}
    except HTTPException: raise
    except Exception as e: return {"error": str(e)}

@app.delete("/api/article/{article_id}")
async def delete_article(article_id: str):
    try:
        supabase.table("articles").delete().eq("id", article_id).execute()
        return {"success": True}
    except Exception as e: return {"error": str(e)}

@app.get("/api/client-articles")
async def get_client_articles(client_id: str, from_date: str, to_date: str):
    try:
        cl = supabase.table("clients").select("*").eq("id", client_id).execute()
        if not cl.data: raise HTTPException(status_code=404, detail="Cliente non trovato")
        kws = [k.strip().lower() for k in (cl.data[0].get("keywords") or "").split(",") if k.strip()]
        arts = supabase.table("articles").select("id, testata, data, giornalista, occhiello, titolo, sottotitolo, testo_completo, macrosettori, tipologia_articolo, tone, dominant_topic, reputational_risk, political_risk, ave, tipo_fonte").gte("data", from_date).lte("data", to_date).order("data", desc=True).execute().data or []
        filtered = [a for a in arts if any(kw in (a.get("testo_completo") or "").lower() or kw in (a.get("titolo") or "").lower() or kw in (a.get("occhiello") or "").lower() for kw in kws)] if kws else arts
        return {"client": cl.data[0], "articles": filtered, "total": len(filtered)}
    except HTTPException: raise
    except Exception as e: return {"error": str(e)}

@app.get("/api/debug-articles")
async def debug_articles():
    try:
        res = supabase.table("articles").select("id, titolo, data, testata, giornalista").order("data", desc=True).limit(5).execute()
        clients = supabase.table("clients").select("id, name, keywords_press, keywords_web, macro_strategici").execute()
        total = supabase.table("articles").select("id", count="exact").execute()
        today = date.today().isoformat()
        oggi = supabase.table("articles").select("id").eq("data", today).execute().data or []
        last = supabase.table("articles").select("data").order("data", desc=True).limit(1).execute()
        return {"ultimi_articoli": res.data, "totale_articoli": total.count, "articoli_oggi": len(oggi),
                "ultima_data": last.data[0]["data"] if last.data else None, "clienti": clients.data}
    except Exception as e: return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════
# SHARE
# ══════════════════════════════════════════════════════════════════

@app.post("/api/share")
async def create_share(req: ShareRequest):
    try:
        if not req.article_ids: return {"error": "Nessun articolo selezionato"}
        token = str(uuid.uuid4())[:8]
        expires_at = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
        supabase.table("shared_reports").insert({"token": token, "filters": {"article_ids": req.article_ids}, "expires_at": expires_at}).execute()
        return {"token": token}
    except Exception as e: return {"error": str(e)}

@app.get("/share/{token}")
async def read_share(token: str):
    try:
        now = datetime.now(timezone.utc).isoformat()
        row = supabase.table("shared_reports").select("*").eq("token", token).gt("expires_at", now).execute()
        if not row.data: return PlainTextResponse("Link scaduto o non trovato.", status_code=404)
        article_ids = row.data[0]["filters"].get("article_ids", [])
        if not article_ids: return PlainTextResponse("Nessun articolo salvato.", status_code=404)
        res = supabase.table("articles").select("id, titolo, testata, data, giornalista, macrosettori, testo_completo").in_("id", article_ids).execute()
        id_order = {aid: i for i, aid in enumerate(article_ids)}
        articles = sorted(res.data or [], key=lambda a: id_order.get(a["id"], 9999))
        lines = [f"ARCHIVIO MAIM - {len(articles)} articoli", ""]
        for i, a in enumerate(articles, 1):
            lines += ["---", f"[{i}] {a.get('titolo') or 'N/D'}",
                      f"Testata: {a.get('testata') or 'N/D'} | Data: {a.get('data') or 'N/D'} | Giornalista: {a.get('giornalista') or 'N/D'}",
                      f"Settori: {a.get('macrosettori') or 'N/D'}", "", a.get("testo_completo") or "Testo non disponibile", ""]
        return PlainTextResponse("\n".join(lines))
    except Exception as e: return PlainTextResponse("Errore: " + str(e), status_code=500)


# ══════════════════════════════════════════════════════════════════
# CLIENTI
# ══════════════════════════════════════════════════════════════════

@app.get("/api/clients")
async def get_clients():
    try: return supabase.table("clients").select("*").execute().data or []
    except Exception as e: return {"error": str(e)}

@app.post("/api/clients")
async def create_client(data: ClientModel):
    try:
        if not data.name or not data.name.strip(): raise HTTPException(status_code=400, detail="Il nome è obbligatorio")
        res = supabase.table("clients").insert({"name": data.name.strip(), "keywords": data.keywords, "keywords_web": data.keywords_web,
            "sector": data.sector, "description": data.description, "website": data.website,
            "contact": data.contact, "semantic_topic": data.semantic_topic, "macro_strategici": data.macro_strategici}).execute()
        return {"success": True, "id": res.data[0].get("id") if res.data else None, "client": res.data[0] if res.data else {}}
    except HTTPException: raise
    except Exception as e: return {"error": str(e)}

@app.put("/api/clients/{client_id}")
async def update_client(client_id: str, data: ClientModel):
    try:
        update_data = {k: v for k, v in data.dict().items() if v is not None}
        if not update_data: return {"success": True, "id": client_id, "client": {}}
        res = supabase.table("clients").update(update_data).eq("id", client_id).execute()
        return {"success": True, "id": client_id, "client": res.data[0] if res.data else {}}
    except Exception as e: return {"error": str(e)}

@app.delete("/api/clients/{client_id}")
async def delete_client(client_id: str):
    try:
        supabase.table("clients").delete().eq("id", client_id).execute()
        return {"success": True, "id": client_id}
    except Exception as e: return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════
# FONTI
# ══════════════════════════════════════════════════════════════════

@app.get("/api/sources")
async def get_sources():
    try: return supabase.table("monitored_sources").select("*").order("name").execute().data or []
    except Exception as e: return {"error": str(e)}

@app.post("/api/sources")
async def create_source(data: SourceModel):
    try:
        res = supabase.table("monitored_sources").insert({"name": data.name, "url": data.url, "type": data.type, "active": data.active}).execute()
        return res.data[0] if res.data else {"success": True}
    except Exception as e: return {"error": str(e)}

@app.delete("/api/sources/{source_id}")
async def delete_source(source_id: str):
    try:
        supabase.table("monitored_sources").delete().eq("id", source_id).execute()
        return {"success": True}
    except Exception as e: return {"error": str(e)}

@app.patch("/api/sources/{source_id}/toggle")
async def toggle_source(source_id: str, request: Request):
    try:
        body = await request.json()
        supabase.table("monitored_sources").update({"active": body.get("active", True)}).eq("id", source_id).execute()
        return {"success": True}
    except Exception as e: return {"error": str(e)}

@app.get("/api/monitored-sources")
async def get_monitored_sources_legacy(): return await get_sources()
@app.post("/api/monitored-sources")
async def create_monitored_source_legacy(data: SourceModel): return await create_source(data)
@app.delete("/api/monitored-sources/{source_id}")
async def delete_monitored_source_legacy(source_id: str): return await delete_source(source_id)
@app.patch("/api/monitored-sources/{source_id}/toggle")
async def toggle_monitored_source_legacy(source_id: str, active: bool = Query(...)):
    try:
        supabase.table("monitored_sources").update({"active": active}).eq("id", source_id).execute()
        return {"success": True}
    except Exception as e: return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════
# WEB MENTIONS / WEB SCAN
# ══════════════════════════════════════════════════════════════════

@app.get("/api/web-mentions")
async def get_web_mentions(client: Optional[str] = None, limit: int = 50):
    try:
        q = supabase.table("web_mentions").select("*").order("published_at", desc=True)
        if client: q = q.ilike("matched_client", f"%{client}%")
        return q.limit(limit).execute().data or []
    except Exception as e: return {"error": str(e)}

@app.post("/api/web-mentions/delete-bulk")
async def delete_web_mentions_bulk(req: DeleteMentionsRequest):
    try:
        supabase.table("web_mentions").delete().in_("id", req.ids).execute()
        return {"success": True, "deleted": len(req.ids)}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

_scan_progress = {}
_scan_progress_lock = threading.Lock()

def _set_scan_progress(job_id, **kwargs):
    with _scan_progress_lock:
        if job_id not in _scan_progress: _scan_progress[job_id] = {}
        _scan_progress[job_id].update(kwargs)

def _get_scan_progress(job_id):
    with _scan_progress_lock: return dict(_scan_progress.get(job_id, {}))

def _run_web_scan(job_id):
    import time as _time
    _set_scan_progress(job_id, status="running", current=0, total=0, current_client="", found=0, duplicates=0, errors=[])
    try:
        sources = supabase.table("monitored_sources").select("id, name").eq("active", True).execute().data or []
        _set_scan_progress(job_id, total=len(sources))
        if not sources:
            _set_scan_progress(job_id, status="done", found=0, duplicates=0, current_client="Nessuna fonte attiva"); return
        clients = supabase.table("clients").select("id, name, keywords_web").execute().data or []
        from services.monitor import fetch_rss, fetch_scrape
        all_records, errors = [], []
        for i, source in enumerate(sources):
            _set_scan_progress(job_id, current=i+1, current_client=source["name"])
            try:
                records = fetch_scrape(source, clients) if source.get("type") == "scrape" else fetch_rss(source, clients)
                all_records.extend(records)
            except Exception as e:
                errors.append(f"{source['name']}: {str(e)}")
            _time.sleep(0.1)
        seen, deduped = set(), []
        for r in all_records:
            if r["content_hash"] not in seen: seen.add(r["content_hash"]); deduped.append(r)
        inserted = 0
        if deduped:
            result = supabase.table("web_mentions").upsert(deduped, on_conflict="content_hash").execute()
            inserted = len(result.data) if result.data else 0
        _set_scan_progress(job_id, status="done", found=inserted, duplicates=len(deduped)-inserted, errors=errors, current_client="")
    except Exception as e:
        _set_scan_progress(job_id, status="error", error=str(e))

@app.post("/api/web-scan/start")
async def web_scan_start():
    job_id = str(uuid.uuid4())[:8]
    threading.Thread(target=_run_web_scan, args=(job_id,), daemon=True).start()
    return {"job_id": job_id}

@app.get("/api/web-scan/status/{job_id}")
async def web_scan_status(job_id: str):
    data = _get_scan_progress(job_id)
    if not data: raise HTTPException(status_code=404, detail="Job non trovato")
    return data

@app.post("/api/monitor/run")
async def monitor_run():
    try:
        if run_monitoring is None: return {"error": "Monitor non disponibile"}
        return run_monitoring()
    except Exception as e: return {"error": str(e), "found": 0, "duplicates": 0}

@app.post("/api/monitor/run-historical")
async def monitor_run_historical(req: HistoricalScanRequest):
    try:
        if run_monitoring is None: return {"error": "Monitor non disponibile"}
        return run_monitoring(from_date=req.from_date, to_date=req.to_date)
    except Exception as e: return {"error": str(e), "found": 0, "duplicates": 0}

@app.get("/api/monitor/scan-info")
async def monitor_scan_info():
    try:
        res = supabase.table("monitor_meta").select("key, value").in_("key", ["last_daily_scan", "last_historical_scan"]).execute()
        info = {row["key"]: row["value"] for row in (res.data or [])}
        return {"last_daily": info.get("last_daily_scan"), "last_historical": info.get("last_historical_scan")}
    except Exception: return {"last_daily": None, "last_historical": None}

@app.get("/api/monitor-meta")
async def get_monitor_meta():
    try: return {"meta": supabase.table("monitor_meta").select("*").execute().data or []}
    except Exception as e: return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════
# PITCH
# ══════════════════════════════════════════════════════════════════

@app.post("/api/pitch")
async def pitch_endpoint(message: str = Form(...), client_id: str = Form(""), history: str = Form("[]")):
    try: hist = json.loads(history) if history else []
    except Exception: hist = []
    try:
        result = pitch_advisor(message=message, client_id=client_id, history=hist)
        return {"success": True, **result}
    except Exception as e: return {"success": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════
# DAILY DIGEST
# ══════════════════════════════════════════════════════════════════

def _generate_audio_bytes(text: str) -> bytes | None:
    """Genera MP3 dal testo del digest via OpenAI TTS. Ritorna bytes o None se fallisce."""
    try:
        from openai import OpenAI
        api_key = os.getenv("OPENAI_API_KEY", "")
        if not api_key: return None
        ai = OpenAI(api_key=api_key)
        clean = text.replace("*","").replace("_","").replace("————————————————————",". ")
        # Limita a 4000 caratteri per non superare il limite TTS
        if len(clean) > 4000:
            clean = clean[:4000] + "... Fine del digest."
        response = ai.audio.speech.create(model="tts-1", voice="shimmer", input=clean, response_format="mp3")
        return response.content
    except Exception as e:
        print(f"[AUDIO] Errore generazione TTS: {e}")
        return None


def _send_digest_email(text, today_str, to_override=None):
    """
    Invia il digest via email con audio MP3 allegato.
    to_override: lista email opzionale — se None usa digest_recipients da Supabase.
    """
    try:
        import resend
        import base64
    except ImportError:
        print("[EMAIL] resend non installato"); return
    api_key = os.getenv("RESEND_API_KEY", "")
    if not api_key: print("[EMAIL] RESEND_API_KEY non configurata"); return
    resend.api_key = api_key

    if to_override:
        to_list = to_override
    else:
        try:
            recipients = supabase.table("digest_recipients").select("email, name").eq("active", True).execute().data or []
        except Exception as e:
            print(f"[EMAIL] Errore lettura destinatari: {e}"); return
        if not recipients: print("[EMAIL] Nessun destinatario"); return
        to_list = [r["email"] for r in recipients if r.get("email")]

    if not to_list: return

    # Genera audio
    audio_bytes = _generate_audio_bytes(text)
    filename = f"MAIM_Digest_{today_str.replace('/', '-')}.mp3"

    try:
        payload = {
            "from":    "MAIM Digest <digest@maim.it>",
            "to":      to_list,
            "subject": f"MAIM DIGEST — {today_str}",
            "text":    text,
        }
        if audio_bytes:
            payload["attachments"] = [{
                "filename": filename,
                "content":  list(audio_bytes),
            }]
            print(f"[EMAIL] Audio allegato: {len(audio_bytes)} bytes")
        else:
            print("[EMAIL] Audio non generato — invio solo testo")

        resend.Emails.send(payload)
        print(f"[EMAIL] Inviato a {len(to_list)} destinatari")
    except Exception as e:
        print(f"[EMAIL] Errore invio: {e}")

def _run_digest_job(job_id):
    import traceback
    try:
        today = date.today().isoformat()
        today_str = date.today().strftime("%d/%m/%Y")
        articles_today = supabase.table("articles").select("id, testata, data, giornalista, titolo, occhiello, testo_completo, tone, ave, tipologia_articolo").eq("data", today).order("ave", desc=True).execute().data or []
        clients = supabase.table("clients").select("id, name, keywords_web").execute().data or []
        result = generate_digest(articles_today=articles_today, clients=clients)
        if "error" in result and not result.get("text"):
            _set_job(job_id, "error", error=result["error"])
        else:
            _set_job(job_id, "done", result=result)
            digest_text = result.get("text", "")
            if digest_text:
                _send_digest_email(digest_text, today_str)
                try:
                    supabase.table("digests").upsert({"data": today, "text": digest_text,
                        "articles_today": result.get("articles_today", 0), "client_mentions": result.get("client_mentions", 0)}, on_conflict="data").execute()
                except Exception as e: print(f"[DIGEST] Errore salvataggio: {e}")
    except Exception as e:
        tb = traceback.format_exc()
        _set_job(job_id, "error", error=str(e) + " | " + tb.splitlines()[-1])

@app.post("/api/digest-send-email")
async def send_digest_email_manual():
    """Invia manualmente il digest del giorno via email con audio."""
    today = date.today().isoformat()
    today_str = date.today().strftime("%d/%m/%Y")
    try:
        res = supabase.table("digests").select("text").eq("data", today).execute()
        if not res.data or not res.data[0].get("text"):
            return {"success": False, "error": "Nessun digest disponibile per oggi. Generalo prima."}
        digest_text = res.data[0]["text"]
        threading.Thread(
            target=_send_digest_email,
            args=(digest_text, today_str),
            daemon=True
        ).start()
        return {"success": True, "message": "Invio in corso…"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/daily-digest")
async def daily_digest_endpoint():
    _cleanup_old_jobs()
    job_id = str(uuid.uuid4())[:12]
    _set_job(job_id, "pending")
    threading.Thread(target=_run_digest_job, args=(job_id,), daemon=True).start()
    return {"success": True, "job_id": job_id}

@app.get("/api/digest/status")
async def digest_status():
    today = date.today().isoformat()
    try:
        res = supabase.table("digests").select("*").eq("data", today).execute()
        if res.data:
            row = res.data[0]
            return {"status": "ready", "text": row.get("text",""), "articles_today": row.get("articles_today",0),
                    "client_mentions": row.get("client_mentions",0), "created_at": row.get("created_at")}
    except Exception as e: print(f"[DIGEST STATUS] {e}")
    for jid, j in _JOBS.items():
        if j.get("status") in ("pending","running"): return {"status": "generating", "job_id": jid}
    return {"status": "idle"}

@app.get("/api/digest/dates")
async def digest_dates():
    try:
        res = supabase.table("digests").select("data, articles_today, client_mentions, created_at").order("data", desc=True).limit(30).execute()
        return {"dates": res.data or []}
    except Exception as e: return {"dates": [], "error": str(e)}

@app.get("/api/digest/{data_str}")
async def get_digest_by_date(data_str: str):
    try:
        res = supabase.table("digests").select("*").eq("data", data_str).execute()
        if not res.data: raise HTTPException(status_code=404, detail="Digest non trovato")
        return res.data[0]
    except HTTPException: raise
    except Exception as e: return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════
# DIGEST AUDIO
# ══════════════════════════════════════════════════════════════════

@app.post("/api/digest-audio")
async def digest_audio(req: DigestAudioRequest):
    from openai import OpenAI
    if not req.text or not req.text.strip(): raise HTTPException(status_code=400, detail="Testo vuoto")
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key: raise HTTPException(status_code=500, detail="OPENAI_API_KEY non configurata")
    try:
        ai = OpenAI(api_key=api_key)
        clean = req.text.replace("*","").replace("_","").replace("————————————————————",". ")
        # OpenAI TTS limite: 4096 caratteri
        if len(clean) > 4096:
            clean = clean[:4096] + "... Fine del digest."
        response = ai.audio.speech.create(model="tts-1", voice="shimmer", input=clean, response_format="mp3")
        return StreamingResponse(iter([response.content]), media_type="audio/mpeg",
                                 headers={"Content-Disposition": "inline; filename=digest.mp3"})
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
# WEB DIGEST
# ══════════════════════════════════════════════════════════════════

@app.post("/api/web-digest/generate")
async def generate_web_digest():
    from openai import OpenAI
    import secrets
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key: raise HTTPException(status_code=500, detail="OPENAI_API_KEY non configurata")
    ai = OpenAI(api_key=api_key)
    today = date.today()
    now_utc = datetime.now(timezone.utc).isoformat()
    start_utc = datetime(today.year, today.month, today.day, 6, 0, 0, tzinfo=timezone.utc).isoformat()
    try:
        mentions = supabase.table("web_mentions").select("id, source_name, title, url, summary, matched_client, tone, published_at, created_at").eq("published_at", today.isoformat()).gte("created_at", start_utc).lte("created_at", now_utc).order("created_at", desc=False).execute().data or []
    except Exception as e: raise HTTPException(status_code=500, detail=f"Errore lettura: {e}")
    if not mentions: raise HTTPException(status_code=404, detail="Nessuna mention trovata per oggi")
    titoli_block = "\n".join(f"[{m.get('source_name','')}] {m.get('title','')}" for m in mentions)
    giorni = ["Lunedì","Martedì","Mercoledì","Giovedì","Venerdì","Sabato","Domenica"]
    mesi = ["","gennaio","febbraio","marzo","aprile","maggio","giugno","luglio","agosto","settembre","ottobre","novembre","dicembre"]
    data_ext = f"{giorni[today.weekday()]} {today.day} {mesi[today.month]} {today.year}"
    try:
        resp = ai.chat.completions.create(model="gpt-4o-mini",
            messages=[{"role":"system","content":"Sei MAIM Intelligence. Produci contenuto conciso e professionale."},
                      {"role":"user","content":f"Data: {data_ext} | Totale: {len(mentions)}\nTITOLI:\n{titoli_block}\n\nProduci:\n▶️ *TEMI PRINCIPALI*\n[Max 5 temi, 2 righe]\n\n▶️ *DA TENERE D'OCCHIO*\n[1-2 segnali deboli]"}],
            temperature=0.1, max_tokens=900)
        themes_text = resp.choices[0].message.content.strip()
    except Exception as e: themes_text = f"Errore: {e}"
    clients_map = {}
    for m in mentions:
        for cl in [c.strip() for c in (m.get("matched_client") or "").split(",") if c.strip()]:
            clients_map.setdefault(cl, []).append(m)
    wa_lines = [f"*MAIM WEB DIGEST*\n{data_ext}\n", themes_text]
    for cl, arts in clients_map.items():
        wa_lines.append(f"\n🟧 *{cl.upper()}* — {len(arts)} articoli\n")
        for a in arts:
            wa_lines.append(f"*{(a.get('source_name') or '').upper()}*\n_{(a.get('title') or '').strip()}_\n→ {(a.get('summary') or '').strip()}\n")
    token = secrets.token_hex(8)
    try:
        supabase.table("web_digests").insert({"token": token, "data": today.isoformat(),
            "themes": themes_text, "mentions": clients_map, "text_wa": "\n".join(wa_lines)}).execute()
    except Exception as e: raise HTTPException(status_code=500, detail=f"Errore salvataggio: {e}")
    return {"token": token, "mentions": len(mentions), "clients": list(clients_map.keys())}

@app.get("/api/web-digest/{token}")
async def get_web_digest(token: str):
    try:
        res = supabase.table("web_digests").select("*").eq("token", token).execute()
        if not res.data: raise HTTPException(status_code=404, detail="Digest non trovato")
        return res.data[0]
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
# TESTATE TIER
# ══════════════════════════════════════════════════════════════════

@app.get("/api/testate-tier")
async def get_testate_tier():
    try: return supabase.table("testate_tier").select("testata, tier, ordine").order("tier").execute().data or []
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/testate-tier/save")
async def save_testate_tier(req: TestateUpdateRequest):
    try:
        rows = [{"testata": t.testata, "tier": t.tier, "ordine": t.ordine} for t in req.testate]
        supabase.table("testate_tier").upsert(rows, on_conflict="testata").execute()
        return {"success": True, "saved": len(rows)}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
# GMAIL IMAP
# ══════════════════════════════════════════════════════════════════

_gmail_state = {"status": "idle", "last_check": None, "last_import": None,
                "found": 0, "imported": 0, "errors": [], "log": []}

def _gmail_log(msg):
    print(f"[GMAIL] {msg}")
    _gmail_state["log"] = ([msg] + _gmail_state["log"])[:50]

def _run_gmail_import(auto=False):
    import re, requests
    from bs4 import BeautifulSoup
    gmail_user = os.getenv("GMAIL_USER", "")
    gmail_pass = os.getenv("GMAIL_APP_PASSWORD", "")
    if not gmail_user or not gmail_pass:
        _gmail_state["status"] = "error"; _gmail_log("Credenziali non configurate"); return
    _gmail_state["status"] = "running"
    _gmail_state["last_check"] = datetime.now().isoformat()
    _gmail_state["errors"] = []
    _gmail_log(f"Connessione a Gmail ({gmail_user})…")
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(gmail_user, gmail_pass)
        mail.select("INBOX")
        since_date = (datetime.now() - timedelta(hours=24)).strftime("%d-%b-%Y")
        mail_ids_all = set()
        for sender in ["Ufficio.Stampa@snam.it", "stampa@maimgroup.com"]:
            _, data = mail.search(None, f'(FROM "{sender}" SINCE "{since_date}")')
            for mid in data[0].split(): mail_ids_all.add(mid)
        mail_ids = list(mail_ids_all)
        _gmail_log(f"Mail trovate (ultime 24h): {len(mail_ids)}")
        if not mail_ids:
            _gmail_state.update({"status": "idle", "found": 0, "imported": 0}); mail.logout(); return
        total_imported = total_found = 0
        for mid in mail_ids:
            _, msg_data = mail.fetch(mid, "(RFC822)")
            msg = _email_lib.message_from_bytes(msg_data[0][1])
            message_id = msg.get("Message-ID", "").strip()
            if message_id:
                mid_hash = f"mid:{_hashlib.md5(message_id.encode()).hexdigest()}"
                if supabase.table("articles").select("id").eq("content_hash_mail", mid_hash).limit(1).execute().data:
                    _gmail_log("Mail già processata — salto"); continue
            subj = ""
            for part, enc in _decode_header(msg.get("Subject", "")):
                subj += part.decode(enc or "utf-8", errors="ignore") if isinstance(part, bytes) else part
            _gmail_log(f"Processo: {subj[:60]}")
            html_body = ""
            for part in msg.walk():
                if part.get_content_type() == "text/html":
                    html_body = part.get_payload(decode=True).decode("utf-8", errors="ignore"); break
            if not html_body: _gmail_log("Nessuna parte HTML — salto"); continue
            articoli = _parse_rassegna_html(html_body)
            total_found += len(articoli)
            _gmail_log(f"Articoli estratti: {len(articoli)}")
            for art in articoli:
                try:
                    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                               "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "it-IT,it;q=0.9", "Connection": "keep-alive"}
                    session = requests.Session()
                    session.get("https://rassegna.snam.it", headers=headers, timeout=10)
                    resp = session.get(art["url"], headers=headers, timeout=15)
                    body = resp.text.strip() if resp.ok else ""
                    testo = "" if ("Checking your browser" in body or "octofence" in body.lower() or len(body) < 100) else body
                    if not testo: _gmail_log(f"Octofence su {art['testata']}")
                except Exception as e:
                    testo = ""; _gmail_state["errors"].append(f"{art['testata']}: {e}")
                h = _hashlib.md5(f"{art['testata']}|{art['titolo']}|{art['data']}".encode()).hexdigest()
                if supabase.table("articles").select("id").eq("content_hash_mail", h).execute().data: continue
                try:
                    supabase.table("articles").insert({"testata": art["testata"], "titolo": art["titolo"],
                        "data": art["data"], "testo_completo": testo, "giornalista": "",
                        "content_hash": h, "content_hash_mail": h, "fonte": "gmail_rassegna"}).execute()
                    total_imported += 1
                except Exception as ins_err: _gmail_log(f"Errore insert {art['testata']}: {ins_err}")
            if message_id:
                try:
                    supabase.table("articles").insert({"testata": "_gmail_processed_", "titolo": f"Processata: {subj[:100]}",
                        "data": date.today().isoformat(), "content_hash": mid_hash, "content_hash_mail": mid_hash, "fonte": "gmail_sentinel"}).execute()
                except Exception: pass
        _gmail_state.update({"found": total_found, "imported": total_imported,
                              "last_import": datetime.now().isoformat(), "status": "idle"})
        _gmail_log(f"Completato: {total_imported}/{total_found} articoli importati")
        mail.logout()
    except Exception as e:
        _gmail_state["status"] = "error"; _gmail_state["errors"].append(str(e)); _gmail_log(f"ERRORE: {e}")


def _parse_rassegna_html(html):
    import re
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    articoli, seen_urls = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"].replace("&amp;", "&")
        if "tiplink=4" not in href or re.search(r"imgatt=:", href) or href in seen_urls: continue
        seen_urls.add(href)
        tr_curr = a.find_parent("tr")
        if not tr_curr: continue
        titolo = re.sub(r"\[.*?\]", "", tr_curr.get_text(separator=" ", strip=True)).strip()
        if not titolo: continue
        tr_prev = tr_curr.find_previous_sibling("tr")
        testata_data = tr_prev.get_text(strip=True) if tr_prev else ""
        m = re.match(r"^(.+?)\s*[·]\s*(\d{2}-\d{2}-\d{4})", testata_data)
        if m:
            testata = re.sub(r"\s+pag\.\S+.*$", "", m.group(1).strip(), flags=re.IGNORECASE)
            d, mo, y = m.group(2).split("-")
            data = f"{y}-{mo}-{d}"
        else:
            testata = testata_data; data = date.today().isoformat()
        articoli.append({"testata": testata, "titolo": titolo, "data": data, "url": href})
    return articoli


@app.post("/api/gmail/import")
async def gmail_import_manual():
    if _gmail_state["status"] == "running": return {"status": "already_running"}
    threading.Thread(target=_run_gmail_import, args=(False,), daemon=True).start()
    return {"status": "started"}

@app.get("/api/gmail/status")
async def gmail_status(): return _gmail_state


# ══════════════════════════════════════════════════════════════════
# AVVIO
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))