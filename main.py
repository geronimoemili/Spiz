import os
import shutil
import uvicorn
import json
import uuid
import time
import threading
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

run_monitoring = None
try:
    from services.monitor import run_monitoring
    from apscheduler.schedulers.background import BackgroundScheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_monitoring, 'cron', hour=6, minute=0)

    # ── Gmail IMAP scheduler: ogni 15 min dalle 07:00 alle 11:00 ──
    def _scheduled_gmail_check():
        from datetime import datetime
        now = datetime.now()
        if 7 <= now.hour < 11:
            print(f"[GMAIL] Controllo automatico ore {now.strftime('%H:%M')}")
            _run_gmail_import(auto=True)

    scheduler.add_job(_scheduled_gmail_check, 'interval', minutes=15)
    scheduler.start()
    print("✅ Scheduler avviato (monitor 06:00 + Gmail ogni 15 min)")
except Exception as e:
    print(f"⚠️ Scheduler non avviato: {e}")

app = FastAPI(title="MAIM Intelligence")
app.mount("/static", StaticFiles(directory="web"), name="static")

os.makedirs("data/raw", exist_ok=True)
os.makedirs("web", exist_ok=True)

# ── DOCX STORE ────────────────────────────────────────────────────────
_DOCX_STORE: dict = {}

def _store_docx(path: str) -> str | None:
    if not path or not os.path.exists(path):
        return None
    token = str(uuid.uuid4())
    _DOCX_STORE[token] = {"path": path, "expires": time.time() + 3600}
    return token

def _cleanup_expired_docx():
    now = time.time()
    expired = [k for k, v in _DOCX_STORE.items() if now > v["expires"]]
    for k in expired:
        try:
            p = _DOCX_STORE[k]["path"]
            if os.path.exists(p): os.remove(p)
        except Exception: pass
        del _DOCX_STORE[k]


# ── JOB QUEUE (in-memory) ─────────────────────────────────────────────
_JOBS: dict = {}
_JOBS_LOCK = threading.Lock()

def _set_job(job_id: str, status: str, result: dict = None, error: str = None):
    with _JOBS_LOCK:
        _JOBS[job_id] = {
            "status":  status,
            "result":  result,
            "error":   error,
            "created": time.time(),
        }

def _get_job(job_id: str) -> dict | None:
    with _JOBS_LOCK:
        return _JOBS.get(job_id)

def _cleanup_old_jobs():
    cutoff = time.time() - 1800
    with _JOBS_LOCK:
        old = [k for k, v in _JOBS.items() if v["created"] < cutoff]
        for k in old:
            del _JOBS[k]


# ── MODELLI ────────────────────────────────────────────────────────────
class ChatRequest(BaseModel):
    message:     Optional[str]  = ""
    context:     Optional[str]  = "week"
    history:     Optional[list] = []
    client_name: Optional[str]  = ""
    topic_name:  Optional[str]  = ""

class GenerateReportRequest(BaseModel):
    client_name:  Optional[str]       = ""
    topic_name:   Optional[str]       = ""
    article_ids:  Optional[List[str]] = []
    report_type:  Optional[str]       = "posizionamento_giornalisti"
    refinement:   Optional[str]       = ""

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

class ClientModel(BaseModel):
    name:             Optional[str] = None
    keywords:         Optional[str] = None
    keywords_web:     Optional[str] = None
    sector:           Optional[str] = None
    description:      Optional[str] = None
    website:          Optional[str] = None
    contact:          Optional[str] = None
    semantic_topic:   Optional[str] = None
    macro_strategici: Optional[str] = None

class SourceModel(BaseModel):
    name:   str
    url:    str
    type:   Optional[str]  = "rss"
    active: Optional[bool] = True

class HistoricalScanRequest(BaseModel):
    from_date: str
    to_date:   str

class ShareRequest(BaseModel):
    article_ids: List[str]


# ══════════════════════════════════════════════════════════════════════
# NAVIGAZIONE
# ══════════════════════════════════════════════════════════════════════

@app.get("/")
async def root():
    return FileResponse("web/home.html")

@app.get("/home")
async def home_page():
    return FileResponse("web/home.html")

@app.get("/press")
async def press_page():
    return FileResponse("web/press.html")

@app.get("/dashboard")
async def dashboard_page():
    return FileResponse("web/press.html")

@app.get("/web")
async def web_page():
    return FileResponse("web/web.html")

@app.get("/monitor")
async def monitor_page():
    return FileResponse("web/web.html")

@app.get("/chat")
async def chat_page():
    return FileResponse("web/chat.html")

@app.get("/clients")
async def clients_page():
    return FileResponse("web/clienti.html")

@app.get("/pitch")
async def pitch_page():
    return FileResponse("web/pitch.html")

@app.get("/web-digest")
async def web_digest_page():
    return FileResponse("web/web_digest.html")

@app.get("/webdigest")
async def webdigest_admin_page():
    return FileResponse("web/webdigest.html")

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.get("/healthcheck")
async def healthcheck():
    return {"status": "ok"}


# ══════════════════════════════════════════════════════════════════════
# UPLOAD CSV
# ══════════════════════════════════════════════════════════════════════

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

    # Trigger digest automatico se almeno un file caricato correttamente
    digest_job_id = None
    if any(r["status"] == "success" for r in results):
        _cleanup_old_jobs()
        digest_job_id = str(uuid.uuid4())[:12]
        _set_job(digest_job_id, "pending")
        t = threading.Thread(target=_run_digest_job, args=(digest_job_id,), daemon=True)
        t.start()
        print(f"[DIGEST] Avviato automaticamente dopo upload (job {digest_job_id})")

    return {"results": results, "digest_job_id": digest_job_id}


# ══════════════════════════════════════════════════════════════════════
# AI REPORT — JOB ASINCRONO
# ══════════════════════════════════════════════════════════════════════

def _run_report_job(job_id: str, client_name: str, topic_name: str, articles: list,
                    report_type: str = "posizionamento_giornalisti", refinement: str = ""):
    import traceback
    try:
        try:
            from api.chat import ask_spiz as _ask
        except ImportError:
            from chat import ask_spiz as _ask

        result = _ask(
            client_name=client_name,
            topic_name=topic_name,
            preloaded_articles=articles,
            report_type=report_type,
            refinement=refinement,
        )
        if "error" in result:
            _set_job(job_id, "error", error=result["error"])
        else:
            _set_job(job_id, "done", result=result)
    except Exception as e:
        _set_job(job_id, "error", error=str(e) + " | " + traceback.format_exc().splitlines()[-1])


@app.post("/api/generate-report")
async def generate_report_endpoint(req: GenerateReportRequest):
    _cleanup_old_jobs()

    if not req.article_ids:
        return {"success": False, "error": "Nessun articolo selezionato."}

    DB_COLS = (
        "id, testata, data, giornalista, occhiello, titolo, sottotitolo, "
        "testo_completo, macrosettori, tipologia_articolo, tone, "
        "dominant_topic, reputational_risk, political_risk, ave, tipo_fonte"
    )
    try:
        res = supabase.table("articles").select(DB_COLS).in_("id", req.article_ids).execute()
        articles = res.data or []
    except Exception as e:
        return {"success": False, "error": f"Errore DB: {e}"}

    if not articles:
        return {"success": False, "error": "Articoli non trovati nel database."}

    job_id = str(uuid.uuid4())[:12]
    _set_job(job_id, "pending")

    t = threading.Thread(
        target=_run_report_job,
        args=(job_id, req.client_name or "", req.topic_name or "", articles,
              req.report_type or "posizionamento_giornalisti", req.refinement or ""),
        daemon=True,
    )
    t.start()

    return {"success": True, "job_id": job_id}


@app.get("/api/job/{job_id}")
async def get_job_status(job_id: str):
    job = _get_job(job_id)
    if not job:
        return {"status": "error", "error": "Job non trovato o scaduto."}

    if job["status"] == "pending":
        return {"status": "pending"}

    if job["status"] == "error":
        return {"status": "error", "error": job["error"]}

    # done — restituisce campi sia report che digest
    result = job["result"] or {}
    return {
        "status":          "done",
        "response":        result.get("response", ""),
        "articles_used":   result.get("articles_used", 0),
        "period_from":     result.get("period_from", ""),
        "period_to":       result.get("period_to", ""),
        "text":            result.get("text", ""),
        "articles_today":  result.get("articles_today", 0),
        "client_mentions": result.get("client_mentions", 0),
    }


@app.post("/api/chat")
async def chat_endpoint(req: ChatRequest):
    try:
        result = ask_spiz(
            message=req.message or "",
            history=req.history or [],
            context=req.context or "week",
            client_name=req.client_name or "",
            topic_name=req.topic_name or "",
        )
    except Exception as e:
        return {"success": False, "error": str(e)}

    if "error" in result:
        return {"success": False, "error": result["error"]}

    return {
        "success":       True,
        "response":      result.get("response", ""),
        "is_report":     result.get("is_report", False),
        "articles_used": result.get("articles_used", 0),
        "period_from":   result.get("period_from", ""),
        "period_to":     result.get("period_to", ""),
        "articles_list": result.get("articles_list", []),
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
    return FileResponse(
        path=path,
        filename=os.path.basename(path),
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


# ══════════════════════════════════════════════════════════════════════
# DASHBOARD STATS
# ══════════════════════════════════════════════════════════════════════

@app.get("/api/dashboard-stats")
async def dashboard_stats():
    try:
        today     = date.today().isoformat()
        week_ago  = (date.today() - timedelta(days=7)).isoformat()
        month_ago = (date.today() - timedelta(days=30)).isoformat()
        total     = supabase.table("articles").select("id", count="exact").execute()
        oggi      = supabase.table("articles").select("id", count="exact").eq("data", today).execute()
        settimana = supabase.table("articles").select("id", count="exact").gte("data", week_ago).execute()
        mese      = supabase.table("articles").select("id", count="exact").gte("data", month_ago).execute()
        return {"totale": total.count or 0, "oggi": oggi.count or 0,
                "settimana": settimana.count or 0, "mese": mese.count or 0}
    except Exception as e:
        return {"totale": 0, "oggi": 0, "settimana": 0, "mese": 0, "error": str(e)}


@app.get("/api/last-upload")
async def last_upload():
    try:
        res = supabase.table("articles").select("data, testata").order("data", desc=True).limit(1).execute()
        if res.data:
            return {"data": res.data[0].get("data"), "testata": res.data[0].get("testata")}
        return {"data": None, "testata": None}
    except Exception as e:
        return {"data": None, "error": str(e)}


@app.get("/api/today-stats")
async def today_stats():
    try:
        today    = date.today().isoformat()
        res      = supabase.table("articles").select("testata, tone, macrosettori, giornalista").eq("data", today).execute()
        articles = res.data or []
        testate_counter     = Counter(a.get("testata","") for a in articles if a.get("testata"))
        giornalisti_counter = Counter(
            a.get("giornalista","") for a in articles
            if a.get("giornalista") and a["giornalista"].lower() not in ("redazione","n.d.","n/d","")
        )
        tones    = Counter(a.get("tone","") for a in articles if a.get("tone"))
        tone_tot = sum(tones.values()) or 1
        return {
            "total_today": len(articles), "totale": len(articles),
            "testate":     [{"name": k, "count": v} for k,v in testate_counter.most_common(10)],
            "giornalisti": [{"nome": k, "articoli": v} for k,v in giornalisti_counter.most_common(20)],
            "sentiment":   {k: round(v/tone_tot*100) for k,v in tones.items() if k},
        }
    except Exception as e:
        return {"total_today": 0, "totale": 0, "testate": [], "giornalisti": [], "sentiment": {}, "error": str(e)}


@app.get("/api/today-mentions")
async def today_mentions():
    try:
        today       = date.today().isoformat()
        clients_res = supabase.table("clients").select("*").execute()
        clients     = clients_res.data or []
        arts_res    = supabase.table("articles").select(
            "id, titolo, testata, giornalista, tone, dominant_topic, testo_completo, occhiello"
        ).eq("data", today).execute()
        articles = arts_res.data or []
        result = []
        for cl in clients:
            raw_keywords = cl.get("keywords_press") or cl.get("keywords") or ""
            keywords = [k.strip().lower() for k in raw_keywords.split(",") if k.strip()]
            count = 0
            if keywords:
                count = sum(
                    1 for a in articles
                    if any(
                        kw in (a.get("testo_completo") or "").lower() or
                        kw in (a.get("titolo") or "").lower() or
                        kw in (a.get("occhiello") or "").lower()
                        for kw in keywords
                    )
                )
            result.append({"id": cl["id"], "name": cl.get("name",""), "keywords": raw_keywords, "today": count})
        return result
    except Exception as e:
        return []


@app.get("/api/period-mentions")
async def period_mentions(from_date: Optional[str] = None, to_date: Optional[str] = None):
    try:
        if not from_date: from_date = date.today().isoformat()
        if not to_date:   to_date   = date.today().isoformat()
        clients_res = supabase.table("clients").select("*").execute()
        clients     = clients_res.data or []
        arts_res    = supabase.table("articles").select(
            "id, titolo, testo_completo, occhiello"
        ).gte("data", from_date).lte("data", to_date).execute()
        articles = arts_res.data or []
        result = []
        for cl in clients:
            raw_keywords = cl.get("keywords_press") or cl.get("keywords") or ""
            keywords = [k.strip().lower() for k in raw_keywords.split(",") if k.strip()]
            count = 0
            if keywords:
                count = sum(
                    1 for a in articles
                    if any(
                        kw in (a.get("testo_completo") or "").lower() or
                        kw in (a.get("titolo") or "").lower() or
                        kw in (a.get("occhiello") or "").lower()
                        for kw in keywords
                    )
                )
            result.append({"id": cl["id"], "name": cl.get("name",""), "keywords": raw_keywords, "count": count})
        return result
    except Exception as e:
        return []


@app.get("/api/macro-groups-count")
async def macro_groups_count(from_date: Optional[str] = None, to_date: Optional[str] = None):
    try:
        if not from_date: from_date = date.today().isoformat()
        if not to_date:   to_date   = date.today().isoformat()
        groups_res = supabase.table("macro_groups").select("id, name").eq("active", True).order("name").execute()
        groups = groups_res.data or []
        if not groups:
            return {"groups": []}
        all_links = supabase.table("macro_group_links").select("macro_group_id, official_macro_id").execute()
        links_by_group: dict = {}
        for lnk in (all_links.data or []):
            gid = lnk["macro_group_id"]
            links_by_group.setdefault(gid, []).append(lnk["official_macro_id"])
        all_official_ids = list({oid for ids in links_by_group.values() for oid in ids})
        macro_names_map: dict = {}
        if all_official_ids:
            macros_res = supabase.table("official_macrosectors").select("id, name").in_("id", all_official_ids).execute()
            for m in (macros_res.data or []):
                macro_names_map[m["id"]] = m["name"]
        arts_res = supabase.table("articles").select("id, macrosettori")\
            .gte("data", from_date).lte("data", to_date).execute()
        articles = arts_res.data or []
        result = []
        for g in groups:
            gid = g["id"]
            official_ids = links_by_group.get(gid, [])
            names_set = {macro_names_map[oid] for oid in official_ids if oid in macro_names_map}
            count = 0
            if names_set:
                count = sum(
                    1 for a in articles
                    if a.get("macrosettori") and any(
                        m.strip() in names_set for m in a["macrosettori"].split(",")
                    )
                )
            result.append({"id": gid, "name": g["name"], "count": count})
        return {"groups": result}
    except Exception as e:
        return {"groups": [], "error": str(e)}


# ══════════════════════════════════════════════════════════════════════
# GIORNALISTI CRM
# ══════════════════════════════════════════════════════════════════════

@app.get("/giornalisti")
async def giornalisti_page():
    return FileResponse("web/giornalisti.html")


class JournalistModel(BaseModel):
    nome:               str
    testata_principale: Optional[str] = None
    tipo_testata:       Optional[str] = None
    email:              Optional[str] = None
    cellulare:          Optional[str] = None
    note:               Optional[str] = None
    clienti_associati:  Optional[str] = None

class JournalistUpdate(BaseModel):
    nome:               Optional[str] = None
    testata_principale: Optional[str] = None
    tipo_testata:       Optional[str] = None
    email:              Optional[str] = None
    cellulare:          Optional[str] = None
    note:               Optional[str] = None
    clienti_associati:  Optional[str] = None


@app.get("/api/journalists/list")
async def list_journalists(
    client_id:    Optional[str] = None,
    tipo_testata: Optional[str] = None,
    macro_id:     Optional[str] = None,
    q:            Optional[str] = None,
):
    """Lista giornalisti dal CRM + archivio con filtri corretti."""
    try:
        from collections import Counter as _Counter
        SKIP = {"", "n.d.", "n/d", "redazione", "autore non indicato"}

        # 1. CRM
        crm_res = supabase.table("journalists").select("*").order("nome").execute()
        crm = {j["nome"].strip().lower(): j for j in (crm_res.data or [])}

        # 2. Articoli — carico tutti i campi necessari
        arts_res = supabase.table("articles").select(
            "giornalista, testata, macrosettori, titolo, occhiello, testo_completo"
        ).execute()
        articles = arts_res.data or []

        # Indici per giornalista
        art_count:   dict = {}
        art_testate: dict = {}
        art_macro_str: dict = {}  # giornalista_lower → set di stringhe macrosettori

        for a in articles:
            g = (a.get("giornalista") or "").strip()
            if not g or g.lower() in SKIP:
                continue
            gl = g.lower()
            art_count[gl] = art_count.get(gl, 0) + 1
            art_testate.setdefault(gl, _Counter())[a.get("testata","") or ""] += 1
            macro_raw = (a.get("macrosettori") or "").upper()
            if macro_raw:
                art_macro_str.setdefault(gl, set()).add(macro_raw)

        # 3. Filtro cliente — cerca keyword negli articoli
        client_journalists: set | None = None
        if client_id:
            cl_res = supabase.table("clients").select("keywords_press, keywords").eq("id", client_id).execute()
            if cl_res.data:
                cl = cl_res.data[0]
                kws = [k.strip().lower() for k in (cl.get("keywords_press") or cl.get("keywords") or "").split(",") if k.strip()]
                client_journalists = set()
                if kws:
                    for a in articles:
                        g = (a.get("giornalista") or "").strip()
                        if not g or g.lower() in SKIP:
                            continue
                        txt = f"{a.get('titolo','')} {a.get('occhiello','')} {a.get('testo_completo','')}".lower()
                        if any(kw in txt for kw in kws):
                            client_journalists.add(g.lower())

        # 4. Filtro macro — usa ILIKE: cerca se il nome del macrosettore è CONTENUTO nella stringa
        macro_journalists: set | None = None
        if macro_id:
            lnk_res = supabase.table("macro_group_links").select("official_macro_id").eq("macro_group_id", macro_id).execute()
            oids = [l["official_macro_id"] for l in (lnk_res.data or [])]
            if oids:
                mac_res = supabase.table("official_macrosectors").select("name").in_("id", oids).execute()
                mac_names = [m["name"].upper() for m in (mac_res.data or [])]
                macro_journalists = set()
                for gl, macro_set in art_macro_str.items():
                    for macro_str in macro_set:
                        if any(mn in macro_str for mn in mac_names):
                            macro_journalists.add(gl)
                            break

        # 5. Unisci e filtra
        all_names = set(crm.keys()) | set(art_count.keys())
        result = []
        for gl in all_names:
            if client_journalists is not None and gl not in client_journalists:
                continue
            if macro_journalists is not None and gl not in macro_journalists:
                continue
            if q and q.lower() not in gl:
                continue

            crm_entry = crm.get(gl, {})
            testata_arch = art_testate[gl].most_common(1)[0][0] if gl in art_testate else ""
            tipo = crm_entry.get("tipo_testata") or _deduce_tipo(testata_arch)

            if tipo_testata and tipo != tipo_testata:
                continue

            result.append({
                "id":                 crm_entry.get("id"),
                "nome":               crm_entry.get("nome") or gl.title(),
                "testata_principale": crm_entry.get("testata_principale") or testata_arch,
                "tipo_testata":       tipo,
                "email":              crm_entry.get("email"),
                "cellulare":          crm_entry.get("cellulare"),
                "note":               crm_entry.get("note"),
                "clienti_associati":  crm_entry.get("clienti_associati"),
                "n_articoli":         art_count.get(gl, 0),
                "in_crm":             bool(crm_entry.get("id")),
            })

        result.sort(key=lambda x: (-x["n_articoli"], x["nome"]))
        return result

    except Exception as e:
        return {"error": str(e)}


@app.get("/api/journalists/bubble-data")
async def journalists_bubble_data(
    client_id: Optional[str] = None,
    macro_id:  Optional[str] = None,
):
    """Dati per bubble map: testate → giornalisti con conteggi."""
    try:
        from collections import Counter as _Counter
        SKIP = {"", "n.d.", "n/d", "redazione", "autore non indicato"}

        arts_res = supabase.table("articles").select(
            "giornalista, testata, macrosettori, titolo, occhiello, testo_completo"
        ).execute()
        articles = arts_res.data or []

        # Filtro cliente
        client_art_ids: set | None = None
        if client_id:
            cl_res = supabase.table("clients").select("keywords_press, keywords").eq("id", client_id).execute()
            if cl_res.data:
                cl = cl_res.data[0]
                kws = [k.strip().lower() for k in (cl.get("keywords_press") or cl.get("keywords") or "").split(",") if k.strip()]
                if kws:
                    client_art_ids = set()
                    for i, a in enumerate(articles):
                        txt = f"{a.get('titolo','')} {a.get('occhiello','')} {a.get('testo_completo','')}".lower()
                        if any(kw in txt for kw in kws):
                            client_art_ids.add(i)

        # Filtro macro
        mac_names: list = []
        if macro_id:
            lnk_res = supabase.table("macro_group_links").select("official_macro_id").eq("macro_group_id", macro_id).execute()
            oids = [l["official_macro_id"] for l in (lnk_res.data or [])]
            if oids:
                mac_res = supabase.table("official_macrosectors").select("name").in_("id", oids).execute()
                mac_names = [m["name"].upper() for m in (mac_res.data or [])]

        # Costruisci struttura testata → giornalisti
        testata_data: dict = {}  # testata → {count, giornalisti: {nome → count}}

        for i, a in enumerate(articles):
            if client_art_ids is not None and i not in client_art_ids:
                continue
            if mac_names:
                macro_str = (a.get("macrosettori") or "").upper()
                if not any(mn in macro_str for mn in mac_names):
                    continue

            t = (a.get("testata") or "").strip()
            g = (a.get("giornalista") or "").strip()
            if not t:
                continue

            if t not in testata_data:
                testata_data[t] = {"testata": t, "count": 0, "giornalisti": {}}
            testata_data[t]["count"] += 1

            if g and g.lower() not in SKIP:
                testata_data[t]["giornalisti"][g] = testata_data[t]["giornalisti"].get(g, 0) + 1

        # Serializza
        nodes = []
        for t, d in testata_data.items():
            nodes.append({
                "testata": t,
                "count":   d["count"],
                "giornalisti": [
                    {"nome": nome, "count": cnt}
                    for nome, cnt in sorted(d["giornalisti"].items(), key=lambda x: -x[1])
                ]
            })
        nodes.sort(key=lambda x: -x["count"])
        return {"nodes": nodes}

    except Exception as e:
        return {"error": str(e)}


def _deduce_tipo(testata: str) -> str:
    """Deduce tipo testata dal nome."""
    if not testata:
        return "altro"
    t = testata.lower()
    if any(x in t for x in ["ansa", "adnkronos", "askanews", "agenzia", "dire "]):
        return "agenzia"
    if any(x in t for x in [".it", "web", "online", "blog", "news"]):
        return "web"
    if any(x in t for x in ["radio", "tv", "rai", "mediaset", "tg", "tele"]):
        return "radio_tv"
    if any(x in t for x in ["settimana", "mensile", "rivista", "magazine"]):
        return "periodico"
    return "quotidiano"


@app.get("/api/journalists/{journalist_id}")
async def get_journalist(journalist_id: str):
    try:
        res = supabase.table("journalists").select("*").eq("id", journalist_id).execute()
        if not res.data:
            raise HTTPException(status_code=404, detail="Giornalista non trovato")
        return res.data[0]
    except HTTPException:
        raise
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/journalists")
async def create_journalist(data: JournalistModel):
    try:
        res = supabase.table("journalists").insert({
            "nome":               data.nome.strip(),
            "testata_principale": data.testata_principale,
            "tipo_testata":       data.tipo_testata,
            "email":              data.email,
            "cellulare":          data.cellulare,
            "note":               data.note,
            "clienti_associati":  data.clienti_associati,
        }).execute()
        return {"success": True, "journalist": res.data[0] if res.data else {}}
    except Exception as e:
        return {"error": str(e)}


@app.put("/api/journalists/{journalist_id}")
async def update_journalist(journalist_id: str, data: JournalistUpdate):
    try:
        update = {k: v for k, v in data.dict().items() if v is not None}
        update["updated_at"] = datetime.now(timezone.utc).isoformat()
        res = supabase.table("journalists").update(update).eq("id", journalist_id).execute()
        return {"success": True, "journalist": res.data[0] if res.data else {}}
    except Exception as e:
        return {"error": str(e)}


@app.delete("/api/journalists/{journalist_id}")
async def delete_journalist(journalist_id: str):
    try:
        supabase.table("journalists").delete().eq("id", journalist_id).execute()
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/journalists/sync-from-articles")
async def sync_journalists_from_articles():
    """Importa nel CRM tutti i giornalisti dell'archivio non ancora presenti."""
    try:
        SKIP = {"", "n.d.", "n/d", "redazione", "autore non indicato"}
        arts_res = supabase.table("articles").select("giornalista, testata").execute()
        crm_res  = supabase.table("journalists").select("nome").execute()
        existing = {j["nome"].strip().lower() for j in (crm_res.data or [])}

        from collections import Counter as _Counter
        testate: dict = {}
        for a in (arts_res.data or []):
            g = (a.get("giornalista") or "").strip()
            if not g or g.lower() in SKIP:
                continue
            testate.setdefault(g.lower(), _Counter())[a.get("testata","") or ""] += 1

        inserted = 0
        for gl, tc in testate.items():
            if gl in existing:
                continue
            nome_display = gl.title()
            testata_main = tc.most_common(1)[0][0]
            tipo = _deduce_tipo(testata_main)
            supabase.table("journalists").insert({
                "nome":               nome_display,
                "testata_principale": testata_main,
                "tipo_testata":       tipo,
            }).execute()
            inserted += 1

        return {"success": True, "inserted": inserted}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/giornalista-articoli")
async def giornalista_articoli(nome: str = Query(...), period: str = Query("all"), limit: int = Query(200)):
    try:
        today = date.today()
        days_map = {"today": 0, "7days": 7, "30days": 30, "6months": 180, "year": 365, "all": None}
        days = days_map.get(period)
        query = supabase.table("articles").select(
            "id, titolo, testata, data, giornalista, tone, dominant_topic, macrosettori"
        ).eq("giornalista", nome)
        if days is not None:
            from_date = today.isoformat() if days == 0 else (today - timedelta(days=days)).isoformat()
            query = query.gte("data", from_date)
        res = query.order("data", desc=True).limit(limit).execute()
        return res.data or []
    except Exception as e:
        return []


@app.get("/api/top-giornalisti")
async def top_giornalisti(period: str = Query("30days"), limit: int = Query(20)):
    try:
        today = date.today()
        days_map = {"today": 0, "7days": 7, "30days": 30, "6months": 180, "year": 365}
        days = days_map.get(period, 30)
        from_date = today.isoformat() if days == 0 else (today - timedelta(days=days)).isoformat()
        res = supabase.table("articles").select("giornalista, testata, data").gte("data", from_date).lte("data", today.isoformat()).execute()
        articles = res.data or []
        SKIP = {"", "N.D.", "N/D", "Redazione", "Autore non indicato", "redazione"}
        counter = Counter(a.get("giornalista","") for a in articles if a.get("giornalista") and a["giornalista"] not in SKIP)
        return [{"nome": nome, "articoli": count} for nome, count in counter.most_common(limit)]
    except Exception as e:
        return []


@app.get("/api/top-giornalisti-ave")
async def top_giornalisti_ave(period: str = Query("today"), limit: int = Query(15)):
    try:
        today = date.today()
        days_map = {"today": 0, "7days": 7, "30days": 30, "6months": 180}
        days = days_map.get(period, 0)
        from_date = today.isoformat() if days == 0 else (today - timedelta(days=days)).isoformat()
        SKIP = {"", "N.D.", "N/D", "Redazione", "Autore non indicato", "redazione"}
        res = supabase.table("articles").select("giornalista, testata, titolo, ave") \
            .gte("data", from_date).lte("data", today.isoformat()).execute()
        best: dict = {}
        for a in res.data or []:
            g = a.get("giornalista", "")
            if not g or g in SKIP:
                continue
            ave = float(a.get("ave") or 0)
            if g not in best or ave > best[g]["ave"]:
                best[g] = {"nome": g, "testata": a.get("testata",""), "titolo": a.get("titolo",""), "ave": ave}
        ranked = sorted(best.values(), key=lambda x: x["ave"], reverse=True)[:limit]
        return ranked
    except Exception as e:
        return []


# ══════════════════════════════════════════════════════════════════════
# MACRO GROUPS
# ══════════════════════════════════════════════════════════════════════

@app.get("/api/macro-groups")
async def get_macro_groups():
    try:
        res = supabase.table("macro_groups").select("id, name").eq("active", True).order("name").execute()
        return {"groups": res.data or []}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/macro-group-articles")
async def get_macro_group_articles(macro_group_id: str, from_date: str, to_date: str):
    try:
        links = supabase.table("macro_group_links").select("official_macro_id").eq("macro_group_id", macro_group_id).execute()
        official_ids = [l["official_macro_id"] for l in (links.data or [])]
        if not official_ids:
            return {"articles": []}
        macros = supabase.table("official_macrosectors").select("name").in_("id", official_ids).execute()
        macro_names = [m["name"] for m in (macros.data or [])]
        articles_res = supabase.table("articles").select("id, titolo, testata, data, giornalista, macrosettori").gte("data", from_date).lte("data", to_date).order("data", desc=True).limit(300).execute()
        all_articles = articles_res.data or []
        filtered = [a for a in all_articles if a.get("macrosettori") and any(m.strip() in macro_names for m in a["macrosettori"].split(","))]
        return {"articles": filtered}
    except Exception as e:
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════════
# ARTICOLI FILTRATI
# ══════════════════════════════════════════════════════════════════════

@app.get("/api/articles-filtered")
async def get_articles_filtered(
    from_date:      str,
    to_date:        str,
    client_id:      Optional[str] = None,
    macro_group_id: Optional[str] = None,
    topic:          Optional[str] = None,
):
    try:
        articles_res = supabase.table("articles") \
            .select("id, titolo, testata, data, giornalista, macrosettori, testo_completo, occhiello, ave") \
            .gte("data", from_date).lte("data", to_date) \
            .order("ave", desc=True).limit(500).execute()
        articles = articles_res.data or []

        if client_id:
            client_res = supabase.table("clients").select("*").eq("id", client_id).execute()
            if client_res.data:
                client = client_res.data[0]
                keywords = [k.strip().lower() for k in (client.get("keywords_press") or client.get("keywords") or "").split(",") if k.strip()]
                if keywords:
                    articles = [a for a in articles if any(
                        kw in (a.get("testo_completo") or "").lower() or
                        kw in (a.get("titolo") or "").lower() or
                        kw in (a.get("occhiello") or "").lower()
                        for kw in keywords
                    )]
        elif topic:
            tl = topic.lower()
            articles = [a for a in articles if
                tl in (a.get("titolo") or "").lower() or
                tl in (a.get("testo_completo") or "").lower() or
                tl in (a.get("occhiello") or "").lower()
            ]

        if macro_group_id:
            links = supabase.table("macro_group_links").select("official_macro_id").eq("macro_group_id", macro_group_id).execute()
            official_ids = [l["official_macro_id"] for l in (links.data or [])]
            if official_ids:
                macros = supabase.table("official_macrosectors").select("name").in_("id", official_ids).execute()
                macro_names = [m["name"] for m in (macros.data or [])]
                articles = [a for a in articles if a.get("macrosettori") and any(m.strip() in macro_names for m in a["macrosettori"].split(","))]

        return {"articles": articles}
    except Exception as e:
        return {"error": str(e), "articles": []}


# ══════════════════════════════════════════════════════════════════════
# SHARE TOKEN
# ══════════════════════════════════════════════════════════════════════

@app.post("/api/share")
async def create_share(req: ShareRequest):
    try:
        if not req.article_ids:
            return {"error": "Nessun articolo selezionato"}
        token      = str(uuid.uuid4())[:8]
        expires_at = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
        supabase.table("shared_reports").insert({"token": token, "filters": {"article_ids": req.article_ids}, "expires_at": expires_at}).execute()
        return {"token": token}
    except Exception as e:
        return {"error": str(e)}


@app.get("/share/{token}")
async def read_share(token: str):
    try:
        now = datetime.now(timezone.utc).isoformat()
        row = supabase.table("shared_reports").select("*").eq("token", token).gt("expires_at", now).execute()
        if not row.data:
            return PlainTextResponse("Link scaduto o non trovato.", status_code=404)
        f = row.data[0]["filters"]
        article_ids = f.get("article_ids", [])
        if not article_ids:
            return PlainTextResponse("Nessun articolo salvato in questo link.", status_code=404)
        res = supabase.table("articles").select("id, titolo, testata, data, giornalista, macrosettori, testo_completo").in_("id", article_ids).execute()
        id_order = {aid: i for i, aid in enumerate(article_ids)}
        articles = sorted(res.data or [], key=lambda a: id_order.get(a["id"], 9999))
        lines = ["ARCHIVIO MAIM - " + str(len(articles)) + " articoli", ""]
        for i, a in enumerate(articles, 1):
            lines += ["---", f"[{i}] {a.get('titolo') or 'N/D'}",
                      f"Testata: {a.get('testata') or 'N/D'} | Data: {a.get('data') or 'N/D'} | Giornalista: {a.get('giornalista') or 'N/D'}",
                      f"Settori: {a.get('macrosettori') or 'N/D'}", "",
                      a.get("testo_completo") or "Testo non disponibile", ""]
        return PlainTextResponse("\n".join(lines))
    except Exception as e:
        return PlainTextResponse("Errore: " + str(e), status_code=500)


# ══════════════════════════════════════════════════════════════════════
# DEBUG
# ══════════════════════════════════════════════════════════════════════

@app.get("/api/debug-articles")
async def debug_articles():
    try:
        res       = supabase.table("articles").select("id, titolo, data, testata, giornalista").order("data", desc=True).limit(5).execute()
        clients   = supabase.table("clients").select("id, name, keywords_press, keywords_web, macro_strategici").execute()
        total     = supabase.table("articles").select("id", count="exact").execute()
        today     = date.today().isoformat()
        oggi      = supabase.table("articles").select("id").eq("data", today).execute().data or []
        last      = supabase.table("articles").select("data").order("data", desc=True).limit(1).execute()
        last_date = last.data[0]["data"] if last.data else None
        return {"ultimi_articoli": res.data, "totale_articoli": total.count,
                "articoli_oggi": len(oggi), "ultima_data": last_date, "clienti": clients.data}
    except Exception as e:
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════════
# ARTICOLI
# ══════════════════════════════════════════════════════════════════════

@app.get("/api/client-articles")
async def get_client_articles(client_id: str, from_date: str, to_date: str):
    try:
        client_res = supabase.table("clients").select("*").eq("id", client_id).execute()
        if not client_res.data:
            raise HTTPException(status_code=404, detail="Cliente non trovato")
        client_data = client_res.data[0]
        keywords = [k.strip().lower() for k in (client_data.get("keywords") or "").split(",") if k.strip()]
        articles_res = supabase.table("articles").select(
            "id, testata, data, giornalista, occhiello, titolo, sottotitolo, "
            "testo_completo, macrosettori, tipologia_articolo, tone, "
            "dominant_topic, reputational_risk, political_risk, ave, tipo_fonte"
        ).gte("data", from_date).lte("data", to_date).order("data", desc=True).execute()
        all_articles = articles_res.data or []
        filtered = [a for a in all_articles if any(
            kw in (a.get("testo_completo") or "").lower() or
            kw in (a.get("titolo") or "").lower() or
            kw in (a.get("occhiello") or "").lower()
            for kw in keywords
        )] if keywords else all_articles
        return {"client": client_data, "articles": filtered, "total": len(filtered)}
    except HTTPException:
        raise
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/articles")
async def get_articles(from_date: Optional[str] = None, to_date: Optional[str] = None, testata: Optional[str] = None, limit: int = 50):
    try:
        query = supabase.table("articles").select("id, titolo, testata, data, occhiello, giornalista, tone, dominant_topic, macrosettori")
        if from_date: query = query.gte("data", from_date)
        if to_date:   query = query.lte("data", to_date)
        if testata:   query = query.eq("testata", testata)
        res = query.order("data", desc=True).limit(limit).execute()
        return {"articles": res.data or [], "total": len(res.data or [])}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/article/{article_id}")
async def get_article(article_id: str):
    try:
        res = supabase.table("articles").select("*").eq("id", article_id).execute()
        if not res.data:
            raise HTTPException(status_code=404, detail="Articolo non trovato")
        return res.data[0]
    except HTTPException:
        raise
    except Exception as e:
        return {"error": str(e)}


@app.put("/api/article/{article_id}")
async def update_article(article_id: str, data: ArticleUpdateSimple):
    try:
        update_data = {k: v for k, v in data.dict().items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="Nessun campo da aggiornare")
        res = supabase.table("articles").update(update_data).eq("id", article_id).execute()
        return res.data[0] if res.data else {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        return {"error": str(e)}


@app.delete("/api/article/{article_id}")
async def delete_article(article_id: str):
    try:
        supabase.table("articles").delete().eq("id", article_id).execute()
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════════
# CLIENTI
# ══════════════════════════════════════════════════════════════════════

@app.get("/api/clients")
async def get_clients():
    try:
        res = supabase.table("clients").select("*").execute()
        return res.data or []
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/clients")
async def create_client(data: ClientModel):
    try:
        if not data.name or not data.name.strip():
            raise HTTPException(status_code=400, detail="Il nome è obbligatorio")
        res = supabase.table("clients").insert({
            "name": data.name.strip(), "keywords": data.keywords, "keywords_web": data.keywords_web,
            "sector": data.sector, "description": data.description, "website": data.website,
            "contact": data.contact, "semantic_topic": data.semantic_topic, "macro_strategici": data.macro_strategici,
        }).execute()
        return {"success": True, "id": res.data[0].get("id") if res.data else None, "client": res.data[0] if res.data else {}}
    except HTTPException:
        raise
    except Exception as e:
        return {"error": str(e)}


@app.put("/api/clients/{client_id}")
async def update_client(client_id: str, data: ClientModel):
    try:
        update_data = {k: v for k, v in data.dict().items() if v is not None}
        if not update_data:
            return {"success": True, "id": client_id, "client": {}}
        res = supabase.table("clients").update(update_data).eq("id", client_id).execute()
        return {"success": True, "id": client_id, "client": res.data[0] if res.data else {}}
    except Exception as e:
        return {"error": str(e)}


@app.delete("/api/clients/{client_id}")
async def delete_client(client_id: str):
    try:
        supabase.table("clients").delete().eq("id", client_id).execute()
        return {"success": True, "id": client_id}
    except Exception as e:
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════════
# FONTI
# ══════════════════════════════════════════════════════════════════════

@app.get("/api/sources")
async def get_sources():
    try:
        res = supabase.table("monitored_sources").select("*").order("name").execute()
        return res.data or []
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/sources")
async def create_source(data: SourceModel):
    try:
        res = supabase.table("monitored_sources").insert({"name": data.name, "url": data.url, "type": data.type, "active": data.active}).execute()
        return res.data[0] if res.data else {"success": True}
    except Exception as e:
        return {"error": str(e)}

@app.delete("/api/sources/{source_id}")
async def delete_source(source_id: str):
    try:
        supabase.table("monitored_sources").delete().eq("id", source_id).execute()
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}

@app.patch("/api/sources/{source_id}/toggle")
async def toggle_source(source_id: str, request: Request):
    try:
        body = await request.json()
        supabase.table("monitored_sources").update({"active": body.get("active", True)}).eq("id", source_id).execute()
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/monitored-sources")
async def get_monitored_sources_legacy():
    return await get_sources()

@app.post("/api/monitored-sources")
async def create_monitored_source_legacy(data: SourceModel):
    return await create_source(data)

@app.delete("/api/monitored-sources/{source_id}")
async def delete_monitored_source_legacy(source_id: str):
    return await delete_source(source_id)

@app.patch("/api/monitored-sources/{source_id}/toggle")
async def toggle_monitored_source_legacy(source_id: str, active: bool = Query(...)):
    try:
        supabase.table("monitored_sources").update({"active": active}).eq("id", source_id).execute()
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════════
# WEB MENTIONS
# ══════════════════════════════════════════════════════════════════════

@app.get("/api/web-mentions")
async def get_web_mentions(client: Optional[str] = None, limit: int = 50):
    try:
        query = supabase.table("web_mentions").select("*").order("published_at", desc=True)
        if client: query = query.ilike("matched_client", f"%{client}%")
        res = query.limit(limit).execute()
        return res.data or []
    except Exception as e:
        return {"error": str(e)}


class DeleteMentionsRequest(BaseModel):
    ids: List[str]

@app.post("/api/web-mentions/delete-bulk")
async def delete_web_mentions_bulk(req: DeleteMentionsRequest):
    try:
        supabase.table("web_mentions").delete().in_("id", req.ids).execute()
        return {"success": True, "deleted": len(req.ids)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════════
# WEB SCAN — Google News RSS
# ══════════════════════════════════════════════════════════════════════

import hashlib
from urllib.parse import quote_plus

# Job state esteso per web scan
_scan_progress: dict = {}
_scan_progress_lock = threading.Lock()

def _set_scan_progress(job_id: str, **kwargs):
    with _scan_progress_lock:
        if job_id not in _scan_progress:
            _scan_progress[job_id] = {}
        _scan_progress[job_id].update(kwargs)

def _get_scan_progress(job_id: str) -> dict:
    with _scan_progress_lock:
        return dict(_scan_progress.get(job_id, {}))

def _run_web_scan(job_id: str):
    """Scansione fonti RSS/scrape da monitored_sources — usa run_monitoring esistente."""
    import time as _time

    _set_scan_progress(job_id,
        status="running", current=0, total=0,
        current_client="", found=0, duplicates=0, errors=[]
    )

    try:
        # Carica fonti attive per sapere quante sono (per il progress)
        res_sources = supabase.table("monitored_sources").select("id, name").eq("active", True).execute()
        sources = res_sources.data or []
        _set_scan_progress(job_id, total=len(sources))

        if not sources:
            _set_scan_progress(job_id, status="done", found=0, duplicates=0, current_client="Nessuna fonte attiva")
            return

        # Aggiorna progress fonte per fonte
        from services.database import supabase as _sb
        clients_res = _sb.table("clients").select("id, name, keywords_web").execute()
        clients = clients_res.data or []

        # Importa le funzioni da monitor
        from services.monitor import fetch_rss, fetch_scrape, make_hash

        import re as _re
        all_records = []
        errors = []

        for i, source in enumerate(sources):
            _set_scan_progress(job_id, current=i+1, current_client=source["name"])
            try:
                if source.get("type") == "scrape":
                    from services.monitor import fetch_scrape as _fs
                    records = _fs(source, clients)
                else:
                    from services.monitor import fetch_rss as _fr
                    records = _fr(source, clients)
                all_records.extend(records)
                print(f"[SCAN] {source['name']}: {len(records)} match")
            except Exception as e:
                err_msg = f"{source['name']}: {str(e)}"
                errors.append(err_msg)
                print(f"[SCAN] ERRORE {err_msg}")
            _time.sleep(0.1)

        # Deduplicazione interna
        seen, deduped = set(), []
        for r in all_records:
            if r["content_hash"] not in seen:
                seen.add(r["content_hash"])
                deduped.append(r)

        # Upsert su Supabase
        inserted = 0
        duplicates = 0
        if deduped:
            result = supabase.table("web_mentions").upsert(
                deduped, on_conflict="content_hash"
            ).execute()
            inserted   = len(result.data) if result.data else 0
            duplicates = len(deduped) - inserted

        _set_scan_progress(job_id,
            status="done",
            found=inserted,
            duplicates=duplicates,
            errors=errors,
            current_client=""
        )
        print(f"[SCAN] Completata — {inserted} inseriti, {duplicates} duplicati")

    except Exception as e:
        _set_scan_progress(job_id, status="error", error=str(e))
        print(f"[SCAN] ERRORE GENERALE: {e}")


@app.post("/api/web-scan/start")
async def web_scan_start():
    """Avvia scansione Google News in background."""
    job_id = str(uuid.uuid4())[:8]
    t = threading.Thread(target=_run_web_scan, args=(job_id,), daemon=True)
    t.start()
    return {"job_id": job_id}


@app.get("/api/web-scan/status/{job_id}")
async def web_scan_status(job_id: str):
    """Polling stato scansione."""
    data = _get_scan_progress(job_id)
    if not data:
        raise HTTPException(status_code=404, detail="Job non trovato")
    return data


# ══════════════════════════════════════════════════════════════════════
# MONITOR (legacy — mantenuto per compatibilità)
# ══════════════════════════════════════════════════════════════════════

@app.post("/api/monitor/run")
async def monitor_run():
    try:
        if run_monitoring is None:
            return {"error": "Monitor non disponibile"}
        return run_monitoring()
    except Exception as e:
        return {"error": str(e), "found": 0, "duplicates": 0}

@app.post("/api/monitor/run-historical")
async def monitor_run_historical(req: HistoricalScanRequest):
    try:
        if run_monitoring is None:
            return {"error": "Monitor non disponibile"}
        return run_monitoring(from_date=req.from_date, to_date=req.to_date)
    except Exception as e:
        return {"error": str(e), "found": 0, "duplicates": 0}

@app.get("/api/monitor/scan-info")
async def monitor_scan_info():
    try:
        res = supabase.table("monitor_meta").select("key, value").in_("key", ["last_daily_scan", "last_historical_scan"]).execute()
        info = {row["key"]: row["value"] for row in (res.data or [])}
        return {"last_daily": info.get("last_daily_scan"), "last_historical": info.get("last_historical_scan")}
    except Exception as e:
        return {"last_daily": None, "last_historical": None}

@app.get("/api/monitor-meta")
async def get_monitor_meta():
    try:
        res = supabase.table("monitor_meta").select("*").execute()
        return {"meta": res.data or []}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/monitor-meta")
async def upsert_monitor_meta(data: dict):
    try:
        supabase.table("monitor_meta").upsert(data).execute()
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════════
# GIORNALISTI
# ══════════════════════════════════════════════════════════════════════

@app.get("/api/journalists")
async def get_journalists(from_date: Optional[str] = None, to_date: Optional[str] = None):
    try:
        query = supabase.table("articles").select("id, giornalista, testata, titolo, data")
        if from_date: query = query.gte("data", from_date)
        if to_date:   query = query.lte("data", to_date)
        res = query.execute()
        articles = res.data or []
        counter = Counter(a.get("giornalista","") for a in articles if a.get("giornalista") and a["giornalista"].lower() not in ("redazione",""))
        return {"journalists": [{"name": n, "count": c} for n, c in counter.most_common(50)], "total_articles": len(articles)}
    except Exception as e:
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════════
# PITCH
# ══════════════════════════════════════════════════════════════════════

@app.post("/api/pitch")
async def pitch_endpoint(message: str = Form(...), client_id: str = Form(""), history: str = Form("[]")):
    try:
        hist = json.loads(history) if history else []
    except Exception:
        hist = []
    try:
        result = pitch_advisor(message=message, client_id=client_id, history=hist)
        return {"success": True, **result}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════
# DAILY DIGEST
# ══════════════════════════════════════════════════════════════════════

def _send_digest_email(text: str, today_str: str):
    """
    Legge i destinatari attivi da digest_recipients su Supabase
    e invia il digest in plain text via Resend.
    Non solleva eccezioni — logga solo.
    """
    try:
        import resend
    except ImportError:
        print("[EMAIL] resend non installato — esegui: pip install resend")
        return

    api_key = os.getenv("RESEND_API_KEY", "")
    if not api_key:
        print("[EMAIL] RESEND_API_KEY non configurata — invio saltato")
        return

    resend.api_key = api_key

    # Legge destinatari attivi
    try:
        res_rec = (
            supabase.table("digest_recipients")
            .select("email, name")
            .eq("active", True)
            .execute()
        )
        recipients = res_rec.data or []
    except Exception as e:
        print(f"[EMAIL] Errore lettura destinatari: {e}")
        return

    if not recipients:
        print("[EMAIL] Nessun destinatario attivo — invio saltato")
        return

    to_list = [r["email"] for r in recipients if r.get("email")]
    print(f"[EMAIL] Invio a {len(to_list)} destinatari: {to_list}")

    try:
        resend.Emails.send({
            "from":    "MAIM Digest <digest@maim.it>",  # cambia con il tuo dominio verificato su Resend
            "to":      to_list,
            "subject": f"MAIM DIGEST — {today_str}",
            "text":    text,
        })
        print(f"[EMAIL] Inviato correttamente a {len(to_list)} destinatari")
    except Exception as e:
        print(f"[EMAIL] Errore invio: {e}")


def _run_digest_job(job_id: str):
    """Eseguito in thread separato. Genera digest, invia email, salva su Supabase."""
    import traceback
    try:
        today     = date.today().isoformat()
        today_str = date.today().strftime("%d/%m/%Y")

        res_art = (
            supabase.table("articles")
            .select("id, testata, data, giornalista, titolo, occhiello, testo_completo, tone, ave, tipologia_articolo")
            .eq("data", today)
            .order("ave", desc=True)
            .execute()
        )
        articles_today = res_art.data or []
        print(f"[DIGEST] {len(articles_today)} articoli oggi")

        res_cli = supabase.table("clients").select("id, name, keywords_web").execute()
        clients = res_cli.data or []

        result = generate_digest(articles_today=articles_today, clients=clients)

        if "error" in result and not result.get("text"):
            _set_job(job_id, "error", error=result["error"])
        else:
            _set_job(job_id, "done", result=result)
            digest_text = result.get("text", "")

            if digest_text:
                # 1. Invia email
                _send_digest_email(digest_text, today_str)

                # 2. Salva su Supabase
                try:
                    supabase.table("digests").upsert({
                        "data":            today,
                        "text":            digest_text,
                        "articles_today":  result.get("articles_today", 0),
                        "client_mentions": result.get("client_mentions", 0),
                    }, on_conflict="data").execute()
                    print(f"[DIGEST] Salvato in Supabase per {today}")
                except Exception as save_err:
                    print(f"[DIGEST] Errore salvataggio Supabase: {save_err}")

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[DIGEST ERROR] {tb}")
        _set_job(job_id, "error", error=str(e) + " | " + tb.splitlines()[-1])


@app.post("/api/daily-digest")
async def daily_digest_endpoint():
    _cleanup_old_jobs()
    job_id = str(uuid.uuid4())[:12]
    _set_job(job_id, "pending")
    t = threading.Thread(target=_run_digest_job, args=(job_id,), daemon=True)
    t.start()
    return {"success": True, "job_id": job_id}


@app.get("/api/digest/status")
async def digest_status():
    """Stato digest del giorno: ready | generating | idle"""
    today = date.today().isoformat()
    try:
        res = supabase.table("digests").select("*").eq("data", today).execute()
        if res.data:
            row = res.data[0]
            return {
                "status":          "ready",
                "text":            row.get("text", ""),
                "articles_today":  row.get("articles_today", 0),
                "client_mentions": row.get("client_mentions", 0),
                "created_at":      row.get("created_at"),
            }
    except Exception as e:
        print(f"[DIGEST STATUS] {e}")

    for jid, j in _JOBS.items():
        if j.get("status") in ("pending", "running"):
            return {"status": "generating", "job_id": jid}

    return {"status": "idle"}


@app.get("/api/digest/dates")
async def digest_dates():
    """Lista date con digest disponibile (ultimi 30 giorni)."""
    try:
        res = supabase.table("digests") \
            .select("data, articles_today, client_mentions, created_at") \
            .order("data", desc=True).limit(30).execute()
        return {"dates": res.data or []}
    except Exception as e:
        return {"dates": [], "error": str(e)}


@app.get("/api/digest/{data_str}")
async def get_digest_by_date(data_str: str):
    """Recupera digest per data YYYY-MM-DD."""
    try:
        res = supabase.table("digests").select("*").eq("data", data_str).execute()
        if not res.data:
            raise HTTPException(status_code=404, detail="Digest non trovato")
        return res.data[0]
    except HTTPException:
        raise
    except Exception as e:
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════════
# DIGEST AUDIO (OpenAI TTS)
# ══════════════════════════════════════════════════════════════════════

class DigestAudioRequest(BaseModel):
    text: str

@app.post("/api/digest-audio")
async def digest_audio(req: DigestAudioRequest):
    """
    Riceve il testo del digest e restituisce un MP3 generato da OpenAI TTS.
    Voce: shimmer — modello: tts-1.
    """
    from openai import OpenAI
    if not req.text or not req.text.strip():
        raise HTTPException(status_code=400, detail="Testo vuoto")

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY non configurata")

    try:
        ai = OpenAI(api_key=api_key)
        # Pulizia testo: rimuove formattazione WA non necessaria per audio
        clean = req.text.replace("*", "").replace("_", "").replace("————————————————————", ". ")
        response = ai.audio.speech.create(
            model="tts-1",
            voice="shimmer",
            input=clean,
            response_format="mp3",
        )
        audio_bytes = response.content
        return StreamingResponse(
            iter([audio_bytes]),
            media_type="audio/mpeg",
            headers={"Content-Disposition": "inline; filename=digest.mp3"}
        )
    except Exception as e:
        print(f"[AUDIO TTS] Errore: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════════
# WEB DIGEST
# ══════════════════════════════════════════════════════════════════════

@app.post("/api/web-digest/generate")
async def generate_web_digest():
    """
    Legge web_mentions pubblicate oggi dalle 07:00 fino al momento della generazione,
    genera temi via GPT, raggruppa per cliente, salva in web_digests,
    restituisce token per la pagina pubblica.
    """
    from openai import OpenAI
    from datetime import datetime, timezone, timedelta
    import secrets

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY non configurata")

    ai = OpenAI(api_key=api_key)

    # Finestra: published_at = oggi, created_at >= 07:00 Roma (06:00 UTC) fino a ora
    today = date.today()
    now_utc   = datetime.now(timezone.utc).isoformat()
    start_utc = datetime(today.year, today.month, today.day, 6, 0, 0, tzinfo=timezone.utc).isoformat()

    try:
        res = (
            supabase.table("web_mentions")
            .select("id, source_name, title, url, summary, matched_client, tone, published_at, created_at")
            .eq("published_at", today.isoformat())
            .gte("created_at", start_utc)
            .lte("created_at", now_utc)
            .order("created_at", desc=False)
            .execute()
        )
        mentions = res.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Errore lettura web_mentions: {e}")

    if not mentions:
        raise HTTPException(status_code=404, detail="Nessuna mention trovata per oggi")

    # ── Temi principali via GPT ───────────────────────────────────────
    titoli_block = "\n".join(
        f"[{m.get('source_name','')}] {m.get('title','')}"
        for m in mentions
    )
    giorni = ["Lunedì","Martedì","Mercoledì","Giovedì","Venerdì","Sabato","Domenica"]
    mesi   = ["","gennaio","febbraio","marzo","aprile","maggio","giugno",
              "luglio","agosto","settembre","ottobre","novembre","dicembre"]
    data_ext = f"{giorni[today.weekday()]} {today.day} {mesi[today.month]} {today.year}"

    try:
        resp_temi = ai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Sei il sistema MAIM Intelligence. Produci contenuto conciso e professionale."},
                {"role": "user", "content": (
                    f"Data: {data_ext} | Fonte: web, ore 08:00-13:00 | Totale articoli: {len(mentions)}\n\n"
                    f"TITOLI:\n{titoli_block}\n\n"
                    f"Produci SOLO questo blocco, formato esatto:\n\n"
                    f"▶️ *TEMI PRINCIPALI*\n"
                    f"[Max 5 temi. Per ognuno: *nome breve in grassetto* + 2 righe di fatti puri. "
                    f"Ordine: politica estera, politica interna, economia, energia, cultura. "
                    f"Solo temi effettivamente presenti.]\n\n"
                    f"▶️ *DA TENERE D'OCCHIO*\n"
                    f"[1-2 segnali deboli utili per chi fa comunicazione.]"
                )}
            ],
            temperature=0.1,
            max_tokens=900,
        )
        themes_text = resp_temi.choices[0].message.content.strip()
    except Exception as e:
        themes_text = f"Errore generazione temi: {e}"

    # ── Raggruppa mention per cliente ─────────────────────────────────
    clients_map: dict = {}
    for m in mentions:
        raw = (m.get("matched_client") or "").strip()
        if not raw:
            continue
        for cl in [c.strip() for c in raw.split(",") if c.strip()]:
            clients_map.setdefault(cl, []).append(m)

    # ── Testo WA (senza link) ─────────────────────────────────────────
    wa_lines = [f"*MAIM WEB DIGEST*\n{data_ext}\n"]
    wa_lines.append(themes_text)
    for cl, arts in clients_map.items():
        wa_lines.append(f"\n🟧 *{cl.upper()}* — {len(arts)} articoli\n")
        for a in arts:
            src  = (a.get("source_name") or "").upper()
            tit  = (a.get("title") or "").strip()
            summ = (a.get("summary") or "").strip()
            wa_lines.append(f"*{src}*\n_{tit}_\n→ {summ}\n")
    wa_text = "\n".join(wa_lines)

    # ── Salva in web_digests ──────────────────────────────────────────
    token = secrets.token_hex(8)
    try:
        supabase.table("web_digests").insert({
            "token":    token,
            "data":     today.isoformat(),
            "themes":   themes_text,
            "mentions": clients_map,
            "text_wa":  wa_text,
        }).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Errore salvataggio: {e}")

    return {"token": token, "mentions": len(mentions), "clients": list(clients_map.keys())}


@app.get("/api/web-digest/{token}")
async def get_web_digest(token: str):
    """Restituisce il web digest per token."""
    try:
        res = supabase.table("web_digests").select("*").eq("token", token).execute()
        if not res.data:
            raise HTTPException(status_code=404, detail="Digest non trovato")
        return res.data[0]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════════
# TESTATE TIER
# ══════════════════════════════════════════════════════════════════════

class TestataUpdate(BaseModel):
    testata: str
    tier:    int
    ordine:  Optional[int] = None

class TestateUpdateRequest(BaseModel):
    testate: List[TestataUpdate]

@app.get("/api/testate-tier")
async def get_testate_tier():
    """Restituisce tutte le testate con tier e ordine."""
    try:
        res = supabase.table("testate_tier").select("testata, tier, ordine").order("tier").execute()
        return res.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/testate-tier/save")
async def save_testate_tier(req: TestateUpdateRequest):
    """Salva l'intero stato tier (upsert bulk)."""
    try:
        rows = [{"testata": t.testata, "tier": t.tier, "ordine": t.ordine} for t in req.testate]
        supabase.table("testate_tier").upsert(rows, on_conflict="testata").execute()
        return {"success": True, "saved": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/testate")
async def testate_page():
    return FileResponse("web/testate.html")


# ══════════════════════════════════════════════════════════════════════
# GMAIL IMAP — IMPORT RASSEGNA
# ══════════════════════════════════════════════════════════════════════

import imaplib
import email as _email_lib
import hashlib as _hashlib
from email.header import decode_header as _decode_header

_gmail_state = {
    "status":      "idle",
    "last_check":  None,
    "last_import": None,
    "found":       0,
    "imported":    0,
    "errors":      [],
    "log":         [],
}

def _gmail_log(msg: str):
    print(f"[GMAIL] {msg}")
    _gmail_state["log"] = ([msg] + _gmail_state["log"])[:50]

def _run_gmail_import(auto: bool = False):
    """Connette a Gmail via IMAP, cerca mail da Ufficio.Stampa@snam.it,
    parsa gli articoli e li ingestiona nel DB."""
    import imaplib, re, requests
    from bs4 import BeautifulSoup
    from datetime import datetime

    gmail_user = os.getenv("GMAIL_USER", "")
    gmail_pass = os.getenv("GMAIL_APP_PASSWORD", "")

    if not gmail_user or not gmail_pass:
        _gmail_state["status"] = "error"
        _gmail_log("GMAIL_USER o GMAIL_APP_PASSWORD non configurati")
        return

    _gmail_state["status"]     = "running"
    _gmail_state["last_check"] = datetime.now().isoformat()
    _gmail_state["errors"]     = []
    _gmail_log(f"Connessione a Gmail ({gmail_user})…")

    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(gmail_user, gmail_pass)
        mail.select("INBOX")

        # Cerca mail delle ultime 24 ore da mittenti autorizzati (lette e non lette)
        from datetime import datetime, timedelta
        since_date = (datetime.now() - timedelta(hours=24)).strftime("%d-%b-%Y")
        allowed_senders = ["Ufficio.Stampa@snam.it", "stampa@maimgroup.com"]
        mail_ids_all = set()
        for sender in allowed_senders:
            _, data = mail.search(None, f'(FROM "{sender}" SINCE "{since_date}")')
            for mid in data[0].split():
                mail_ids_all.add(mid)
        mail_ids = list(mail_ids_all)
        _gmail_log(f"Mail trovate (ultime 24h): {len(mail_ids)}")

        if not mail_ids:
            _gmail_state["status"]  = "idle"
            _gmail_state["found"]   = 0
            _gmail_state["imported"]= 0
            mail.logout()
            return

        total_imported = 0
        total_found    = 0

        for mid in mail_ids:
            _, msg_data = mail.fetch(mid, "(RFC822)")
            raw = msg_data[0][1]
            msg = _email_lib.message_from_bytes(raw)

            # Deduplicazione per Message-ID — evita reimport
            message_id = msg.get("Message-ID", "").strip()
            if message_id:
                mid_hash = f"mid:{_hashlib.md5(message_id.encode()).hexdigest()}"
                exists = supabase.table("articles").select("id").eq("content_hash_mail", mid_hash).limit(1).execute()
                if exists.data:
                    _gmail_log(f"Mail già processata — salto")
                    continue

            # Estrai soggetto
            subj = ""
            for part, enc in _decode_header(msg.get("Subject", "")):
                if isinstance(part, bytes):
                    subj += part.decode(enc or "utf-8", errors="ignore")
                else:
                    subj += part

            _gmail_log(f"Processo: {subj[:60]}")

            # Estrai HTML
            html_body = ""
            for part in msg.walk():
                if part.get_content_type() == "text/html":
                    html_body = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                    break

            if not html_body:
                _gmail_log("Nessuna parte HTML — salto")
                continue

            # Parsa articoli
            articoli = _parse_rassegna_html(html_body)
            total_found += len(articoli)
            _gmail_log(f"Articoli estratti: {len(articoli)}")

            # Fetcha testo e ingestiona
            for art in articoli:
                try:
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                        "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Connection": "keep-alive",
                        "Upgrade-Insecure-Requests": "1",
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "none",
                        "Sec-Fetch-User": "?1",
                        "Cache-Control": "max-age=0",
                    }
                    session = requests.Session()
                    # Prima richiesta alla home per ottenere cookie
                    session.get("https://rassegna.snam.it", headers=headers, timeout=10)
                    # Poi fetcha l'articolo con i cookie ottenuti
                    resp = session.get(art["url"], headers=headers, timeout=15)
                    body = resp.text.strip() if resp.ok else ""
                    # Verifica che non sia la pagina Octofence
                    if "Checking your browser" in body or "octofence" in body.lower() or len(body) < 100:
                        testo = ""
                        _gmail_log(f"Octofence su {art['testata']} — testo non disponibile")
                    else:
                        testo = body
                except Exception as e:
                    testo = ""
                    _gmail_state["errors"].append(f"{art['testata']}: {e}")

                # Deduplicazione su hash testata+titolo+data
                h = _hashlib.md5(f"{art['testata']}|{art['titolo']}|{art['data']}".encode()).hexdigest()

                exists = supabase.table("articles").select("id").eq("content_hash_mail", h).execute()
                if exists.data:
                    continue

                try:
                    supabase.table("articles").insert({
                        "testata":           art["testata"],
                        "titolo":            art["titolo"],
                        "data":              art["data"],
                        "testo_completo":    testo,
                        "giornalista":       "",
                        "content_hash":      h,
                        "content_hash_mail": h,
                        "fonte":             "gmail_rassegna",
                    }).execute()
                    total_imported += 1
                except Exception as ins_err:
                    _gmail_state["errors"].append(f"Insert {art['testata']}: {ins_err}")
                    _gmail_log(f"Errore insert {art['testata']}: {ins_err}")

            # Salva un record sentinella con mid_hash per non riprocessare la mail
            if message_id:
                try:
                    supabase.table("articles").insert({
                        "testata":           "_gmail_processed_",
                        "titolo":            f"Processata: {subj[:100]}",
                        "data":              date.today().isoformat(),
                        "content_hash":      mid_hash,
                        "content_hash_mail": mid_hash,
                        "fonte":             "gmail_sentinel",
                    }).execute()
                except Exception:
                    pass

        _gmail_state["found"]    = total_found
        _gmail_state["imported"] = total_imported
        _gmail_state["last_import"] = datetime.now().isoformat()
        _gmail_state["status"]   = "idle"
        _gmail_log(f"Completato: {total_imported}/{total_found} articoli importati")
        mail.logout()

    except Exception as e:
        _gmail_state["status"] = "error"
        _gmail_state["errors"].append(str(e))
        _gmail_log(f"ERRORE: {e}")


def _parse_rassegna_html(html: str) -> list:
    """Parsa il body HTML della mail e restituisce lista di articoli."""
    import re
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    articoli = []
    seen_urls = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].replace("&amp;", "&")
        if "tiplink=4" not in href:
            continue
        # Salta duplicati con : nel imgatt
        if re.search(r"imgatt=:", href):
            continue
        if href in seen_urls:
            continue
        seen_urls.add(href)

        tr_curr = a.find_parent("tr")
        if not tr_curr:
            continue

        # Titolo = testo riga corrente senza testi link
        titolo_raw = tr_curr.get_text(separator=" ", strip=True)
        titolo = re.sub(r"\[.*?\]", "", titolo_raw).strip()
        if not titolo:
            continue

        # Testata e data = riga precedente
        tr_prev = tr_curr.find_previous_sibling("tr")
        testata_data = tr_prev.get_text(strip=True) if tr_prev else ""
        m = re.match(r"^(.+?)\s*[·•]\s*(\d{2}-\d{2}-\d{4})", testata_data)
        if m:
            testata  = m.group(1).strip()
            d, mo, y = m.group(2).split("-")
            data     = f"{y}-{mo}-{d}"
        else:
            testata = testata_data
            data    = date.today().isoformat()

        # Pulisci testata da " pag.XX"
        testata = re.sub(r"\s+pag\.\S+.*$", "", testata, flags=re.IGNORECASE).strip()

        articoli.append({
            "testata": testata,
            "titolo":  titolo,
            "data":    data,
            "url":     href,
        })

    return articoli


@app.post("/api/gmail/import")
async def gmail_import_manual():
    """Avvia import manuale Gmail in background."""
    if _gmail_state["status"] == "running":
        return {"status": "already_running"}
    t = threading.Thread(target=_run_gmail_import, args=(False,), daemon=True)
    t.start()
    return {"status": "started"}


@app.get("/api/gmail/status")
async def gmail_status():
    """Stato corrente del Gmail importer."""
    return _gmail_state


# ══════════════════════════════════════════════════════════════════════
# AVVIO
# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))