import os
import shutil
import uvicorn
import json
import uuid
import time
import hashlib
import secrets
from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query, Request
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
from typing import List, Optional
from datetime import date, timedelta, datetime, timezone
from collections import Counter

try:
    from api.ingestion import process_csv
    from services.database import supabase
    from api.chat import ask_spiz
    from api.pitch import pitch_advisor
except ImportError as e:
    print(f"❌ ERRORE IMPORTAZIONE CORE: {e}")

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

app = FastAPI(title="MAIM Intelligence")
app.mount("/static", StaticFiles(directory="web"), name="static")

# ══════════════════════════════════════════════════════════════════════
# AUTENTICAZIONE
# ══════════════════════════════════════════════════════════════════════

LOGIN_USERNAME = os.environ.get("LOGIN_USERNAME", "admin")
LOGIN_PASSWORD = os.environ.get("LOGIN_PASSWORD", "!?!19481948")
SESSION_COOKIE = "maim_session"

# Sessioni attive in memoria: token -> expiry timestamp
_SESSIONS: dict = {}
SESSION_DURATION = 8 * 3600  # 8 ore

def _create_session() -> str:
    token = secrets.token_hex(32)
    _SESSIONS[token] = time.time() + SESSION_DURATION
    return token

def _is_valid_session(token: str | None) -> bool:
    if not token:
        return False
    expiry = _SESSIONS.get(token)
    if not expiry:
        return False
    if time.time() > expiry:
        del _SESSIONS[token]
        return False
    # Rinnova la sessione ad ogni richiesta
    _SESSIONS[token] = time.time() + SESSION_DURATION
    return True

# Route pubbliche (non richiedono login)
PUBLIC_PATHS = {"/login", "/api/login", "/health", "/healthcheck"}

class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        # Permettiamo sempre static files e route pubbliche
        if path.startswith("/static") or path in PUBLIC_PATHS:
            return await call_next(request)
        # Controlliamo il cookie di sessione
        token = request.cookies.get(SESSION_COOKIE)
        if not _is_valid_session(token):
            # Se è una chiamata API restituiamo 401, altrimenti redirect al login
            if path.startswith("/api/"):
                return JSONResponse({"error": "Non autenticato"}, status_code=401)
            return RedirectResponse(url="/login", status_code=302)
        return await call_next(request)

app.add_middleware(AuthMiddleware)

os.makedirs("data/raw", exist_ok=True)
os.makedirs("web", exist_ok=True)

# ── STORAGE TEMPORANEO DOCX ───────────────────────────────────────────
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
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass
        del _DOCX_STORE[k]


# ── MODELLI ────────────────────────────────────────────────────────────
class ChatRequest(BaseModel):
    message: str
    context: Optional[str] = "general"
    history: Optional[list] = []

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
# LOGIN / LOGOUT
# ══════════════════════════════════════════════════════════════════════

@app.get("/login")
async def login_page():
    return FileResponse("web/login.html")

class LoginRequest(BaseModel):
    username: str
    password: str

@app.post("/api/login")
async def api_login(data: LoginRequest):
    if data.username == LOGIN_USERNAME and data.password == LOGIN_PASSWORD:
        token = _create_session()
        response = JSONResponse({"success": True})
        response.set_cookie(
            key=SESSION_COOKIE,
            value=token,
            httponly=True,
            samesite="lax",
            max_age=SESSION_DURATION,
        )
        return response
    return JSONResponse({"success": False, "error": "Credenziali non valide"}, status_code=401)

@app.post("/api/logout")
async def api_logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if token and token in _SESSIONS:
        del _SESSIONS[token]
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(SESSION_COOKIE)
    return response


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
    return FileResponse("web/press.html")  # alias legacy

@app.get("/web")
async def web_page():
    return FileResponse("web/web.html")

@app.get("/monitor")
async def monitor_page():
    return FileResponse("web/web.html")  # alias legacy

@app.get("/chat")
async def chat_page():
    return FileResponse("web/chat.html")

@app.get("/clients")
async def clients_page():
    return FileResponse("web/clienti.html")

@app.get("/giornalisti")
async def giornalisti_page():
    return FileResponse("web/giornalisti.html")

@app.get("/pitch")
async def pitch_page():
    return FileResponse("web/pitch.html")

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.get("/healthcheck")
async def healthcheck():
    return {"status": "ok"}


# ══════════════════════════════════════════════════════════════════════
# UPLOAD CSV INGESTIONE
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
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            results.append({"file": file.filename, "status": "error", "message": str(e)})
    return {"results": results}


# ══════════════════════════════════════════════════════════════════════
# CHAT
# ══════════════════════════════════════════════════════════════════════

@app.post("/api/chat")
async def chat_endpoint(req: ChatRequest):
    try:
        result = ask_spiz(
            message=req.message,
            history=req.history or [],
            context=req.context or "general",
        )
    except Exception as e:
        return {"success": False, "error": str(e)}

    if "error" in result:
        return {"success": False, "error": result["error"]}

    _cleanup_expired_docx()
    docx_token = _store_docx(result.get("docx_path"))

    return {
        "success":       True,
        "response":      result.get("response", ""),
        "is_report":     result.get("is_report", False),
        "articles_used": result.get("articles_used", 0),
        "total_period":  result.get("total_period", 0),
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
        return {
            "totale":    total.count or 0,
            "oggi":      oggi.count or 0,
            "settimana": settimana.count or 0,
            "mese":      mese.count or 0,
        }
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
        res      = supabase.table("articles").select(
            "testata, tone, macrosettori, giornalista"
        ).eq("data", today).execute()
        articles = res.data or []

        testate_counter     = Counter(a.get("testata","") for a in articles if a.get("testata"))
        giornalisti_counter = Counter(
            a.get("giornalista","") for a in articles
            if a.get("giornalista") and a["giornalista"].lower() not in ("redazione","n.d.","n/d","")
        )
        tones    = Counter(a.get("tone","") for a in articles if a.get("tone"))
        tone_tot = sum(tones.values()) or 1

        return {
            "total_today": len(articles),
            "totale":      len(articles),
            "testate":     [{"name": k, "count": v} for k,v in testate_counter.most_common(10)],
            "giornalisti": [{"nome": k, "articoli": v} for k,v in giornalisti_counter.most_common(20)],
            "sentiment":   {k: round(v/tone_tot*100) for k,v in tones.items() if k},
        }
    except Exception as e:
        return {"total_today": 0, "totale": 0, "testate": [], "giornalisti": [], "sentiment": {}, "error": str(e)}


@app.get("/api/today-mentions")
async def today_mentions():
    try:
        today = date.today().isoformat()

        clients_res = supabase.table("clients").select("*").execute()
        clients     = clients_res.data or []

        arts_res = supabase.table("articles").select(
            "id, titolo, testata, giornalista, tone, dominant_topic, testo_completo, occhiello"
        ).eq("data", today).execute()
        articles = arts_res.data or []

        result = []
        for cl in clients:
            raw_keywords = cl.get("keywords_press") or cl.get("keywords") or ""
            keywords = [k.strip().lower() for k in raw_keywords.split(",") if k.strip()]
            if not keywords:
                count = 0
            else:
                count = sum(
                    1 for a in articles
                    if any(
                        kw in (a.get("testo_completo") or "").lower() or
                        kw in (a.get("titolo") or "").lower() or
                        kw in (a.get("occhiello") or "").lower()
                        for kw in keywords
                    )
                )
            result.append({
                "id":       cl["id"],
                "name":     cl.get("name",""),
                "keywords": raw_keywords,
                "today":    count,
            })

        return result
    except Exception as e:
        return []


# ══════════════════════════════════════════════════════════════════════
# TOP GIORNALISTI
# ══════════════════════════════════════════════════════════════════════

@app.get("/api/top-giornalisti")
async def top_giornalisti(
    period: str = Query("30days"),
    limit:  int = Query(20),
):
    try:
        today = date.today()
        days_map = {"today": 0, "7days": 7, "30days": 30, "6months": 180, "year": 365}
        days = days_map.get(period, 30)
        from_date = today.isoformat() if days == 0 else (today - timedelta(days=days)).isoformat()
        to_date   = today.isoformat()

        res = supabase.table("articles").select(
            "giornalista, testata, data"
        ).gte("data", from_date).lte("data", to_date).execute()

        articles = res.data or []
        SKIP = {"", "N.D.", "N/D", "Redazione", "Autore non indicato", "redazione"}
        counter = Counter(
            a.get("giornalista","") for a in articles
            if a.get("giornalista") and a["giornalista"] not in SKIP
        )

        return [{"nome": nome, "articoli": count} for nome, count in counter.most_common(limit)]
    except Exception as e:
        return []



@app.get("/api/top-giornalisti-ave")
async def top_giornalisti_ave(
    period: str = Query("today"),
    limit:  int = Query(15),
):
    """Top giornalisti per AVE del loro articolo più importante nel periodo."""
    try:
        today = date.today()
        days_map = {"today": 0, "7days": 7, "30days": 30, "6months": 180, "year": 365}
        days = days_map.get(period, 0)
        from_date = today.isoformat() if days == 0 else (today - timedelta(days=days)).isoformat()
        to_date = today.isoformat()

        res = supabase.table("articles").select(
            "giornalista, testata, ave, titolo"
        ).gte("data", from_date).lte("data", to_date).execute()

        articles = res.data or []
        SKIP = {"", "N.D.", "N/D", "Redazione", "Autore non indicato", "redazione"}

        from collections import defaultdict
        # Per ogni giornalista teniamo l'articolo con AVE massima
        best = {}
        for a in articles:
            g = (a.get("giornalista") or "").strip()
            if not g or g in SKIP:
                continue
            ave = float(a.get("ave") or 0)
            if g not in best or ave > best[g]["ave"]:
                best[g] = {
                    "ave":     ave,
                    "testata": a.get("testata") or "",
                    "titolo":  (a.get("titolo") or "")[:80],
                }

        result = [
            {"nome": nome, "ave": round(v["ave"], 0), "testata": v["testata"], "titolo": v["titolo"]}
            for nome, v in best.items()
            if v["ave"] > 0
        ]
        result.sort(key=lambda x: x["ave"], reverse=True)
        return result[:limit]
    except Exception as e:
        return []

@app.get("/api/giornalista-articoli")
async def giornalista_articoli(
    nome:   str = Query(...),
    period: str = Query("30days"),
    limit:  int = Query(100),
):
    try:
        today = date.today()
        days_map = {"today": 0, "7days": 7, "30days": 30, "6months": 180, "year": 365}
        days = days_map.get(period, 30)
        from_date = today.isoformat() if days == 0 else (today - timedelta(days=days)).isoformat()
        to_date   = today.isoformat()

        res = (supabase.table("articles")
               .select("id, titolo, testata, data, giornalista, tone, dominant_topic")
               .eq("giornalista", nome)
               .gte("data", from_date)
               .lte("data", to_date)
               .order("data", desc=True)
               .limit(limit)
               .execute())

        return res.data or []
    except Exception as e:
        return []


# ══════════════════════════════════════════════════════════════════════
# MACRO GROUPS
# ══════════════════════════════════════════════════════════════════════

@app.get("/api/macro-groups")
async def get_macro_groups():
    try:
        res = supabase.table("macro_groups") \
            .select("id, name") \
            .eq("active", True) \
            .order("name") \
            .execute()
        return {"groups": res.data or []}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/macro-group-articles")
async def get_macro_group_articles(
    macro_group_id: str,
    from_date: str,
    to_date: str,
):
    try:
        links = supabase.table("macro_group_links") \
            .select("official_macro_id") \
            .eq("macro_group_id", macro_group_id) \
            .execute()
        official_ids = [l["official_macro_id"] for l in (links.data or [])]

        if not official_ids:
            return {"articles": []}

        macros = supabase.table("official_macrosectors") \
            .select("name") \
            .in_("id", official_ids) \
            .execute()
        macro_names = [m["name"] for m in (macros.data or [])]

        articles_res = supabase.table("articles") \
            .select("id, titolo, testata, data, giornalista, macrosettori") \
            .gte("data", from_date) \
            .lte("data", to_date) \
            .order("data", desc=True) \
            .limit(300) \
            .execute()
        all_articles = articles_res.data or []

        filtered = []
        for a in all_articles:
            if not a.get("macrosettori"):
                continue
            article_macros = [m.strip() for m in a["macrosettori"].split(",")]
            if any(m in macro_names for m in article_macros):
                filtered.append(a)

        return {"articles": filtered}
    except Exception as e:
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════════
# ARTICOLI FILTRATI
# ══════════════════════════════════════════════════════════════════════

@app.get("/api/articles-filtered")
async def get_articles_filtered(
    from_date: str,
    to_date: str,
    client_id: str | None = None,
    macro_group_id: str | None = None,
    sort: str | None = None,
):
    try:
        articles_res = supabase.table("articles") \
            .select("id, titolo, testata, data, giornalista, macrosettori, testo_completo, occhiello, ave") \
            .gte("data", from_date) \
            .lte("data", to_date) \
            .order("data", desc=True) \
            .limit(500) \
            .execute()

        articles = articles_res.data or []

        if client_id:
            client_res = supabase.table("clients").select("*").eq("id", client_id).execute()
            if client_res.data:
                client = client_res.data[0]
                keywords = [
                    k.strip().lower()
                    for k in (client.get("keywords_press") or client.get("keywords") or "").split(",")
                    if k.strip()
                ]
                if keywords:
                    articles = [
                        a for a in articles
                        if any(
                            kw in (a.get("testo_completo") or "").lower()
                            or kw in (a.get("titolo") or "").lower()
                            or kw in (a.get("occhiello") or "").lower()
                            for kw in keywords
                        )
                    ]

        if macro_group_id:
            links = supabase.table("macro_group_links") \
                .select("official_macro_id") \
                .eq("macro_group_id", macro_group_id) \
                .execute()
            official_ids = [l["official_macro_id"] for l in (links.data or [])]
            if official_ids:
                macros = supabase.table("official_macrosectors") \
                    .select("name") \
                    .in_("id", official_ids) \
                    .execute()
                macro_names = [m["name"] for m in (macros.data or [])]
                articles = [
                    a for a in articles
                    if a.get("macrosettori") and any(
                        m.strip() in macro_names
                        for m in a["macrosettori"].split(",")
                    )
                ]

        if sort == "ave":
            articles = sorted(articles, key=lambda a: float(a.get("ave") or 0), reverse=True)
        articles = articles[:50]  # max 50 after filtering
        return {"articles": articles}

    except Exception as e:
        return {"error": str(e)}


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
        supabase.table("shared_reports").insert({
            "token": token,
            "filters": {"article_ids": req.article_ids},
            "expires_at": expires_at,
        }).execute()
        return {"token": token}
    except Exception as e:
        return {"error": str(e)}


@app.get("/share/{token}")
async def read_share(token: str):
    try:
        now = datetime.now(timezone.utc).isoformat()
        row = supabase.table("shared_reports") \
            .select("*") \
            .eq("token", token) \
            .gt("expires_at", now) \
            .execute()

        if not row.data:
            return PlainTextResponse("Link scaduto o non trovato.", status_code=404)

        f           = row.data[0]["filters"]
        article_ids = f.get("article_ids", [])

        if not article_ids:
            return PlainTextResponse("Nessun articolo salvato in questo link.", status_code=404)

        res = supabase.table("articles") \
            .select("id, titolo, testata, data, giornalista, macrosettori, testo_completo") \
            .in_("id", article_ids) \
            .execute()

        id_order = {aid: i for i, aid in enumerate(article_ids)}
        articles = sorted(res.data or [], key=lambda a: id_order.get(a["id"], 9999))
        lines = []
        lines.append("ARCHIVIO MAIM - " + str(len(articles)) + " articoli")
        lines.append("")
        for i, a in enumerate(articles, 1):
            lines.append("---")
            lines.append("[" + str(i) + "] " + (a.get("titolo") or "N/D"))
            lines.append("Testata: " + (a.get("testata") or "N/D") + " | Data: " + (a.get("data") or "N/D") + " | Giornalista: " + (a.get("giornalista") or "N/D"))
            lines.append("Settori: " + (a.get("macrosettori") or "N/D"))
            lines.append("")
            lines.append(a.get("testo_completo") or "Testo non disponibile")
            lines.append("")

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
        return {
            "ultimi_articoli": res.data,
            "totale_articoli": total.count,
            "articoli_oggi":   len(oggi),
            "ultima_data":     last_date,
            "clienti":         clients.data,
        }
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
        keywords    = [
            k.strip().lower()
            for k in (client_data.get("keywords") or "").split(",")
            if k.strip()
        ]

        articles_res = supabase.table("articles").select(
            "id, testata, data, giornalista, occhiello, titolo, sottotitolo, "
            "testo_completo, macrosettori, tipologia_articolo, tone, "
            "dominant_topic, reputational_risk, political_risk, ave, tipo_fonte"
        ).gte("data", from_date).lte("data", to_date).order("data", desc=True).execute()

        all_articles = articles_res.data or []

        if keywords:
            filtered = [
                a for a in all_articles
                if any(
                    kw in (a.get("testo_completo") or "").lower() or
                    kw in (a.get("titolo") or "").lower() or
                    kw in (a.get("occhiello") or "").lower()
                    for kw in keywords
                )
            ]
        else:
            filtered = all_articles

        return {"client": client_data, "articles": filtered, "total": len(filtered)}
    except HTTPException:
        raise
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/articles")
async def get_articles(
    from_date: Optional[str] = None,
    to_date:   Optional[str] = None,
    testata:   Optional[str] = None,
    limit:     int           = 50,
    sort:      Optional[str] = None,
):
    try:
        query = supabase.table("articles").select(
            "id, titolo, testata, data, occhiello, giornalista, tone, dominant_topic, macrosettori, ave"
        )
        if from_date: query = query.gte("data", from_date)
        if to_date:   query = query.lte("data", to_date)
        if testata:   query = query.eq("testata", testata)
        if sort == "ave":
            res = query.order("ave", desc=True).limit(limit).execute()
        else:
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
        if res.data:
            return res.data[0]
        return {"success": True}
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


@app.post("/api/article/{article_id}/split-giornalista")
async def split_giornalista(article_id: str):
    """Duplica un articolo con doppia firma, uno per ogni giornalista separato da virgola."""
    try:
        res = supabase.table("articles").select("*").eq("id", article_id).execute()
        if not res.data:
            raise HTTPException(status_code=404, detail="Articolo non trovato")
        original = res.data[0]
        giornalista = original.get("giornalista", "") or ""
        nomi = [n.strip() for n in giornalista.split(",") if n.strip()]
        if len(nomi) < 2:
            raise HTTPException(status_code=400, detail="Il campo giornalista non contiene più di un nome separato da virgola")
        # Aggiorna originale col primo nome
        supabase.table("articles").update({"giornalista": nomi[0]}).eq("id", article_id).execute()
        # Crea copie per gli altri nomi
        created = []
        for nome in nomi[1:]:
            copy = {k: v for k, v in original.items() if k != "id"}
            copy["giornalista"] = nome
            r = supabase.table("articles").insert(copy).execute()
            if r.data:
                created.append(r.data[0].get("id"))
        return {"success": True, "nomi": nomi, "created_ids": created}
    except HTTPException:
        raise
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
            "name":             data.name.strip(),
            "keywords":         data.keywords,
            "keywords_web":     data.keywords_web,
            "sector":           data.sector,
            "description":      data.description,
            "website":          data.website,
            "contact":          data.contact,
            "semantic_topic":   data.semantic_topic,
            "macro_strategici": data.macro_strategici,
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
# FONTI / SORGENTI MONITORATE
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
        res = supabase.table("monitored_sources").insert({
            "name":   data.name,
            "url":    data.url,
            "type":   data.type,
            "active": data.active,
        }).execute()
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
        body   = await request.json()
        active = body.get("active", True)
        supabase.table("monitored_sources").update({"active": active}).eq("id", source_id).execute()
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


# Alias legacy
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
async def get_web_mentions(
    client: Optional[str] = None,
    limit:  int           = 50,
):
    try:
        query = supabase.table("web_mentions") \
            .select("*") \
            .order("published_at", desc=True)

        if client:
            query = query.ilike("matched_client", f"%{client}%")

        res = query.limit(limit).execute()
        return res.data or []
    except Exception as e:
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════════
# MONITOR
# ══════════════════════════════════════════════════════════════════════

@app.post("/api/monitor/run")
async def monitor_run():
    try:
        if run_monitoring is None:
            return {"error": "Monitor non disponibile (import fallito)"}
        result = run_monitoring()
        return result
    except Exception as e:
        return {"error": str(e), "found": 0, "duplicates": 0}


@app.post("/api/monitor/run-historical")
async def monitor_run_historical(req: HistoricalScanRequest):
    try:
        if run_monitoring is None:
            return {"error": "Monitor non disponibile (import fallito)"}
        result = run_monitoring(from_date=req.from_date, to_date=req.to_date)
        return result
    except Exception as e:
        return {"error": str(e), "found": 0, "duplicates": 0}


@app.get("/api/monitor/scan-info")
async def monitor_scan_info():
    try:
        res = supabase.table("monitor_meta") \
            .select("key, value") \
            .in_("key", ["last_daily_scan", "last_historical_scan"]) \
            .execute()
        info = {row["key"]: row["value"] for row in (res.data or [])}
        return {
            "last_daily":      info.get("last_daily_scan"),
            "last_historical": info.get("last_historical_scan"),
        }
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
        res      = query.execute()
        articles = res.data or []
        counter  = Counter(
            a.get("giornalista","") for a in articles
            if a.get("giornalista") and a["giornalista"].lower() not in ("redazione","")
        )
        return {
            "journalists":    [{"name": n, "count": c} for n, c in counter.most_common(50)],
            "total_articles": len(articles),
        }
    except Exception as e:
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════════
# PITCH ADVISOR
# ══════════════════════════════════════════════════════════════════════

@app.post("/api/pitch")
async def pitch_endpoint(
    message:   str = Form(...),
    client_id: str = Form(""),
    history:   str = Form("[]"),
):
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
# AVVIO
# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))