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
    titolo: Optional[str] = None
    testata: Optional[str] = None
    data: Optional[str] = None
    giornalista: Optional[str] = None
    occhiello: Optional[str] = None
    sottotitolo: Optional[str] = None
    testo_completo: Optional[str] = None
    tone: Optional[str] = None
    reputational_risk: Optional[str] = None
    political_risk: Optional[str] = None
    dominant_topic: Optional[str] = None
    macrosettori: Optional[str] = None
    tipologia_articolo: Optional[str] = None
    ave: Optional[float] = None
    tipo_fonte: Optional[str] = None

# --- ROTTE NAVIGAZIONE ---
@app.get("/")
async def index():
    """Dashboard quantitativa con clienti e articoli"""
    return FileResponse('web/index.html')

@app.get("/chat")
async def chat_page():
    """Interfaccia chat qualitativa"""
    return FileResponse('web/chat.html')

@app.get("/clients")
async def clients_page():
    """Gestione clienti"""
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
    """Restituisce la lista clienti"""
    try:
        res = supabase.table("clients").select("id, name, keywords, semantic_topic").execute()
        return res.data or []
    except Exception as e:
        return []

# --- ENDPOINT DASHBOARD QUANTITATIVA ---

@app.get("/api/dashboard-stats")
async def get_dashboard_stats():
    """Statistiche generali (totali)"""
    try:
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
    """Menzioni clienti per oggi"""
    try:
        today    = date.today().isoformat()
        clients  = supabase.table("clients").select("*").execute().data or []
        articles = supabase.table("articles").select("titolo, testo_completo").eq("data", today).execute().data or []
        results  = []
        for client in clients:
            keywords = [k.strip().lower() for k in (client.get('keywords') or "").split(',') if k.strip()]
            count    = sum(
                1 for art in articles
                if any(
                    kw in f"{art.get('titolo','')} {art.get('testo_completo','')}".lower()
                    for kw in keywords
                )
            )
            results.append({"name": client['name'], "today": count, "id": client.get('id'), "keywords": client.get('keywords')})
        return results
    except Exception:
        return []

@app.get("/api/today-stats")
async def get_today_stats():
    """Statistiche dettagliate di oggi: articoli, giornalisti, testate"""
    today = date.today().isoformat()
    try:
        res = supabase.table("articles").select("id, giornalista, testata, titolo").eq("data", today).execute()
        articles_today = res.data or []

        total_today = len(articles_today)

        giornalisti_oggi = {}
        for a in articles_today:
            g = a.get('giornalista') or a.get('autore') or ''
            if g and g.strip() not in ("", "N.D.", "Redazione", "Autore non indicato"):
                giornalisti_oggi[g] = giornalisti_oggi.get(g, 0) + 1

        testate_oggi = {}
        for a in articles_today:
            t = a.get('testata', 'N/D')
            testate_oggi[t] = testate_oggi.get(t, 0) + 1

        ultimi_articoli = sorted(articles_today, key=lambda x: x.get('data', ''), reverse=True)[:5]

        return {
            "total_today": total_today,
            "giornalisti": [{"nome": k, "articoli": v} for k, v in sorted(giornalisti_oggi.items(), key=lambda x: -x[1])],
            "testate": [{"nome": k, "articoli": v} for k, v in sorted(testate_oggi.items(), key=lambda x: -x[1])],
            "ultimi_articoli": ultimi_articoli
        }
    except Exception as e:
        print(f"Errore today-stats: {e}")
        return {"total_today": 0, "giornalisti": [], "testate": [], "ultimi_articoli": []}

@app.get("/api/last-upload")
async def get_last_upload():
    """Data dell'ultimo caricamento"""
    try:
        res = supabase.table("articles").select("data").order("data", desc=True).limit(1).execute()
        if res.data:
            return {"last_upload": res.data[0]['data']}
        else:
            return {"last_upload": None}
    except Exception:
        return {"last_upload": None}

# --- ENDPOINT PER LA DASHBOARD CLIENTI INTERATTIVA ---

@app.get("/api/client-articles")
async def get_client_articles(client_id: str, from_date: str, to_date: str):
    """Restituisce gli articoli per un cliente in un intervallo di date (cerca in titolo, occhiello, sottotitolo, testo_completo)"""
    try:
        # Recupera il cliente
        client_res = supabase.table("clients").select("*").eq("id", client_id).execute()
        if not client_res.data:
            return {"error": "Cliente non trovato"}
        client = client_res.data[0]
        keywords = [k.strip().lower() for k in (client.get('keywords') or "").split(',') if k.strip()]

        if not keywords:
            return []

        # Seleziona i campi necessari per la lista (includiamo anche testo_completo per il filtro, ma non lo restituiamo)
        query = supabase.table("articles").select(
            "id, testata, data, titolo, giornalista, occhiello, sottotitolo, testo_completo"
        ).gte("data", from_date).lte("data", to_date)

        articles = query.execute().data or []

        # Filtra in Python: cerca keyword in titolo, occhiello, sottotitolo, testo_completo
        results = []
        for art in articles:
            text = f"{art.get('titolo','')} {art.get('occhiello','')} {art.get('sottotitolo','')} {art.get('testo_completo','')}".lower()
            if any(kw in text for kw in keywords):
                # Rimuovi testo_completo dalla risposta per non appesantire
                art.pop('testo_completo', None)
                results.append(art)

        return results
    except Exception as e:
        print(f"Errore client-articles: {e}")
        return {"error": str(e)}

@app.get("/api/article/{article_id}")
async def get_article(article_id: str):
    """Restituisce un articolo completo per ID"""
    try:
        res = supabase.table("articles").select("*").eq("id", article_id).execute()
        if res.data:
            return res.data[0]
        else:
            return {"error": "Articolo non trovato"}
    except Exception as e:
        return {"error": str(e)}

@app.put("/api/article/{article_id}")
async def update_article(article_id: str, updates: ArticleUpdateSimple):
    try:
        update_data = {k: v for k, v in updates.dict().items() if v is not None}
        if not update_data:
            return {"error": "Nessun campo da aggiornare"}
        print(f"Aggiornamento articolo {article_id} con: {update_data}")  # debug
        res = supabase.table("articles").update(update_data).eq("id", article_id).execute()
        print("Risposta Supabase:", res)  # debug
        if res.data:
            return res.data[0]
        else:
            return {"error": "Articolo non trovato o aggiornamento fallito"}
    except Exception as e:
        print("Eccezione:", e)
        return {"error": str(e)}

@app.delete("/api/article/{article_id}")
async def delete_article(article_id: str):
    """Elimina un articolo"""
    try:
        res = supabase.table("articles").delete().eq("id", article_id).execute()
        if res.data:
            return {"success": True, "message": "Articolo eliminato"}
        else:
            return {"error": "Articolo non trovato"}
    except Exception as e:
        return {"error": str(e)}

# --- ENDPOINT GESTIONE CLIENTI (CRUD) ---

@app.post("/api/clients")
async def create_client(client: dict):
    """Crea un nuovo cliente"""
    try:
        # client deve contenere name, keywords, semantic_topic
        data = {
            "name": client.get("name"),
            "keywords": client.get("keywords", ""),
            "semantic_topic": client.get("semantic_topic", "")
        }
        res = supabase.table("clients").insert(data).execute()
        if res.data:
            return res.data[0]
        else:
            return {"error": "Inserimento fallito"}
    except Exception as e:
        return {"error": str(e)}

@app.put("/api/clients/{client_id}")
async def update_client(client_id: str, client: dict):
    """Aggiorna un cliente esistente"""
    try:
        data = {}
        if "name" in client:
            data["name"] = client["name"]
        if "keywords" in client:
            data["keywords"] = client["keywords"]
        if "semantic_topic" in client:
            data["semantic_topic"] = client["semantic_topic"]
        if not data:
            return {"error": "Nessun campo da aggiornare"}
        res = supabase.table("clients").update(data).eq("id", client_id).execute()
        if res.data:
            return res.data[0]
        else:
            return {"error": "Aggiornamento fallito"}
    except Exception as e:
        return {"error": str(e)}

@app.delete("/api/clients/{client_id}")
async def delete_client(client_id: str):
    """Elimina un cliente"""
    try:
        res = supabase.table("clients").delete().eq("id", client_id).execute()
        if res.data:
            return {"success": True, "message": "Cliente eliminato"}
        else:
            return {"error": "Cliente non trovato"}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)