"""
api/chat.py — SPIZ Intelligence Core

STRATEGIA TOKEN:
- Conteggi, liste autori/testate, AVE → risolti DIRETTAMENTE su Supabase (0 token OpenAI)
- Lettura, analisi, report → OpenAI con budget stretto (max 12k token totali)
- Modello: gpt-4o-mini per query semplici, gpt-4o per analisi complesse
"""

import os
import openai
import tiktoken
import re
from datetime import date, timedelta
from services.database import supabase
from openai import OpenAI

# ─── CONFIG ────────────────────────────────────────────────────────────────────
MODEL_SMART         = "gpt-4o"
MODEL_FAST          = "gpt-4o-mini"
MAX_CONTEXT_TOKENS  = 12_000
MAX_RESPONSE_TOKENS = 1_000
BASE_URL            = os.getenv("APP_BASE_URL", "https://tua-app.replit.app")

enc = tiktoken.encoding_for_model("gpt-4o")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def _tokens(text: str) -> int:
    return len(enc.encode(text))

# ─── MEMORIA CONVERSAZIONE ──────────────────────────────────────────────────────
_sessions: dict[str, list[dict]] = {}

def get_history(session_id: str) -> list[dict]:
    return _sessions.setdefault(session_id, [])

def save_turn(session_id: str, question: str, answer: str):
    h = get_history(session_id)
    h.append({"role": "user",      "content": question})
    h.append({"role": "assistant", "content": answer})

def trim_history(history: list[dict], budget: int = 3_000) -> list[dict]:
    out, used = [], 0
    for msg in reversed(history):
        t = _tokens(msg["content"])
        if used + t > budget:
            break
        out.insert(0, msg)
        used += t
    return out

def reset_session(session_id: str):
    _sessions[session_id] = []

# ─── CLIENTI ───────────────────────────────────────────────────────────────────
def load_client(client_id: str) -> dict | None:
    try:
        res = supabase.table("clients").select("*").eq("id", client_id).execute()
        if res.data:
            return res.data[0]
        res = supabase.table("clients").select("*").ilike("name", client_id).execute()
        return res.data[0] if res.data else None
    except Exception:
        return None

def list_clients() -> list[dict]:
    try:
        res = supabase.table("clients").select("id, name, keywords, semantic_topic").execute()
        return res.data or []
    except Exception:
        return []

def parse_keywords(raw: str) -> list[str]:
    if not raw:
        return []
    sep = "," if "," in raw else "\n"
    return [k.strip().lower() for k in raw.split(sep) if k.strip()]

# ─── RANGE TEMPORALE MIGLIORATA ─────────────────────────────────────────────────
def date_range_from_question(question: str) -> tuple[str | None, str | None]:
    """
    Interpreta la domanda per estrarre un intervallo di date.
    Restituisce (date_from, date_to) o (None, None) per "tutti gli articoli".
    """
    q = question.lower()
    today = date.today()

    # --- RICHIESTA TOTALE (nessun filtro data) ---
    if re.search(r'tutti (gli )?articoli|totali|complessivi|nell\'?intero archivio|da sempre|in totale|nell\'archivio|in archivio|nel database', q):
        return None, None

    # --- OGGI / GIORNO SPECIFICO ---
    if "oggi" in q:
        return str(today), str(today)
    if "ieri" in q:
        d = today - timedelta(days=1)
        return str(d), str(d)

    # --- ULTIMI X GIORNI ---
    match = re.search(r'ultim[oi]\s*(\d+)\s*giorn?i?', q)
    if match:
        days = int(match.group(1))
        start = today - timedelta(days=days)
        return str(start), str(today)

    # --- ULTIMI X MESI / ULTIMO MESE ---
    match = re.search(r'ultim[oi]\s*(\d+)\s*mesi', q)
    if match:
        months = int(match.group(1))
        start = today - timedelta(days=30 * months)
        return str(start), str(today)
    if re.search(r'(ultimo mese|nell\'ultimo mese|quest\'ultimo mese)', q):
        start = today - timedelta(days=30)
        return str(start), str(today)

    # --- ULTIMI X ANNI / ULTIMO ANNO ---
    match = re.search(r'ultim[oi]\s*(\d+)\s*ann?i?', q)
    if match:
        years = int(match.group(1))
        start = today - timedelta(days=365 * years)
        return str(start), str(today)
    if re.search(r'(ultimo anno|nell\'ultimo anno|quest\'ultimo anno)', q):
        start = today - timedelta(days=365)
        return str(start), str(today)

    # --- ULTIMO TRIMESTRE ---
    if re.search(r'(ultimo trimestre|nell\'ultimo trimestre|quest\'ultimo trimestre)', q):
        start = today - timedelta(days=90)
        return str(start), str(today)

    # --- ULTIMI 6 MESI ---
    if re.search(r'ultimi 6 mesi|ultimi sei mesi|negli ultimi 6 mesi', q):
        start = today - timedelta(days=180)
        return str(start), str(today)

    # --- SETTIMANA SCORSA ---
    if "settimana scorsa" in q:
        end = today - timedelta(days=today.weekday() + 1)
        start = end - timedelta(days=6)
        return str(start), str(end)

    # --- QUESTA SETTIMANA ---
    if "questa settimana" in q:
        start = today - timedelta(days=today.weekday())
        return str(start), str(today)

    # --- QUESTO MESE ---
    if "questo mese" in q:
        start = today.replace(day=1)
        return str(start), str(today)

    # --- ULTIMI 7 GIORNI ---
    if "ultimi 7" in q or "7 giorni" in q:
        start = today - timedelta(days=7)
        return str(start), str(today)
    if "ultimi 30" in q or "30 giorni" in q:
        start = today - timedelta(days=30)
        return str(start), str(today)

    # --- FALLBACK: nessun range specificato → ultimi 30 giorni ---
    start = today - timedelta(days=30)
    return str(start), str(today)

# ─── KEYWORD DALLA DOMANDA ──────────────────────────────────────────────────────
_STOPWORDS = {
    "di","il","la","lo","le","i","un","una","è","e","per","che","con","da","in",
    "su","non","mi","si","ho","ha","hai","sono","fare","del","della","degli","delle",
    "dei","gli","questo","questa","questi","queste","qual","quale","quali","dammi",
    "voglio","mostra","analizza","cerca","quanti","quante","chi","cosa","come",
    "quando","dove","perché","tutti","tutte","tutto","anche","però","ancora","già",
    "sempre","mai","molto","poco","tanti","tante","alcuni","alcune","ogni","oppure",
    "oggi","ieri","settimana","mese","articoli","articolo","news","elenco","lista",
    "hanno","scritto","hanno","firma","firmati","senza","quelli","quali",
}

def keywords_from_question(question: str) -> list[str]:
    return [
        w for w in question.lower().split()
        if len(w) > 3 and w not in _STOPWORDS
    ][:5]

# ─── CLASSIFICAZIONE INTENT ─────────────────────────────────────────────────────
INTENTS = {
    "totale": [
        "quanti articoli ci sono in totale", "conta tutti gli articoli", "totale articoli",
        "numeri totali", "consistenza archivio", "quanti articoli abbiamo", "numeri complessivi",
        "quanti articoli nel database", "quanti nel database", "nell'archivio", "in archivio",
        "nel database", "nell'intero archivio", "archivio completo", "tutti gli articoli"
    ],
    "conta": [
        "quanti", "conta ", "numero di", "quante volte", "frequenza",
        "quante testate", "quanti giornalisti", "quanti articoli", "quante notizie",
        "firmati", "non firmati", "senza firma", "con firma", "anonimi",
    ],
    "ave": [
        "ave", "valore economico", "copertura economica", "quanto vale", "impatto economico",
    ],
    "autore": [
        "chi ha scritto", "giornalista", "giornalisti", "autore", "autori",
        "firma", "firme", "chi parla", "elenco giornalisti", "elenco autori",
        "lista giornalisti", "lista autori", "chi scrive",
    ],
    "fonte": [
        "quali testate", "quale giornale", "quali fonti", "elenco testate",
        "lista testate", "da quali giornali",
    ],
    "rischio": [
        "rischio", "reputational", "political risk", "pericoloso", "minaccia", "alert",
    ],
    "leggi": [
        "leggi", "mostrami il testo", "testo integrale", "voglio leggere",
        "testo completo", "articolo intero", "fammelo leggere",
    ],
    "analisi": [
        "analizza", "analisi", "confronta", "trend", "andamento",
        "sintesi", "riassumi", "paragona", "cosa emerge", "cosa dicono",
    ],
    "report": [
        "report", "rassegna", "panoramica", "dammi un quadro", "sommario", "briefing",
    ],
}

def classify_intent(question: str) -> str:
    q = question.lower()
    for intent, triggers in INTENTS.items():
        if any(t in q for t in triggers):
            return intent
    return "generico"

# ─── FILTRI DB ─────────────────────────────────────────────────────────────────
def extract_db_filters(question: str) -> dict:
    q, f = question.lower(), {}
    if "negativ" in q:    f["tone"] = "Negative"
    elif "positiv" in q:  f["tone"] = "Positive"
    elif "neutro" in q:   f["tone"] = "Neutral"
    if "rischio alto" in q or "alto rischio" in q:
        f["reputational_risk"] = "Alto"
    elif "rischio medio" in q:
        f["reputational_risk"] = "Medio"
    return f

# ─── EMBEDDING E RICERCA VETTORIALE ────────────────────────────────────────────

def generate_embedding(text: str) -> list[float] | None:
    """Genera un embedding per il testo usando OpenAI"""
    if not text or len(text.strip()) == 0:
        return None
    try:
        response = client.embeddings.create(
            model="text-embedding-ada-002",
            input=text[:8000]  # tronca per sicurezza
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"Errore generazione embedding: {e}")
        return None

def vector_search_articles(
    query_text: str,
    date_from: str | None,
    date_to: str | None,
    extra_filters: dict | None = None,
    match_threshold: float = 0.7,
    match_count: int = 15
) -> list[dict]:
    """
    Cerca articoli per similarità semantica usando Supabase pgvector
    """
    embedding = generate_embedding(query_text)
    if not embedding:
        print("Impossibile generare embedding per la query")
        return []

    try:
        # Chiamata RPC con date opzionali
        params = {
            "query_embedding": embedding,
            "match_threshold": match_threshold,
            "match_count": match_count,
            "date_from": date_from,
            "date_to": date_to
        }
        response = supabase.rpc("match_articles", params).execute()

        results = response.data
        # Applica filtri aggiuntivi (tone, rischio, ecc.) lato client
        if extra_filters:
            filtered = []
            for r in results:
                match = True
                for col, val in extra_filters.items():
                    if r.get(col) != val:
                        match = False
                        break
                if match:
                    filtered.append(r)
            results = filtered

        return results
    except Exception as e:
        print(f"Errore nella ricerca vettoriale: {e}")
        return []

# ═══════════════════════════════════════════════════════════════════════════════
# QUERY BASE LEGGERA — solo metadati, 0 testo, usata dai resolver diretti
# ═══════════════════════════════════════════════════════════════════════════════
FIELDS_LIGHT = (
    "id, testata, data, titolo, giornalista, autore, "
    "tone, reputational_risk, dominant_topic, macrosettori, ave"
)

def _search_fields() -> list[str]:
    return ["titolo", "testo_completo", "macrosettori", "dominant_topic", "occhiello"]

def _base_query(
    date_from: str | None,
    date_to: str | None,
    keywords: list[str],
    filters: dict,
    limit: int = 2000,
) -> list[dict]:
    """
    Query base per recuperare articoli con metadati (senza testo).
    Se date_from e date_to sono None, non applica filtri temporali.
    """
    seen, results = set(), []

    def _add(rows):
        for r in (rows or []):
            if r["id"] not in seen:
                seen.add(r["id"])
                results.append(r)

    # Costruzione query base
    query = supabase.table("articles").select(FIELDS_LIGHT)

    # Applica filtri data solo se specificati
    if date_from:
        query = query.gte("data", date_from)
    if date_to:
        query = query.lte("data", date_to)

    # Applica filtri aggiuntivi
    for col, val in filters.items():
        query = query.eq(col, val)

    # Se ci sono keywords, cerca per ogni keyword
    if keywords:
        for kw in keywords:
            for field in _search_fields():
                try:
                    q_copy = query.ilike(field, f"%{kw}%")
                    _add(q_copy.limit(limit).execute().data)
                except Exception:
                    pass
    else:
        # Senza keyword, prendi tutti gli articoli nel range
        try:
            q_copy = query.order("data", desc=True)
            _add(q_copy.limit(limit).execute().data)
        except Exception:
            pass

    return results

# ═══════════════════════════════════════════════════════════════════════════════
# RESOLVER DIRETTI — zero token OpenAI
# ═══════════════════════════════════════════════════════════════════════════════

def _autore_str(r: dict) -> str:
    return (r.get("giornalista") or r.get("autore") or "").strip()

def _is_firmato(r: dict) -> bool:
    return _autore_str(r) not in ("", "N.D.", "N/D", "Redazione", "Autore non indicato")

def resolve_totale(rows: list[dict], date_from: str | None, date_to: str | None) -> str:
    """Resolve per richieste di totale complessivo"""
    if not rows:
        return "📭 Nessun articolo presente nel database."

    by_testata: dict[str, int] = {}
    for r in rows:
        t = r.get("testata", "N/D")
        by_testata[t] = by_testata.get(t, 0) + 1

    table = "| Testata | Articoli |\n|---------|----------|\n"
    for testata, n in sorted(by_testata.items(), key=lambda x: -x[1]):
        table += f"| {testata} | {n} |\n"

    periodo = "TOTALE ARCHIVIO" if date_from is None else f"dal {date_from} al {date_to}"
    return (
        f"**Articoli presenti in archivio: {len(rows)}**\n\n"
        f"{table}\n"
        f"**Totale: {len(rows)} articoli su {len(by_testata)} testate**"
    )

def resolve_conta(rows: list[dict], question: str, date_from: str | None, date_to: str | None) -> str:
    if not rows:
        return "📭 Nessun articolo trovato per i criteri specificati."

    periodo = f"dal {date_from} al {date_to}" if date_from and date_to else "TOTALE ARCHIVIO"
    q       = question.lower()

    # ── Firmati vs non firmati ────────────────────────────────────────────────
    if any(t in q for t in ("firma", "firmati", "non firmati", "senza firma", "con firma", "anonimo", "anonimi")):
        firmati  = [r for r in rows if _is_firmato(r)]
        non_firm = [r for r in rows if not _is_firmato(r)]
        pct      = len(firmati) / len(rows) * 100 if rows else 0
        return (
            f"**Articoli {periodo}: {len(rows)} totali**\n\n"
            f"✍️ **Con firma:** {len(firmati)}\n"
            f"📋 **Senza firma / Redazione:** {len(non_firm)}\n\n"
            f"*Firmati: {pct:.1f}% del totale*"
        )

    # ── Conteggio per testata (default) ──────────────────────────────────────
    by_testata: dict[str, int] = {}
    for r in rows:
        t = r.get("testata", "N/D")
        by_testata[t] = by_testata.get(t, 0) + 1

    table = "| Testata | Articoli |\n|---------|----------|\n"
    for testata, n in sorted(by_testata.items(), key=lambda x: -x[1]):
        table += f"| {testata} | {n} |\n"

    return (
        f"**Articoli trovati {periodo}: {len(rows)}**\n\n"
        f"{table}\n"
        f"**Totale: {len(rows)} articoli su {len(by_testata)} testate**"
    )

def resolve_autore(rows: list[dict], date_from: str | None, date_to: str | None) -> str:
    if not rows:
        return "📭 Nessun articolo trovato."

    periodo = f"dal {date_from} al {date_to}" if date_from and date_to else "TOTALE ARCHIVIO"

    by_autore: dict[str, list[str]] = {}
    for r in rows:
        autore  = _autore_str(r) or "Senza firma"
        entry   = f"*{r.get('testata','N/D')}* — {r.get('titolo','N/D')}"
        by_autore.setdefault(autore, []).append(entry)

    lines = [f"**Giornalisti/Autori {periodo} ({len(rows)} articoli totali):**\n"]
    for autore, titoli in sorted(by_autore.items(), key=lambda x: -len(x[1])):
        lines.append(f"\n**{autore}** ({len(titoli)} art.)")
        for t in titoli[:5]:
            lines.append(f"  • {t}")
        if len(titoli) > 5:
            lines.append(f"  ... e altri {len(titoli)-5} articoli")

    return "\n".join(lines)

def resolve_fonte(rows: list[dict], date_from: str | None, date_to: str | None) -> str:
    if not rows:
        return "📭 Nessun articolo trovato."

    periodo = f"dal {date_from} al {date_to}" if date_from and date_to else "TOTALE ARCHIVIO"
    by_testata: dict[str, list[dict]] = {}
    for r in rows:
        by_testata.setdefault(r.get("testata", "N/D"), []).append(r)

    lines = [f"**Testate {periodo} ({len(rows)} articoli totali):**\n"]
    for testata, arts in sorted(by_testata.items(), key=lambda x: -len(x[1])):
        autori = {_autore_str(a) for a in arts if _is_firmato(a)}
        lines.append(f"\n**{testata}** — {len(arts)} articoli")
        if autori:
            lines.append(f"  Firme: {', '.join(sorted(autori))}")

    return "\n".join(lines)

def resolve_ave(rows: list[dict], date_from: str | None, date_to: str | None) -> str:
    if not rows:
        return "📭 Nessun articolo trovato."

    periodo = f"dal {date_from} al {date_to}" if date_from and date_to else "TOTALE ARCHIVIO"
    table   = "| Testata | Titolo | AVE (€) |\n|---------|--------|---------|\n"
    totale, senza = 0.0, 0

    for r in sorted(rows, key=lambda x: float(x.get("ave") or 0), reverse=True)[:50]:
        testata = r.get("testata", "N/D")
        titolo  = (r.get("titolo") or "N/D")[:55]
        ave_raw = r.get("ave")
        try:
            ave_val  = float(ave_raw)
            totale  += ave_val
            table   += f"| {testata} | {titolo} | {ave_val:,.0f} |\n"
        except (TypeError, ValueError):
            senza += 1
            table += f"| {testata} | {titolo} | N/D |\n"

    footer = f"\n**AVE TOTALE {periodo}: €{totale:,.0f}**"
    if senza:
        footer += f"\n*{senza} articoli senza valore AVE esclusi dal totale.*"
    return table + footer

# ═══════════════════════════════════════════════════════════════════════════════
# BUILD CONTEXT PER OPENAI
# ═══════════════════════════════════════════════════════════════════════════════
def build_context(articles: list[dict], budget: int) -> tuple[str, int]:
    context, used, n = "", 0, 0
    for a in articles:
        art_id = a.get("id", "")
        link   = f"{BASE_URL}/articolo/{art_id}"
        chunk  = (
            f"╔══ ARTICOLO [{n+1}] ══════════════════════\n"
            f"  TESTATA   : {a.get('testata','N/D')}  |  DATA: {a.get('data','N/D')}\n"
            f"  AUTORE    : {a.get('giornalista') or a.get('autore','N/D')}\n"
            f"  TITOLO    : {a.get('titolo','N/D')}\n"
            f"  OCCHIELLO : {a.get('occhiello','')}\n"
            f"  TONE      : {a.get('tone','N/D')}  |  RISCHIO: {a.get('reputational_risk','N/D')}\n"
            f"  TOPIC     : {a.get('dominant_topic','N/D')}  |  SETTORI: {a.get('macrosettori','N/D')}\n"
            f"  AVE (€)   : {a.get('ave','N/D')}\n"
            f"  LINK      : {link}\n"
            f"  TESTO     :\n{a.get('testo_completo','')}\n"
            f"╚══════════════════════════════════════════\n\n"
        )
        t = _tokens(chunk)
        if used + t > budget:
            break
        context += chunk
        used += t
        n += 1
    return f"[{n}/{len(articles)} articoli | {used:,} token]\n\n" + context, n

# ─── SYSTEM PROMPT (MIGLIORATO) ──────────────────────────────────────────────
_INTENT_GUIDE = {
    "leggi":   "MODALITÀ LETTURA: restituisci il TESTO INTEGRALE dell'articolo più pertinente senza tagli.\nIntestazione: **[TESTATA] — [TITOLO]** | di [AUTORE] | [📰 Leggi](LINK)",
    "rischio": "MODALITÀ RISK ALERT: elenca gli articoli con rischio reputazionale o politico elevato. Usa il seguente formato per ogni articolo:\n\n---\n### 📰 [TESTATA]\n**[TITOLO]**  \n*[AUTORE]*\n\n[Spiegazione del rischio o motivo]\n\n[→ Leggi l'articolo](URL)\n---\n\nSe ci sono più articoli, separali con una linea orizzontale. Alla fine, aggiungi una breve sintesi dei rischi emergenti.",
    "analisi": "MODALITÀ ANALISI: estrai temi, posizioni, tendenze. Confronta fonti. Usa un formato chiaro con paragrafi e, se utile, elenchi puntati. Per ogni articolo citato, includi testata, titolo, autore e link.",
    "report":  "MODALITÀ REPORT: rassegna professionale con titoli di sezione (H2/H3). Per ogni articolo, riporta testata in grassetto, titolo, autore, un breve estratto/sintesi e il link. Alla fine, un sommario dei punti chiave.",
    "generico":"Rispondi in modo chiaro e strutturato. Per ogni articolo menzionato, usa il formato:\n\n### 📰 [TESTATA]\n**[TITOLO]**  \n*[AUTORE]*\n\n[Commento o estratto]\n\n[→ Leggi l'articolo](URL)\n\nSe appropriato, aggiungi una sintesi finale.",
}

def build_system_prompt(intent: str, client: dict | None, date_from: str | None, date_to: str | None) -> str:
    if date_from and date_to:
        periodo = f"dal {date_from} al {date_to}" if date_from != date_to else f"del {date_from}"
    else:
        periodo = "TUTTO L'ARCHIVIO"

    client_block = ""
    if client:
        client_block = (
            f"\nCLIENTE: **{client.get('name')}** | "
            f"Keywords: {client.get('keywords','—')} | "
            f"Settore: {client.get('semantic_topic','—')}\n"
        )
    return (
        "Sei SPIZ, analista professionale di rassegna stampa.\n"
        "• Cita SEMPRE: Testata, Autore, Titolo, Link [📰 Leggi](URL) per ogni articolo.\n"
        "• Non inventare dati assenti nel contesto.\n"
        "• Sii preciso e diretto. Zero preamboli.\n\n"
        f"PERIODO: {periodo}{client_block}\n"
        f"{_INTENT_GUIDE.get(intent, _INTENT_GUIDE['generico'])}"
    )

# ═══════════════════════════════════════════════════════════════════════════════
# FUNZIONE PRINCIPALE
# ═══════════════════════════════════════════════════════════════════════════════
def ask_spiz(
    question:   str,
    session_id: str = "default",
    client_id:  str | None = None,
) -> str:

    q_low = question.strip().lower()

    # ── Comandi speciali ──────────────────────────────────────────────────────
    if q_low in ("reset", "/reset", "nuova conversazione"):
        reset_session(session_id)
        return "🔄 Conversazione azzerata."

    if q_low in ("clienti", "/clienti", "lista clienti"):
        clients = list_clients()
        if not clients:
            return "Nessun cliente nel database."
        rows = "\n".join(
            f"• **{c['name']}** (ID: `{c['id']}`)\n"
            f"  Keywords: {c.get('keywords','—')} | Settore: {c.get('semantic_topic','—')}"
            for c in clients
        )
        return f"**Clienti registrati ({len(clients)}):**\n\n{rows}"

    # ── Domande sulla sessione corrente ───────────────────────────────────────
    if any(t in q_low for t in ("in memoria", "hai in memoria", "hai caricato", "nel contesto", "quanti ne hai")):
        n_turni = len(get_history(session_id)) // 2
        if n_turni == 0:
            return (
                "Non ho ancora nessun scambio in memoria per questa sessione.\n"
                "La memoria si costruisce man mano che fai domande."
            )
        return (
            f"In questa sessione ho risposto a **{n_turni} domande**.\n\n"
            "Nota: non 'carico' articoli fissi in memoria — ogni domanda interroga "
            "il database in tempo reale, così hai sempre i dati aggiornati."
        )

    # ── Carica cliente ────────────────────────────────────────────────────────
    client = None
    if client_id:
        client = load_client(client_id)
        if not client:
            return f"⚠️ Cliente '{client_id}' non trovato nel database."

    # ── Keyword ───────────────────────────────────────────────────────────────
    if client:
        keywords = parse_keywords(client.get("keywords", ""))
        topic    = client.get("semantic_topic", "")
        if topic:
            keywords.append(topic.lower())
    else:
        keywords = keywords_from_question(question)

    # ── Date e filtri ─────────────────────────────────────────────────────────
    date_from, date_to = date_range_from_question(question)
    db_filters         = extract_db_filters(question)
    intent             = classify_intent(question)

    # ══════════════════════════════════════════════════════════════════════════
    # PERCORSO 1 — Resolver diretto su DB (0 token OpenAI)
    # ══════════════════════════════════════════════════════════════════════════
    if intent in ("totale", "conta", "autore", "fonte", "ave"):
        rows = _base_query(date_from, date_to, keywords, db_filters, limit=2000)
        if intent == "totale":  return resolve_totale(rows, date_from, date_to)
        if intent == "conta":   return resolve_conta(rows, question, date_from, date_to)
        if intent == "autore":  return resolve_autore(rows, date_from, date_to)
        if intent == "fonte":   return resolve_fonte(rows, date_from, date_to)
        if intent == "ave":     return resolve_ave(rows, date_from, date_to)

    # ══════════════════════════════════════════════════════════════════════════
    # PERCORSO 2 — OpenAI (analisi, report, lettura, rischio, generico)
    # ══════════════════════════════════════════════════════════════════════════
    articles = vector_search_articles(
        query_text=question,
        date_from=date_from,
        date_to=date_to,
        extra_filters=db_filters,
        match_threshold=0.7,
        match_count=15
    )

    if not articles:
        periodo = f"dal {date_from} al {date_to}" if date_from and date_to else "nell'archivio"
        chi     = f" per il cliente '{client['name']}'" if client else ""
        return f"📭 Nessun articolo trovato {periodo}{chi}."

    history    = trim_history(get_history(session_id), budget=2_000)
    ctx_budget = MAX_CONTEXT_TOKENS - MAX_RESPONSE_TOKENS - 1_500
    db_context, _ = build_context(articles, budget=ctx_budget)

    model         = MODEL_SMART if intent in ("analisi", "report") else MODEL_FAST
    system_prompt = build_system_prompt(intent, client, date_from, date_to)
    user_message  = f"DATABASE:\n{db_context}\n\nRICHIESTA: {question}"

    messages = [
        {"role": "system", "content": system_prompt},
        *history,
        {"role": "user",   "content": user_message},
    ]

    try:
        response = openai.chat.completions.create(
            model       = model,
            messages    = messages,
            temperature = 0,
            max_tokens  = MAX_RESPONSE_TOKENS,
        )
        answer = response.choices[0].message.content
        save_turn(session_id, question, answer)
        return answer

    except openai.RateLimitError:
        return (
            "⚠️ Limite token OpenAI raggiunto. "
            "Aspetta 60 secondi e riprova, oppure fai una domanda più specifica."
        )
    except openai.APIError as e:
        return f"⚠️ Errore API OpenAI: {e}"
    except Exception as e:
        return f"⚠️ Errore inatteso: {e}"


# ─── TEST DA TERMINALE ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    client_arg = sys.argv[1] if len(sys.argv) > 1 else None
    session    = "test"

    if client_arg:
        print(f"🔍 Modalità cliente: {client_arg}\n")
    print("SPIZ pronto. Comandi: 'reset', 'clienti'. Ctrl+C per uscire.\n")

    while True:
        try:
            q = input("Tu: ").strip()
            if not q:
                continue
            print(f"\nSPIZ: {ask_spiz(q, session_id=session, client_id=client_arg)}\n")
        except KeyboardInterrupt:
            print("\nArrivederci.")
            break