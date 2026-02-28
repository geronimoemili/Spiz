"""
api/chat.py - SPIZ AI v10
FIXED:
- Report genera testo strutturato vero (non JSON grezzo)
- docx_path prodotto realmente via docx_builder.js
- Intent "quantitative" ora gestisce lista giornalisti correttamente
- Fallback robusto se embedding fallisce
"""

import os
import re
import json
import subprocess
import tempfile
from datetime import date, timedelta
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from openai import OpenAI
from services.database import supabase

ai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

_BUILDER_JS = os.path.join(os.path.dirname(__file__), "docx_builder.js")

DB_COLS = (
    "id, testata, data, giornalista, occhiello, titolo, sottotitolo, "
    "testo_completo, macrosettori, tipologia_articolo, tone, "
    "dominant_topic, reputational_risk, political_risk, ave, tipo_fonte"
)

# ══════════════════════════════════════════════════════════════════════
# PARSING TEMPORALE
# ══════════════════════════════════════════════════════════════════════

_TIME_RULES = [
    (r"oggi|odiern",                                               0),
    (r"ultime?\s*24.?ore|ieri",                                    1),
    (r"ultim[ie]\s*(?:[23]\s*(?:giorn|gg\b|g\b))",                3),
    (r"ultim[ie]\s*(?:[67]\s*(?:giorn|gg\b|g\b)|settiman|7\s*(?:giorn|gg))", 7),
    (r"ultim[ie]\s*(?:15\s*(?:giorn|gg\b)|due\s*settiman)",       15),
    (r"ultim[ie]\s*(?:30\s*(?:giorn|gg\b|g\b)?)\b|ultimo\s*mese|mese\s*scors", 30),
    (r"ultim[ie]\s*(?:[23]\s*mesi|[69]0\s*giorn)",                90),
    (r"ultim[ie]\s*(?:[46]\s*mesi)",                              180),
    (r"ultimo\s*anno|ultim[ie]\s*12\s*mesi",                     365),
]

def _parse_days(msg: str):
    for pattern, days in _TIME_RULES:
        if re.search(pattern, msg.lower()):
            return days
    return None

def _date_range(context: str, message: str):
    days = _parse_days(message)
    if days is None:
        days = {"today": 0, "week": 7, "month": 30, "year": 365}.get(context, 30)
    today = date.today()
    if days == 0:
        return today.isoformat(), today.isoformat()
    return (today - timedelta(days=days)).isoformat(), today.isoformat()


# ══════════════════════════════════════════════════════════════════════
# SEMANTIC SEARCH (pgvector)
# ══════════════════════════════════════════════════════════════════════

def _semantic_search(from_date: str, to_date: str, user_message: str, limit: int = 200):
    try:
        emb = ai.embeddings.create(
            model="text-embedding-3-small",
            input=user_message[:8000],
        ).data[0].embedding

        res = supabase.rpc(
            "match_articles",
            {
                "query_embedding": emb,
                "match_from":      from_date,
                "match_to":        to_date,
                "match_count":     limit,
            }
        ).execute()
        return res.data or []
    except Exception as e:
        print(f"[SPIZ] semantic search error: {e}")
        return []


def _fallback_search(from_date: str, to_date: str, limit: int = 100):
    """Ricerca senza embedding quando pgvector non è disponibile."""
    try:
        res = (supabase.table("articles")
               .select(DB_COLS)
               .gte("data", from_date)
               .lte("data", to_date)
               .order("data", desc=True)
               .limit(limit)
               .execute())
        return res.data or []
    except Exception as e:
        print(f"[SPIZ] fallback search error: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════
# INTENT DETECTION
# ══════════════════════════════════════════════════════════════════════

_QUANT_PATTERNS = [
    r"giornalist[ie]",
    r"chi\s+ha\s+scritto",
    r"quant[ie]\s+(articol|testat|giornalist)",
    r"top\s+\d",
    r"classifica",
    r"più\s+(citati?|attivi?|presenti?)",
]

_REPORT_KW = [
    "report", "profilo mediatico", "sintesi strategica", "analisi completa",
    "criticita", "reputazion", "relazione", "redigi", "elabora", "documento",
]

_DOCX_KW = ["word", "docx", "scarica", "download", "file"]

def _detect_intent(message: str) -> str:
    msg = message.lower()
    if any(kw in msg for kw in _REPORT_KW):
        return "report"
    for pat in _QUANT_PATTERNS:
        if re.search(pat, msg):
            return "quantitative"
    return "quick"

def _wants_docx(message: str) -> bool:
    return any(kw in message.lower() for kw in _DOCX_KW)


# ══════════════════════════════════════════════════════════════════════
# STATISTICHE
# ══════════════════════════════════════════════════════════════════════

def _stats(articles: list) -> dict:
    if not articles:
        return {}
    testate     = Counter(a.get("testata","")      for a in articles if a.get("testata"))
    giornalisti = Counter(a.get("giornalista","")  for a in articles if a.get("giornalista"))
    tones       = Counter(a.get("tone","")         for a in articles if a.get("tone"))
    tone_tot    = sum(tones.values()) or 1
    dates       = [a.get("data","") for a in articles if a.get("data")]
    return {
        "totale":      len(articles),
        "periodo_da":  min(dates) if dates else "",
        "periodo_a":   max(dates) if dates else "",
        "testate":     dict(testate.most_common(20)),
        "giornalisti": dict(giornalisti.most_common(50)),
        "sentiment":   {k: round(v/tone_tot*100) for k,v in tones.items() if k},
    }


# ══════════════════════════════════════════════════════════════════════
# QUICK ANSWER
# ══════════════════════════════════════════════════════════════════════

_QUICK_SYSTEM = """Sei SPIZ, analista mediatico senior di MAIM Public Diplomacy & Media Relations.

REGOLE ASSOLUTE:
1. Rispondi ESCLUSIVAMENTE usando gli articoli forniti nel corpus.
2. Non usare MAI la tua conoscenza generale o inventare fatti.
3. Ogni affermazione deve citare testata e data dell'articolo.
4. Se un'informazione non è nel corpus: "Non ho articoli su questo nel periodo."
5. Italiano professionale corporate. Nessuna emoji.
"""

def _quick_answer(user_message: str, articles: list, stats: dict, history: list = None) -> str:
    lines = []
    for a in articles[:30]:
        testo = (a.get("testo_completo") or "")[:600]
        lines.append(
            f"[{a.get('data','')}] {a.get('testata','')} | {a.get('giornalista','')}\n"
            f"TITOLO: {a.get('titolo','')}\n"
            f"TESTO: {testo}"
        )
    corpus_txt = "\n---\n".join(lines)

    messages = [{"role": "system", "content": f"{_QUICK_SYSTEM}\n\nCORPUS ({len(articles)} articoli):\n{corpus_txt}"}]
    for msg in (history or [])[-10:]:
        if msg.get("role") in ("user","assistant") and msg.get("content"):
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": user_message})

    resp = ai.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        temperature=0.1,
        max_tokens=2000,
    )
    return resp.choices[0].message.content.strip()


# ══════════════════════════════════════════════════════════════════════
# QUANTITATIVE ANSWER (giornalisti, testate, conteggi)
# ══════════════════════════════════════════════════════════════════════

def _quantitative_answer(user_message: str, articles: list, stats: dict) -> str:
    stats_txt = (
        f"TOTALE ARTICOLI: {stats.get('totale',0)}\n"
        f"PERIODO: {stats.get('periodo_da','')} → {stats.get('periodo_a','')}\n"
        f"TOP TESTATE: {', '.join(f'{k}({v})' for k,v in list(stats.get('testate',{}).items())[:15])}\n"
        f"TOP GIORNALISTI: {', '.join(f'{k}({v})' for k,v in list(stats.get('giornalisti',{}).items())[:30])}\n"
        f"SENTIMENT: {', '.join(f'{k}: {v}%' for k,v in stats.get('sentiment',{}).items())}\n"
    )

    resp = ai.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": (
                "Sei SPIZ, analista mediatico. Rispondi con dati precisi basandoti SOLO sulle "
                "statistiche fornite. Italiano professionale, nessuna emoji."
            )},
            {"role": "user", "content": f"STATISTICHE:\n{stats_txt}\n\nDOMANDA: {user_message}"},
        ],
        temperature=0.0,
        max_tokens=1500,
    )
    return resp.choices[0].message.content.strip()


# ══════════════════════════════════════════════════════════════════════
# REPORT STRUTTURATO (MAP + REDUCE)
# ══════════════════════════════════════════════════════════════════════

_MAP_SYSTEM = """Analizza gli articoli e restituisci un JSON con una lista "articoli", 
dove ogni elemento ha:
- testata, data, titolo
- fatti_chiave (array di stringhe, max 3)
- angolo (stringa, angolazione giornalistica)
- criticita (stringa o null)
- rilevanza (1-5)
Rispondi SOLO con JSON valido."""

def _map_batch(batch: list, idx: int):
    lines = []
    for a in batch:
        testo = (a.get("testo_completo") or "")[:1500]
        lines.append(
            f"TESTATA: {a.get('testata')}\nDATA: {a.get('data')}\n"
            f"TITOLO: {a.get('titolo')}\nTESTO: {testo}"
        )
    try:
        resp = ai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": _MAP_SYSTEM},
                {"role": "user", "content": "\n\n".join(lines)},
            ],
            temperature=0.0,
            max_tokens=3000,
            response_format={"type": "json_object"},
        )
        parsed = json.loads(resp.choices[0].message.content)
        items = parsed.get("articoli", parsed) if isinstance(parsed, dict) else parsed
        return idx, items if isinstance(items, list) else []
    except Exception as e:
        print(f"[MAP] batch {idx} error: {e}")
        return idx, []

def _map_articles_parallel(articles: list, batch_size: int = 5, max_workers: int = 4) -> list:
    batches = [articles[i:i+batch_size] for i in range(0, len(articles), batch_size)]
    results = [None] * len(batches)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_map_batch, b, i): i for i, b in enumerate(batches)}
        for f in futures:
            idx, data = f.result()
            results[idx] = data
    out = []
    for r in results:
        if r:
            out.extend(r)
    return out

_REPORT_SYSTEM = """Sei SPIZ, analista senior di MAIM Public Diplomacy & Media Relations.
Produci un report mediatico professionale strutturato con ESATTAMENTE queste sezioni:

## 1. PROFILO MEDIATICO
## 2. INTERVISTE E PRESENZA VERTICI
## 3. TEMI LONGEVI
## 4. NOTIZIE FINANZIARIE E CORPORATE
## 5. GOVERNANCE E MANAGEMENT
## 6. FOCUS TERRITORIALE
## 7. CRITICITÀ REPUTAZIONALI
## 8. ANALISI DEL SENTIMENT
## 9. COMUNICAZIONE ISTITUZIONALE
## 10. SINTESI STRATEGICA

Per ogni sezione cita articoli specifici con testata e data.
Se una sezione non ha dati rilevanti scrivere: "Nessun elemento rilevante nel periodo."
Usa SOLO i dati forniti. Italiano professionale corporate."""

def _reduce_to_report(user_message: str, extracted: list, stats: dict) -> str:
    stats_txt = (
        f"TOTALE ARTICOLI ANALIZZATI: {stats.get('totale',0)}\n"
        f"PERIODO: {stats.get('periodo_da','')} → {stats.get('periodo_a','')}\n"
        f"TESTATE PRINCIPALI: {', '.join(f'{k}({v})' for k,v in list(stats.get('testate',{}).items())[:10])}\n"
        f"SENTIMENT: {', '.join(f'{k}: {v}%' for k,v in stats.get('sentiment',{}).items())}\n"
    )

    # Trunca estratto se troppo lungo
    extracted_txt = json.dumps(extracted[:80], ensure_ascii=False, indent=None)
    if len(extracted_txt) > 15000:
        extracted_txt = extracted_txt[:15000] + "...]"

    resp = ai.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": _REPORT_SYSTEM},
            {"role": "user", "content": (
                f"RICHIESTA: {user_message}\n\n"
                f"STATISTICHE:\n{stats_txt}\n\n"
                f"ARTICOLI ESTRATTI (JSON):\n{extracted_txt}"
            )},
        ],
        temperature=0.1,
        max_tokens=8000,
    )
    return resp.choices[0].message.content.strip()


# ══════════════════════════════════════════════════════════════════════
# DOCX BUILDER
# ══════════════════════════════════════════════════════════════════════

def _build_docx(report_text: str, title: str = "Report SPIZ") -> str | None:
    """Chiama docx_builder.js e restituisce il path del file .docx generato."""
    if not os.path.exists(_BUILDER_JS):
        print(f"[DOCX] builder non trovato: {_BUILDER_JS}")
        return None
    try:
        tmp = tempfile.NamedTemporaryFile(suffix=".docx", delete=False, prefix="spiz_report_")
        out_path = tmp.name
        tmp.close()

        payload = json.dumps({"title": title, "content": report_text})
        result = subprocess.run(
            ["node", _BUILDER_JS, out_path],
            input=payload,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            print(f"[DOCX] node error: {result.stderr}")
            return None
        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            return out_path
        return None
    except Exception as e:
        print(f"[DOCX] build error: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

def ask_spiz(message: str, history: list = None, context: str = "general") -> dict:
    if not message or len(message.strip()) < 2:
        return {"error": "Messaggio troppo corto."}

    from_date, to_date = _date_range(context, message)
    intent = _detect_intent(message)
    wants_docx = _wants_docx(message)

    print(f"[SPIZ] intent={intent} from={from_date} to={to_date} docx={wants_docx}")

    # Ricerca semantica con fallback
    filtered = _semantic_search(from_date, to_date, message, limit=200)
    if not filtered:
        print("[SPIZ] semantic vuota, uso fallback")
        filtered = _fallback_search(from_date, to_date, limit=100)

    if not filtered:
        return {
            "response":      "Nessun articolo trovato nel periodo richiesto.",
            "is_report":     False,
            "docx_path":     None,
            "articles_used": 0,
            "total_period":  0,
        }

    stats = _stats(filtered)

    # ── REPORT ──
    if intent == "report":
        extracted   = _map_articles_parallel(filtered[:150])
        report_text = _reduce_to_report(message, extracted, stats)

        docx_path = None
        if wants_docx:
            docx_path = _build_docx(report_text)

        return {
            "response":      report_text,
            "is_report":     True,
            "docx_path":     docx_path,
            "articles_used": len(filtered),
            "total_period":  len(filtered),
        }

    # ── QUANTITATIVO ──
    elif intent == "quantitative":
        response_text = _quantitative_answer(message, filtered, stats)
        return {
            "response":      response_text,
            "is_report":     False,
            "docx_path":     None,
            "articles_used": len(filtered),
            "total_period":  len(filtered),
        }

    # ── QUICK ──
    else:
        response_text = _quick_answer(message, filtered, stats, history)
        return {
            "response":      response_text,
            "is_report":     False,
            "docx_path":     None,
            "articles_used": len(filtered),
            "total_period":  len(filtered),
        }