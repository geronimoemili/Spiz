"""
api/chat.py - SPIZ AI v11
ARCHITETTURA:
- Un solo percorso: intelligence report (map → reduce)
- Zero routing per intent: la AI è sempre in modalità analista senior
- Zero riferimenti a settori specifici: funziona per qualsiasi cliente/tema
- I numeri (conteggi, testate, sentiment %) restano a Python via _stats()
  e vengono passati al modello come dati già calcolati, mai da calcolare
- client_name: se fornito, personalizza la sezione "Spazi narrativi"
  senza bisogno di scriverlo nel messaggio ogni volta
"""

import os
from dotenv import load_dotenv
load_dotenv()

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
    (r"oggi|odiern",                                                0),
    (r"ultime?\s*24.?ore|ieri",                                     1),
    (r"ultim[ie]\s*(?:[23]\s*(?:giorn|gg\b|g\b))",                 3),
    (r"ultim[ie]\s*(?:[67]\s*(?:giorn|gg\b|g\b)|settiman|7\s*(?:giorn|gg))", 7),
    (r"ultim[ie]\s*(?:15\s*(?:giorn|gg\b)|due\s*settiman)",        15),
    (r"ultim[ie]\s*(?:30\s*(?:giorn|gg\b|g\b)?)\b|ultimo\s*mese|mese\s*scors", 30),
    (r"ultim[ie]\s*(?:[23]\s*mesi|[69]0\s*giorn)",                 90),
    (r"ultim[ie]\s*(?:[46]\s*mesi)",                               180),
    (r"ultimo\s*anno|ultim[ie]\s*12\s*mesi",                      365),
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
# RICERCA
# ══════════════════════════════════════════════════════════════════════

def _semantic_search(from_date: str, to_date: str, user_message: str, limit: int = 200):
    try:
        emb = ai.embeddings.create(
            model="text-embedding-3-small",
            input=user_message[:8000],
        ).data[0].embedding
        res = supabase.rpc(
            "match_articles",
            {"query_embedding": emb, "match_from": from_date,
             "match_to": to_date, "match_count": limit},
        ).execute()
        return res.data or []
    except Exception as e:
        print(f"[SPIZ] semantic search error: {e}")
        return []

def _fallback_search(from_date: str, to_date: str, limit: int = 100):
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
# STATISTICHE  (i numeri li calcola Python, non il modello)
# ══════════════════════════════════════════════════════════════════════

def _stats(articles: list) -> dict:
    if not articles:
        return {}
    testate     = Counter(a.get("testata", "")     for a in articles if a.get("testata"))
    giornalisti = Counter(a.get("giornalista", "") for a in articles if a.get("giornalista"))
    tones       = Counter(a.get("tone", "")        for a in articles if a.get("tone"))
    tone_tot    = sum(tones.values()) or 1
    dates       = [a.get("data", "") for a in articles if a.get("data")]
    return {
        "totale":      len(articles),
        "periodo_da":  min(dates) if dates else "",
        "periodo_a":   max(dates) if dates else "",
        "testate":     dict(testate.most_common(20)),
        "giornalisti": dict(giornalisti.most_common(50)),
        "sentiment":   {k: round(v / tone_tot * 100) for k, v in tones.items() if k},
    }


# ══════════════════════════════════════════════════════════════════════
# DOCX BUILDER
# ══════════════════════════════════════════════════════════════════════

def _wants_docx(message: str) -> bool:
    return any(kw in message.lower() for kw in ["word", "docx", "scarica", "download", "file"])

def _build_docx(report_text: str, title: str = "Report SPIZ") -> str | None:
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
            input=payload, capture_output=True, text=True, timeout=30,
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
# MAP — estrazione strutturata per ogni batch di articoli
# Il modello legge e sintetizza; NON conta, NON calcola percentuali
# ══════════════════════════════════════════════════════════════════════

_MAP_SYSTEM = """Sei un analista di media monitoring. Leggi gli articoli forniti e
restituisci un JSON con una lista "articoli". Per ogni articolo includi:
- testata (string)
- data (string)
- titolo (string)
- fatti_chiave: array di max 3 stringhe — i fatti oggettivi riportati
- angolo: string — l'angolazione giornalistica scelta dalla testata
- attori: array di stringhe — soggetti citati (aziende, persone, istituzioni)
- tensione: string o null — eventuale contrapposizione narrativa presente
- rilevanza: intero 1-5 rispetto al tema della richiesta

Rispondi SOLO con JSON valido, nessun testo fuori dal JSON."""

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
                {"role": "user",   "content": "\n\n".join(lines)},
            ],
            temperature=0.0,
            max_tokens=3000,
            response_format={"type": "json_object"},
        )
        parsed = json.loads(resp.choices[0].message.content)
        items  = parsed.get("articoli", parsed) if isinstance(parsed, dict) else parsed
        return idx, items if isinstance(items, list) else []
    except Exception as e:
        print(f"[MAP] batch {idx} error: {e}")
        return idx, []

def _map_articles_parallel(articles: list, batch_size: int = 5, max_workers: int = 4) -> list:
    batches = [articles[i:i + batch_size] for i in range(0, len(articles), batch_size)]
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


# ══════════════════════════════════════════════════════════════════════
# REDUCE — produzione del report intelligence
# Sistema universale: nessun riferimento a settori, tecnologie o contesti
# specifici. Il modello legge il corpus e si adatta al dominio da solo.
# client_name: se presente, personalizza la sezione Spazi narrativi.
# ══════════════════════════════════════════════════════════════════════

_REPORT_SYSTEM = """Sei SPIZ, analista senior di comunicazione e media intelligence
di MAIM Public Diplomacy & Media Relations.

Il tuo compito è produrre report strategici di media intelligence destinati
a professionisti della comunicazione che devono supportare i loro clienti
nelle relazioni con i media.

REGOLE FONDAMENTALI:
1. Usa ESCLUSIVAMENTE i dati e gli articoli forniti. Mai la tua conoscenza generale.
2. Ogni affermazione deve essere ricavabile dal corpus ricevuto.
3. Se un elemento non è presente nel corpus: "Non emergono elementi su questo punto."
4. Italiano professionale da advisor di comunicazione. Nessuna emoji.
5. Sii analitico e orientato all'azione: ogni osservazione deve avere
   un'implicazione pratica per chi fa media relations.

STRUTTURA OBBLIGATORIA DEL REPORT:

## 1. CLIMA MEDIATICO
Sintesi in 5-7 righe del quadro generale del dibattito nei media sul tema/settore
del corpus: tono prevalente, eventuali fratture narrative, elementi di contesto
politico, economico o istituzionale rilevanti, dinamiche tra attori.

## 2. TEMI DOMINANTI
Individua 3-5 temi principali emersi dal corpus. Per ciascuno:
- come viene raccontato dai media
- quali attori compaiono e con quale ruolo
- eventuali tensioni o contrapposizioni narrative

## 3. SPAZI NARRATIVI PER IL CLIENTE
Questa è la sezione più strategica. Individua 4-6 possibili ganci giornalistici
utilizzabili per posizionare il cliente nel dibattito corrente. Per ciascuno:
- titolo sintetico del frame narrativo
- spiegazione (3-4 righe)
- perché è coerente con il dibattito attuale
- come potrebbe essere raccontato da un giornalista

## 4. ANGOLI GIORNALISTICI IMMEDIATI
3 spunti editoriali o interviste proponibili rapidamente alle redazioni.
Per ciascuno:
- titolo possibile dell'articolo
- taglio giornalistico
- perché potrebbe interessare una redazione oggi

Lunghezza target: 600-800 parole. Sintetico, analitico, orientato all'azione."""


def _reduce_to_report(
    user_message: str,
    extracted: list,
    stats: dict,
    client_name: str = "",
) -> str:
    # Contesto cliente: se fornito, personalizza la sezione Spazi narrativi
    client_block = ""
    if client_name and client_name.strip():
        client_block = (
            f"\nCLIENTE: {client_name.strip()}\n"
            "Nella sezione 'Spazi narrativi per il cliente' costruisci le opportunità "
            f"specificamente per {client_name.strip()}, usando il suo nome dove pertinente "
            "e ragionando su come potrebbe inserirsi nel dibattito come voce autorevole.\n"
        )

    stats_txt = (
        f"TOTALE ARTICOLI NEL CORPUS: {stats.get('totale', 0)}\n"
        f"PERIODO: {stats.get('periodo_da', '')} → {stats.get('periodo_a', '')}\n"
        f"TESTATE (per numero di articoli): "
        f"{', '.join(f'{k}({v})' for k, v in list(stats.get('testate', {}).items())[:12])}\n"
        f"SENTIMENT COMPLESSIVO: "
        f"{', '.join(f'{k}: {v}%' for k, v in stats.get('sentiment', {}).items())}\n"
        f"GIORNALISTI PIÙ ATTIVI: "
        f"{', '.join(f'{k}({v})' for k, v in list(stats.get('giornalisti', {}).items())[:10])}\n"
    )

    extracted_txt = json.dumps(extracted[:100], ensure_ascii=False, indent=None)
    if len(extracted_txt) > 18000:
        extracted_txt = extracted_txt[:18000] + "...]"

    resp = ai.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": _REPORT_SYSTEM},
            {"role": "user", "content": (
                f"RICHIESTA: {user_message}\n"
                f"{client_block}\n"
                f"DATI STATISTICI (calcolati dal sistema, usa solo questi per i numeri):\n"
                f"{stats_txt}\n\n"
                f"CORPUS ARTICOLI ANALIZZATI (JSON estratto):\n{extracted_txt}"
            )},
        ],
        temperature=0.15,
        max_tokens=8000,
    )
    return resp.choices[0].message.content.strip()


# ══════════════════════════════════════════════════════════════════════
# ENTRY POINT UNICO
# Non c'è più routing per intent. La chat fa sempre e solo intelligence.
# client_name: opzionale, passato dal frontend quando l'utente seleziona
#              un cliente dalla dropdown. Personalizza la sezione 3.
# ══════════════════════════════════════════════════════════════════════

def ask_spiz(
    message: str,
    history: list = None,
    context: str = "general",
    client_name: str = "",
) -> dict:
    if not message or len(message.strip()) < 2:
        return {"error": "Messaggio troppo corto."}

    from_date, to_date = _date_range(context, message)
    wants_docx = _wants_docx(message)

    print(f"[SPIZ v11] from={from_date} to={to_date} docx={wants_docx} client={client_name!r}")

    # Recupero articoli
    articles = _semantic_search(from_date, to_date, message, limit=200)
    if not articles:
        print("[SPIZ] semantic vuota, uso fallback")
        articles = _fallback_search(from_date, to_date, limit=150)

    if not articles:
        return {
            "response":      "Nessun articolo trovato nel periodo richiesto.",
            "is_report":     False,
            "docx_path":     None,
            "articles_used": 0,
            "total_period":  0,
        }

    # Statistiche: Python conta, il modello non tocca numeri
    stats = _stats(articles)

    # Map: ogni articolo viene strutturato in parallelo (gpt-4o-mini, veloce)
    extracted = _map_articles_parallel(articles[:150])

    # Reduce: il modello produce il report intelligence (gpt-4o, qualità)
    report_text = _reduce_to_report(message, extracted, stats, client_name=client_name)

    # Docx opzionale
    docx_path = None
    if wants_docx:
        title = f"Report SPIZ — {client_name}" if client_name else "Report SPIZ"
        docx_path = _build_docx(report_text, title=title)

    return {
        "response":      report_text,
        "is_report":     True,
        "docx_path":     docx_path,
        "articles_used": len(articles),
        "total_period":  len(articles),
    }