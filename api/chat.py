"""
api/chat.py - SPIZ AI v13

CAMBIO PRINCIPALE:
- ask_spiz() accetta ora preloaded_articles: list
  Se fornito, salta tutta la fase di ricerca e usa quegli articoli direttamente.
  Questo consente alla UI di pre-selezionare gli articoli (stessa meccanica di Press)
  e passare solo quelli scelti dall'utente alla fase map-reduce.

- La ricerca per client_name ora usa keyword search (come Press), non semantica.
- Mantenuta compatibilità backward: se preloaded_articles è vuoto, ricerca normale.
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

def _date_range(context: str, message: str = ""):
    days = _parse_days(message) if message else None
    if days is None:
        days = {"today": 0, "week": 7, "month": 30, "year": 365}.get(context, 30)
    today = date.today()
    if days == 0:
        return today.isoformat(), today.isoformat()
    return (today - timedelta(days=days)).isoformat(), today.isoformat()


# ══════════════════════════════════════════════════════════════════════
# RICERCA (fallback se non arrivano articoli pre-caricati)
# ══════════════════════════════════════════════════════════════════════

def _fetch_articles_by_ids(article_ids: list) -> list:
    """Recupera gli articoli completi a partire da una lista di id."""
    if not article_ids:
        return []
    try:
        res = (supabase.table("articles")
               .select(DB_COLS)
               .in_("id", article_ids)
               .execute())
        return res.data or []
    except Exception as e:
        print(f"[SPIZ] fetch by ids error: {e}")
        return []

def _semantic_search(from_date: str, to_date: str, query: str, limit: int = 200) -> list:
    try:
        emb = ai.embeddings.create(
            model="text-embedding-3-small",
            input=query[:8000],
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

def _fallback_search(from_date: str, to_date: str, limit: int = 100) -> list:
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
# STATISTICHE
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

def _build_docx(report_text: str, title: str = "Report SPIZ") -> str | None:
    if not os.path.exists(_BUILDER_JS):
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
        return out_path if os.path.exists(out_path) and os.path.getsize(out_path) > 0 else None
    except Exception as e:
        print(f"[DOCX] build error: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════
# MAP
# ══════════════════════════════════════════════════════════════════════

_MAP_SYSTEM = """Sei un analista di media monitoring. Leggi gli articoli e restituisci
un JSON con lista "articoli". Per ogni articolo:
- id_ref: "TESTATA_YYYYMMDD_N" (es. SOLE24ORE_20260305_1)
- testata, data, titolo, giornalista (string)
- fatti_chiave: array max 3 stringhe — fatti oggettivi
- angolo: string — angolazione giornalistica della testata
- attori: array string — soggetti citati
- tensione: string|null — contrapposizione narrativa
- tono_verso_soggetto: "positivo"|"negativo"|"neutro"
- rilevanza: intero 1-5

Rispondi SOLO con JSON valido."""

def _map_batch(batch: list, idx: int):
    lines = []
    for i, a in enumerate(batch):
        testo = (a.get("testo_completo") or "")[:1500]
        lines.append(
            f"[ART {i+1}]\n"
            f"TESTATA: {a.get('testata','')}\nDATA: {a.get('data','')}\n"
            f"TITOLO: {a.get('titolo','')}\nGIORNALISTA: {a.get('giornalista','')}\n"
            f"TESTO: {testo}"
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
# REDUCE — prompt v13
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
5. Ogni osservazione deve avere un'implicazione pratica per chi fa media relations.
6. Cita sempre testata, autore (se presente) e titolo quando menzioni un articolo.

STRUTTURA OBBLIGATORIA:

## 1. CLIMA MEDIATICO
10-15 righe. Non descrivere i fatti: interpretali.
- Tono prevalente e cosa rivela sull'atteggiamento dei media verso il soggetto
- Fratture narrative tra testate (es. chi attacca, chi difende, chi ignora)
- Dinamiche di potere tra gli attori citati
- Dove si sta evolvendo il dibattito e cosa potrebbe succedere nei prossimi giorni

## 2. TEMI DOMINANTI
3-5 temi. Per ciascuno:
- Angolazione, tono, registro narrativo usato dai media
- Attori principali e loro ruolo nel racconto
- Tensioni o contrapposizioni narrative presenti
- Cita almeno un articolo specifico: testata, autore, titolo

## 3. SPAZI NARRATIVI PER IL CLIENTE
La sezione più strategica. 4-6 ganci giornalistici concreti.

REGOLA CRITICA: ogni spazio narrativo deve nascere da qualcosa che accade
ADESSO in questo corpus. Se non puoi ancorarlo a un articolo o dinamica
specifica del corpus, non includerlo.

Per ciascuno, 10+ righe:
- Titolo sintetico del frame narrativo
- Quale fatto/articolo/dinamica del corpus apre questo spazio (citare esplicitamente)
- Perché questo è il momento giusto per questo posizionamento
- Cosa dovrebbe dire/fare concretamente il cliente
- Come lo racconterebbe un giornalista (angolo, registro, format)
- Quale testata o giornalista del corpus sarebbe più ricettivo e perché

## 4. ANGOLI GIORNALISTICI IMMEDIATI
3 pitch concreti proponibili oggi.
Per ciascuno:
- Titolo come uscirebbe su quella testata specifica (non generico)
- A quale giornalista/testata del corpus proporlo e perché
- Taglio: notizia, analisi, intervista, dossier, commento
- Perché una redazione lo aprirebbe oggi e non fra una settimana

---
Chiudi con:
**CORPUS:** [N] articoli · [TESTATA1(n), TESTATA2(n), …top 5] · [DATA_DA] → [DATA_A]

Lunghezza target: 900-1100 parole. Denso, concreto, zero genericità."""


def _reduce_to_report(
    query: str,
    extracted: list,
    stats: dict,
    client_name: str = "",
    topic_name: str = "",
) -> str:
    focus_block = ""
    if client_name and client_name.strip():
        focus_block = (
            f"\nCLIENTE: {client_name.strip()}\n"
            f"Sezione 3: costruisci gli spazi narrativi specificamente per "
            f"{client_name.strip()}. Usa il nome. Parti dai fatti del corpus. "
            f"Zero spazi narrativi generici sul settore.\n"
        )
    elif topic_name and topic_name.strip():
        focus_block = (
            f"\nARGOMENTO: {topic_name.strip()}\n"
            f"Report centrato su questo argomento. "
            f"Sezione 3: spazi per un soggetto che voglia posizionarsi su "
            f"{topic_name.strip()}, ancorati ai fatti del corpus.\n"
        )

    stats_txt = (
        f"TOTALE ARTICOLI: {stats.get('totale', 0)}\n"
        f"PERIODO: {stats.get('periodo_da', '')} → {stats.get('periodo_a', '')}\n"
        f"TESTATE: {', '.join(f'{k}({v})' for k, v in list(stats.get('testate', {}).items())[:12])}\n"
        f"SENTIMENT: {', '.join(f'{k}: {v}%' for k, v in stats.get('sentiment', {}).items())}\n"
        f"GIORNALISTI ATTIVI: {', '.join(f'{k}({v})' for k, v in list(stats.get('giornalisti', {}).items())[:10])}\n"
    )

    extracted_txt = json.dumps(extracted[:80], ensure_ascii=False, indent=None)
    if len(extracted_txt) > 18000:
        extracted_txt = extracted_txt[:18000] + "...]"

    resp = ai.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": _REPORT_SYSTEM},
            {"role": "user", "content": (
                f"QUERY: {query}\n"
                f"{focus_block}\n"
                f"STATISTICHE (usa solo questi numeri):\n{stats_txt}\n\n"
                f"CORPUS:\n{extracted_txt}"
            )},
        ],
        temperature=0.15,
        max_tokens=8000,
    )
    return resp.choices[0].message.content.strip()


# ══════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

def ask_spiz(
    message: str = "",
    history: list = None,
    context: str = "week",
    client_name: str = "",
    topic_name: str = "",
    preloaded_articles: list = None,   # NEW: articoli già selezionati dalla UI
) -> dict:
    client_name = (client_name or "").strip()
    topic_name  = (topic_name  or "").strip()

    # ── Se arrivano articoli pre-caricati dalla UI, li usiamo direttamente ──
    if preloaded_articles:
        articles = preloaded_articles
        print(f"[SPIZ v13] usando {len(articles)} articoli pre-selezionati dalla UI")
    else:
        # Fallback: ricerca nel DB
        if not client_name and not topic_name and not message:
            return {"error": "Nessun cliente, argomento o query specificata."}

        from_date, to_date = _date_range(context, message)
        search_query = client_name or topic_name or message
        print(f"[SPIZ v13] ricerca: {from_date}→{to_date} | query={search_query!r}")

        articles = _semantic_search(from_date, to_date, search_query, limit=200)
        if not articles:
            articles = _fallback_search(from_date, to_date, limit=100)

    if not articles:
        return {
            "response":      "Nessun articolo trovato.",
            "is_report":     False,
            "articles_used": 0,
            "articles_list": [],
        }

    stats       = _stats(articles)
    extracted   = _map_articles_parallel(articles[:150])
    report_text = _reduce_to_report(
        client_name or topic_name or message,
        extracted, stats,
        client_name=client_name,
        topic_name=topic_name,
    )

    articles_list = [
        {
            "id":          a.get("id", ""),
            "testata":     a.get("testata", ""),
            "data":        a.get("data", ""),
            "titolo":      a.get("titolo", ""),
            "giornalista": a.get("giornalista", ""),
            "occhiello":   a.get("occhiello", ""),
            "tone":        a.get("tone", ""),
            "ave":         a.get("ave", ""),
            "url":         a.get("url", ""),
            "tipo_fonte":  a.get("tipo_fonte", ""),
        }
        for a in articles
    ]

    return {
        "response":      report_text,
        "is_report":     True,
        "articles_used": len(articles),
        "period_from":   stats.get("periodo_da", ""),
        "period_to":     stats.get("periodo_a", ""),
        "articles_list": articles_list,
    }