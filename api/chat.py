"""
api/chat.py - SPIZ AI v14

OTTIMIZZAZIONI vs v13:
- batch_size 10 (era 5): dimezza le chiamate API nel map
- testo troncato a 800 chars (era 1500): token ridotti del 45%
- corpus cappato a 60 articoli nel reduce (era 80/100)
- preloaded_articles: salta tutta la ricerca
"""

import os
from dotenv import load_dotenv
load_dotenv()

import re
import json
from datetime import date, timedelta
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from openai import OpenAI
from services.database import supabase

ai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

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
# MAP — batch 10, testo 800 chars
# ══════════════════════════════════════════════════════════════════════

_MAP_SYSTEM = """Sei un analista di media monitoring. Leggi gli articoli e restituisci
un JSON con lista "articoli". Per ogni articolo:
- id_ref: "TESTATA_DATA_N"
- testata, data, titolo, giornalista (string)
- fatti_chiave: array max 2 stringhe — fatti oggettivi principali
- angolo: string — angolazione giornalistica della testata
- attori: array max 3 string — soggetti principali citati
- tensione: string|null — contrapposizione narrativa
- tono_verso_soggetto: "positivo"|"negativo"|"neutro"
- rilevanza: intero 1-5

Rispondi SOLO con JSON valido."""

def _map_batch(batch: list, idx: int):
    lines = []
    for i, a in enumerate(batch):
        testo = (a.get("testo_completo") or "")[:800]   # 800 chars, era 1500
        lines.append(
            f"[{i+1}] TESTATA:{a.get('testata','')} DATA:{a.get('data','')}\n"
            f"TITOLO:{a.get('titolo','')}\nGIORN:{a.get('giornalista','')}\n"
            f"TESTO:{testo}"
        )
    try:
        resp = ai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": _MAP_SYSTEM},
                {"role": "user",   "content": "\n---\n".join(lines)},
            ],
            temperature=0.0,
            max_tokens=4000,
            response_format={"type": "json_object"},
        )
        parsed = json.loads(resp.choices[0].message.content)
        items  = parsed.get("articoli", parsed) if isinstance(parsed, dict) else parsed
        return idx, items if isinstance(items, list) else []
    except Exception as e:
        print(f"[MAP] batch {idx} error: {e}")
        return idx, []

def _map_articles_parallel(articles: list, batch_size: int = 10, max_workers: int = 6) -> list:
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
# REDUCE
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
10-15 righe. Non descrivere: interpreta.
- Tono prevalente e cosa rivela sull'atteggiamento dei media verso il soggetto
- Fratture narrative tra testate diverse
- Dinamiche di potere tra gli attori citati
- Dove si sta evolvendo il dibattito

## 2. TEMI DOMINANTI
3-5 temi. Per ciascuno:
- Angolazione, tono, registro narrativo
- Attori principali e ruolo nel racconto
- Tensioni o contrapposizioni narrative
- Cita almeno un articolo: testata, autore, titolo

## 3. SPAZI NARRATIVI PER IL CLIENTE
4-6 ganci giornalistici. REGOLA CRITICA: ogni spazio deve nascere da qualcosa
che accade ADESSO nel corpus. Zero spazi generici sul settore.

Per ciascuno, 10+ righe:
- Titolo sintetico del frame narrativo
- Quale fatto/articolo/dinamica del corpus apre questo spazio (citare)
- Perché questo è il momento giusto
- Cosa dovrebbe dire/fare il cliente
- Come lo racconterebbe un giornalista
- Quale testata/giornalista del corpus è più ricettivo e perché

## 4. ANGOLI GIORNALISTICI IMMEDIATI
3 pitch concreti proponibili oggi.
Per ciascuno:
- Titolo come uscirebbe su quella testata (non generico)
- A quale giornalista/testata proporlo e perché
- Taglio: notizia, analisi, intervista, dossier, commento
- Perché una redazione lo aprirebbe oggi

---
Chiudi con:
**CORPUS:** [N] articoli · [TESTATA1(n), TESTATA2(n), …top 5] · [DATA_DA] → [DATA_A]

Lunghezza: 900-1100 parole. Denso, concreto, zero genericità."""


def _reduce_to_report(
    query: str,
    extracted: list,
    stats: dict,
    client_name: str = "",
    topic_name: str = "",
) -> str:
    focus_block = ""
    if client_name:
        focus_block = (
            f"\nCLIENTE: {client_name}\n"
            f"Sezione 3: spazi narrativi specifici per {client_name}. "
            f"Usa il nome. Parti dai fatti del corpus. Zero genericità.\n"
        )
    elif topic_name:
        focus_block = (
            f"\nARGOMENTO: {topic_name}\n"
            f"Sezione 3: spazi per un soggetto che voglia posizionarsi su "
            f"{topic_name}, ancorati ai fatti del corpus.\n"
        )

    stats_txt = (
        f"TOTALE: {stats.get('totale', 0)} articoli\n"
        f"PERIODO: {stats.get('periodo_da', '')} → {stats.get('periodo_a', '')}\n"
        f"TESTATE: {', '.join(f'{k}({v})' for k, v in list(stats.get('testate', {}).items())[:12])}\n"
        f"SENTIMENT: {', '.join(f'{k}: {v}%' for k, v in stats.get('sentiment', {}).items())}\n"
        f"GIORNALISTI: {', '.join(f'{k}({v})' for k, v in list(stats.get('giornalisti', {}).items())[:10])}\n"
    )

    # Cap a 60 articoli nel reduce
    extracted_txt = json.dumps(extracted[:60], ensure_ascii=False, indent=None)
    if len(extracted_txt) > 16000:
        extracted_txt = extracted_txt[:16000] + "...]"

    resp = ai.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": _REPORT_SYSTEM},
            {"role": "user", "content": (
                f"QUERY: {query}\n{focus_block}\n"
                f"STATISTICHE:\n{stats_txt}\n\n"
                f"CORPUS:\n{extracted_txt}"
            )},
        ],
        temperature=0.15,
        max_tokens=4000,   # era 8000: dimezza il tempo di risposta
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
    preloaded_articles: list = None,
) -> dict:
    client_name = (client_name or "").strip()
    topic_name  = (topic_name  or "").strip()

    if preloaded_articles:
        articles = preloaded_articles
        print(f"[SPIZ v14] preloaded: {len(articles)} articoli")
    else:
        if not client_name and not topic_name and not message:
            return {"error": "Nessun cliente, argomento o query."}

        from_date, to_date = _date_range(context, message)
        search_query = client_name or topic_name or message

        try:
            emb = ai.embeddings.create(
                model="text-embedding-3-small",
                input=search_query[:8000],
            ).data[0].embedding
            res = supabase.rpc(
                "match_articles",
                {"query_embedding": emb, "match_from": from_date,
                 "match_to": to_date, "match_count": 200},
            ).execute()
            articles = res.data or []
        except Exception as e:
            print(f"[SPIZ] search error: {e}")
            articles = []

        if not articles:
            try:
                res = (supabase.table("articles")
                       .select(DB_COLS)
                       .gte("data", from_date)
                       .lte("data", to_date)
                       .order("data", desc=True)
                       .limit(100)
                       .execute())
                articles = res.data or []
            except Exception as e:
                print(f"[SPIZ] fallback error: {e}")

    if not articles:
        return {"error": "Nessun articolo trovato.", "is_report": False}

    stats     = _stats(articles)
    # Map su max 60 articoli (era 150)
    extracted = _map_articles_parallel(articles[:60])
    report    = _reduce_to_report(
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
            "tone":        a.get("tone", ""),
            "ave":         a.get("ave", ""),
            "url":         a.get("url", ""),
        }
        for a in articles
    ]

    return {
        "response":      report,
        "is_report":     True,
        "articles_used": len(articles),
        "period_from":   stats.get("periodo_da", ""),
        "period_to":     stats.get("periodo_a", ""),
        "articles_list": articles_list,
    }