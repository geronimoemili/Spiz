"""
api/chat.py - SPIZ AI v7
Pipeline MAP+REDUCE per report giornalistici di qualità.
Supporta: Supabase DB + CSV allegato, output testo + .docx
"""

import os
import re
import csv
import json
import subprocess
import tempfile
from datetime import date, timedelta
from collections import Counter
from io import StringIO
from openai import OpenAI
from services.database import supabase

ai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Path al builder docx (stessa cartella di chat.py oppure api/)
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
    (r"oggi|odiern",                                                          0),
    (r"ultime?\s*24.?ore|ieri",                                               1),
    (r"ultim[ie]\s*[23]\s*(?:giorn|gg\b|g\b)",                               3),
    (r"ultim[ie]\s*[45]\s*(?:giorn|gg\b|g\b)",                               5),
    (r"ultim[ie]\s*(?:[67]\s*(?:giorn|gg\b|g\b)|settiman|7\s*(?:giorn|gg))", 7),
    (r"ultim[ie]\s*10\s*(?:giorn|gg\b|g\b)",                                10),
    (r"ultim[ie]\s*15\s*(?:giorn|gg\b|g\b)|due\s*settiman",                 15),
    (r"ultim[ie]\s*20\s*(?:giorn|gg\b|g\b)",                                20),
    (r"ultim[ie]\s*(?:30\s*(?:giorn|gg\b|g\b)?)\b|ultimo\s*mese|mese\s*scors", 30),
    (r"ultim[ie]\s*(?:60\s*(?:giorn|gg\b|g\b)|2\s*mesi)",                   60),
    (r"ultim[ie]\s*(?:90\s*(?:giorn|gg\b|g\b)|3\s*mesi)",                   90),
    (r"ultim[ie]\s*(?:6\s*mesi|180\s*(?:giorn|gg\b))",                     180),
    (r"ultimo\s*anno|ultim[ie]\s*12\s*mesi",                                365),
]

def _parse_days(msg: str):
    m = msg.lower()
    for pattern, days in _TIME_RULES:
        if re.search(pattern, m):
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
# ESTRAZIONE TOPIC / GIORNALISTA
# ══════════════════════════════════════════════════════════════════════
_STOP_WORDS = {
    "ultima", "ultim", "articol", "settim", "giorn", "mese", "anno",
    "realizza", "fammi", "dammi", "fai", "crea", "scrivi", "redigi",
    "due", "tre", "pagin", "voglio", "sapere", "succede", "quali",
    "interessant", "criticita", "quanti", "utilizza", "oggi", "allegat",
}

def _extract_topic(message: str):
    msg = message.lower().strip()
    patterns = [
        r"(?:sul?|il|la|dello?|della|dei|degli|delle)?\s*tema\s+([a-z0-9àèéìòùäöü\s\-&/]+?)(?:\s*[,?.!]|$|\s+(?:ultim|negli|nell|voglio|dammi|fammi|utilizza|crea|redigi|allegat))",
        r"(?:riguardant[ei]|relativ[oi]\s+a|parlano\s+di|riguardo\s+a?)\s+([a-z0-9àèéìòùäöü\s\-&/]+?)(?:\s*[,?.!]|$|\s+(?:ultim|negli|nell|voglio|dammi|fammi|utilizza))",
        r"(?:documento|report|analisi|sintesi|profilo|relazione|panoramica|overview|rassegna)\s+(?:su|sul|sulla|sullo|sugli|di|del|della|dello|dei|sulle)\s+([a-z0-9àèéìòùäöü\s\-&/]+?)(?:\s*[,?.!]|$|\s+(?:ultim|negli|nell|voglio|dammi|fammi|utilizza|crea|redigi))",
        r"(?:su|di)\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,3})",  # nomi propri
    ]
    for pat in patterns:
        m = re.search(pat, msg)
        if m:
            raw = m.group(1).strip().rstrip("., ")
            words = raw.split()
            while words and any(s in words[-1] for s in _STOP_WORDS):
                words.pop()
            candidate = " ".join(words).strip()
            if len(candidate) >= 2:
                return candidate
    return None

def _extract_journalist(message: str):
    msg = message.lower()
    patterns = [
        r"articoli\s+(?:di|scritti\s+da|firmati\s+da)\s+([a-z]+(?:\s+[a-z]+){1,2})",
        r"(?:scritti|firmati)\s+da\s+([a-z]+(?:\s+[a-z]+){1,2})",
        r"cosa\s+ha\s+scritto\s+([a-z]+(?:\s+[a-z]+){1,2})",
    ]
    for pat in patterns:
        m = re.search(pat, msg)
        if m:
            candidate = m.group(1).strip()
            if not any(b in candidate for b in _STOP_WORDS):
                return candidate
    return None


# ══════════════════════════════════════════════════════════════════════
# DATA SOURCES
# ══════════════════════════════════════════════════════════════════════

def _load_from_db(from_date: str, to_date: str, limit: int = 2000) -> list:
    try:
        res = (
            supabase.table("articles")
            .select(DB_COLS)
            .gte("data", from_date)
            .lte("data", to_date)
            .order("data", desc=True)
            .limit(limit)
            .execute()
        )
        return res.data or []
    except Exception as e:
        print(f"[SPIZ] DB error: {e}")
        return []


def _load_from_csv(csv_content: str) -> list:
    """
    Converte il CSV della rassegna stampa nel formato interno articoli.
    Gestisce sia virgole che punto e virgola come separatore.
    """
    articles = []
    try:
        # Prova a rilevare il separatore
        sample = csv_content[:2000]
        sep = ";" if sample.count(";") > sample.count(",") else ","

        reader = csv.DictReader(StringIO(csv_content), delimiter=sep)
        for row in reader:
            # Mapping colonne CSV → schema interno
            # Supporta sia il formato "rassegna stampa" che un CSV generico
            def g(*keys):
                for k in keys:
                    v = row.get(k, "").strip()
                    if v:
                        return v
                return ""

            # Data: normalizza da dd-mm-yyyy a yyyy-mm-dd
            raw_date = g("Data Testata", "data", "Data", "DATA")
            normalized_date = raw_date
            m = re.match(r"(\d{2})-(\d{2})-(\d{4})", raw_date)
            if m:
                normalized_date = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

            articles.append({
                "id":                 g("id"),
                "testata":            g("Testata", "testata", "TESTATA"),
                "data":               normalized_date,
                "giornalista":        g("Autore", "giornalista", "Autore/i"),
                "occhiello":          g("Occhiello", "occhiello"),
                "titolo":             g("Titolo", "titolo", "TITOLO"),
                "sottotitolo":        g("Sottotitolo", "sottotitolo"),
                "testo_completo":     g("Testo", "testo_completo", "Testo Abstract", "Sommario Abstract"),
                "macrosettori":       g("Macrosettori", "macrosettori"),
                "tipologia_articolo": g("Tipologia articolo", "tipologia_articolo"),
                "tone":               g("Sentiment", "tone", "Tone"),
                "dominant_topic":     g("Argomento", "dominant_topic"),
                "reputational_risk":  g("reputational_risk"),
                "political_risk":     g("political_risk"),
                "ave":                g("AVE", "ave"),
                "tipo_fonte":         g("Tipo Fonte", "tipo_fonte"),
            })
    except Exception as e:
        print(f"[SPIZ] CSV parse error: {e}")
    return articles


# ══════════════════════════════════════════════════════════════════════
# RICERCA / FILTRO
# ══════════════════════════════════════════════════════════════════════

def _search_scored(articles: list, query: str) -> list:
    if not query or not articles:
        return articles
    words = [w.lower() for w in re.split(r'\W+', query) if len(w) >= 2]
    if not words:
        return articles
    scored = []
    for a in articles:
        title_hay = " ".join(filter(None, [a.get("titolo"), a.get("occhiello")])).lower()
        full_hay  = " ".join(filter(None, [
            a.get("titolo"), a.get("occhiello"), a.get("sottotitolo"),
            a.get("testo_completo"), a.get("macrosettori"), a.get("dominant_topic"),
        ])).lower()
        score = sum(3 if w in title_hay else (1 if w in full_hay else 0) for w in words)
        if score > 0:
            scored.append((score, a))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [a for _, a in scored]

def _filter_journalist(articles: list, name: str) -> list:
    nl = name.lower()
    return [a for a in articles if nl in (a.get("giornalista") or "").lower()]


# ══════════════════════════════════════════════════════════════════════
# STATISTICHE
# ══════════════════════════════════════════════════════════════════════

def _stats(articles: list) -> dict:
    if not articles:
        return {}
    testate     = Counter(a.get("testata","") for a in articles if a.get("testata"))
    giornalisti = Counter(a.get("giornalista","") for a in articles if a.get("giornalista"))
    tones       = Counter(a.get("tone","") for a in articles if a.get("tone"))
    topics      = Counter(a.get("dominant_topic","") for a in articles if a.get("dominant_topic"))
    settori_all = []
    monthly     = Counter()
    for a in articles:
        d = a.get("data","")
        if d and len(d) >= 7:
            monthly[d[:7]] += 1
        for s in (a.get("macrosettori") or "").split(","):
            s = s.strip()
            if s: settori_all.append(s)
    settori   = Counter(settori_all)
    tone_tot  = sum(tones.values()) or 1
    dates     = [a.get("data","") for a in articles if a.get("data")]
    return {
        "totale":      len(articles),
        "periodo_da":  min(dates) if dates else "",
        "periodo_a":   max(dates) if dates else "",
        "testate":     dict(testate.most_common(20)),
        "giornalisti": dict(giornalisti.most_common(10)),
        "settori":     dict(settori.most_common(10)),
        "topics":      dict(topics.most_common(8)),
        "sentiment":   {k: round(v/tone_tot*100) for k,v in tones.items() if k},
        "mensile":     dict(sorted(monthly.items())),
    }


# ══════════════════════════════════════════════════════════════════════
# MAP — Estrazione strutturata per articolo (batch)
# ══════════════════════════════════════════════════════════════════════

_MAP_SYSTEM = """Sei un analista mediatico. Ti vengono forniti degli articoli.
Per CIASCUN articolo restituisci un oggetto JSON con questi campi:
- id: numero progressivo fornito
- testata: nome testata
- data: data articolo
- giornalista: nome giornalista (o "Redazione")
- titolo: titolo originale
- fatti_chiave: array di 2-4 stringhe, fatti concreti con numeri/dati se presenti
- citazioni: array di 0-2 oggetti {chi, cosa} — solo citazioni dirette rilevanti
- angolo: "positivo"|"negativo"|"neutro"|"critico" — tono editoriale reale
- criticita: stringa vuota se nessuna, altrimenti descrizione breve del problema reputazionale
- rilevanza: numero 1-5 (5=imprescindibile, 1=copia-incolla senza valore aggiunto)
- nota_redattore: massimo una frase su cosa rende unico questo articolo rispetto agli altri

Rispondi SOLO con un array JSON valido, senza markdown, senza testo prima o dopo."""

def _map_batch(batch: list, batch_idx: int) -> list:
    """Processa un batch di articoli e restituisce lista di dict estratti."""
    lines = []
    for i, a in enumerate(batch):
        testo = (a.get("testo_completo") or "")[:2000]
        lines.append(
            f"--- ARTICOLO {i+1} ---\n"
            f"TESTATA: {a.get('testata','')}\n"
            f"DATA: {a.get('data','')}\n"
            f"GIORNALISTA: {a.get('giornalista','')}\n"
            f"TITOLO: {a.get('titolo','')}\n"
            f"OCCHIELLO: {a.get('occhiello','')}\n"
            f"TESTO: {testo}"
        )

    prompt = "\n\n".join(lines)
    try:
        resp = ai.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": _MAP_SYSTEM},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.0,
            max_tokens=3000,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content.strip()
        # Il modello potrebbe restituire {"articles": [...]} o direttamente [...]
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
        # Cerca la prima lista nel dict
        for v in parsed.values():
            if isinstance(v, list):
                return v
        return []
    except Exception as e:
        print(f"[SPIZ MAP] batch {batch_idx} error: {e}")
        return []


def _map_articles(articles: list, batch_size: int = 5) -> list:
    """MAP phase: estrae dati strutturati da tutti gli articoli in batch."""
    all_extracted = []
    batches = [articles[i:i+batch_size] for i in range(0, len(articles), batch_size)]
    print(f"[SPIZ MAP] {len(articles)} articoli in {len(batches)} batch")
    for idx, batch in enumerate(batches):
        extracted = _map_batch(batch, idx)
        all_extracted.extend(extracted)
        print(f"[SPIZ MAP] batch {idx+1}/{len(batches)} → {len(extracted)} estratti")
    return all_extracted


# ══════════════════════════════════════════════════════════════════════
# REDUCE — Sintesi report da dati estratti
# ══════════════════════════════════════════════════════════════════════

_REDUCE_SYSTEM = """Sei SPIZ, analista senior di MAIM Public Diplomacy & Media Relations.

Ricevi:
1. Una richiesta dell'utente
2. Dati strutturati estratti da articoli (fase MAP)
3. Statistiche del corpus

Il tuo compito è scrivere un report giornalistico professionale.

REGOLE ASSOLUTE:
- Usa SOLO i dati forniti. Zero invenzioni.
- Ogni affermazione cita testata e data: (Corriere della Sera, 26/02/2026)
- Dati numerici: usa solo quelli presenti nei fatti_chiave
- Se un tema non è coperto: "Non disponibile nel corpus"
- Lingua: italiano professionale corporate. Zero emoji.

STRUTTURA OBBLIGATORIA per report:
## 1. PROFILO MEDIATICO
## 2. TEMI PRINCIPALI E ARTICOLI DA SEGNALARE
## 3. CRITICITÀ E RISCHI REPUTAZIONALI
## 4. SINTESI STRATEGICA

Per ogni sezione: paragrafi completi, non elenchi puntati piatti.
Segnala esplicitamente gli articoli più rilevanti (rilevanza 4-5) con testata, giornalista e perché.
"""

def _reduce_report(user_message: str, extracted: list, stats: dict, history: list = None) -> str:
    payload = {
        "richiesta_utente": user_message,
        "statistiche": stats,
        "articoli_analizzati": extracted,
    }
    messages = [{"role": "system", "content": _REDUCE_SYSTEM}]
    for msg in (history or [])[-4:]:
        if msg.get("role") in ("user", "assistant") and msg.get("content"):
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": json.dumps(payload, ensure_ascii=False)})

    resp = ai.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        temperature=0.1,
        max_tokens=8000,
    )
    return resp.choices[0].message.content.strip()


# ══════════════════════════════════════════════════════════════════════
# QUICK ANSWER — Per domande semplici, senza MAP
# ══════════════════════════════════════════════════════════════════════

_QUICK_SYSTEM = """Sei SPIZ, analista mediatico di MAIM.
Rispondi usando SOLO gli articoli del corpus fornito.
Cita sempre testata e data. Non inventare nulla.
Lingua: italiano professionale. Conciso e preciso."""

def _quick_answer(user_message: str, articles: list, stats: dict, history: list = None) -> str:
    # Corpus compatto per domande veloci
    lines = []
    for a in articles[:60]:
        testo = (a.get("testo_completo") or "")[:600]
        lines.append(
            f"[{a.get('data','')}] {a.get('testata','')} — {a.get('titolo','')}\n"
            f"Firma: {a.get('giornalista','')} | Tone: {a.get('tone','')} | Topic: {a.get('dominant_topic','')}\n"
            f"{testo}"
        )
    corpus_txt = "\n\n---\n\n".join(lines)
    stats_txt  = json.dumps(stats, ensure_ascii=False, indent=2)

    messages = [
        {"role": "system", "content": _QUICK_SYSTEM + f"\n\nSTATISTICHE:\n{stats_txt}\n\nCORPUS:\n{corpus_txt}"}
    ]
    for msg in (history or [])[-6:]:
        if msg.get("role") in ("user", "assistant") and msg.get("content"):
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
# DOCX GENERATOR
# ══════════════════════════════════════════════════════════════════════

def _generate_docx(report_text: str, stats: dict, extracted: list, topic: str) -> str | None:
    """
    Chiama docx_builder.js passando i dati come JSON.
    Ritorna il path al file .docx generato, o None se fallisce.
    """
    if not os.path.exists(_BUILDER_JS):
        print(f"[SPIZ DOCX] builder non trovato: {_BUILDER_JS}")
        return None

    payload = {
        "report_text": report_text,
        "stats":       stats,
        "extracted":   extracted,
        "topic":       topic,
        "date":        date.today().strftime("%d/%m/%Y"),
    }

    # Scrivi payload in file temporaneo
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
        payload_path = f.name

    out_path = payload_path.replace(".json", ".docx")

    try:
        result = subprocess.run(
            ["node", _BUILDER_JS, payload_path, out_path],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0 and os.path.exists(out_path):
            print(f"[SPIZ DOCX] generato: {out_path}")
            return out_path
        else:
            print(f"[SPIZ DOCX] errore: {result.stderr}")
            return None
    except Exception as e:
        print(f"[SPIZ DOCX] exception: {e}")
        return None
    finally:
        try: os.unlink(payload_path)
        except: pass


# ══════════════════════════════════════════════════════════════════════
# INTENT DETECTION
# ══════════════════════════════════════════════════════════════════════

_REPORT_KEYWORDS = [
    "report", "analisi", "profilo mediatico", "temi longevi", "sentiment",
    "sintesi strategica", "criticita", "reputazion", "documento", "redigi",
    "elabora", "due pagine", "tre pagine", "relazione", "panoramica",
    "overview", "sintesi", "rassegna", "cosa succede", "che succede",
    "raccontami", "spiegami tutto", "dimmi tutto",
]

_DOCX_KEYWORDS = [
    "documento", "word", "docx", "file", "scarica", "scaricabile",
    "due pagine", "tre pagine", "report", "relazione",
]

def _wants_report(message: str) -> bool:
    msg = message.lower()
    return any(kw in msg for kw in _REPORT_KEYWORDS)

def _wants_docx(message: str) -> bool:
    msg = message.lower()
    return any(kw in msg for kw in _DOCX_KEYWORDS)


# ══════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

def ask_spiz(
    message:     str,
    history:     list = None,
    context:     str  = "general",
    csv_content: str  = None,   # stringa CSV allegata dall'utente
) -> dict:
    """
    Parametri:
      message:     testo dell'utente
      history:     lista [{role, content}] della conversazione
      context:     "today"|"week"|"month"|"year"|"general"
      csv_content: contenuto grezzo del CSV allegato (opzionale)

    Ritorna:
      {
        response:       str   — testo del report/risposta
        is_report:      bool
        docx_path:      str|None — path al .docx se generato
        articles_used:  int
        total_period:   int
        source:         "csv"|"db"
      }
    """
    if not message or len(message.strip()) < 2:
        return {"error": "Messaggio troppo corto."}

    # ── 1. CARICA ARTICOLI ────────────────────────────────────────────
    if csv_content:
        all_articles = _load_from_csv(csv_content)
        source = "csv"
        print(f"[SPIZ] CSV: {len(all_articles)} articoli")
    else:
        from_date, to_date = _date_range(context, message)
        all_articles = _load_from_db(from_date, to_date)
        source = "db"
        print(f"[SPIZ] DB: {len(all_articles)} articoli ({from_date} → {to_date})")

    if not all_articles:
        return {
            "response": (
                "Nessun articolo trovato nel corpus.\n\n"
                + ("Il CSV allegato non contiene articoli leggibili." if csv_content
                   else f"Nessun articolo nel database per il periodo richiesto.")
            ),
            "is_report": False, "docx_path": None,
            "articles_used": 0, "total_period": 0, "source": source,
        }

    # ── 2. FILTRA / CERCA ─────────────────────────────────────────────
    journalist = _extract_journalist(message)
    topic      = None if journalist else _extract_topic(message)

    print(f"[SPIZ] journalist={journalist!r} topic={topic!r}")

    if journalist:
        filtered = _filter_journalist(all_articles, journalist)
        if not filtered:
            # Fallback su cognome
            for part in journalist.split():
                if len(part) > 3:
                    filtered = _filter_journalist(all_articles, part)
                    if filtered: break
        if not filtered:
            return {
                "response": f"Nessun articolo trovato per '{journalist}' nel corpus ({len(all_articles)} articoli totali).",
                "is_report": False, "docx_path": None,
                "articles_used": 0, "total_period": len(all_articles), "source": source,
            }
    elif topic:
        filtered = _search_scored(all_articles, topic)
        print(f"[SPIZ] topic search '{topic}': {len(filtered)} rilevanti")
        if not filtered:
            filtered = all_articles
    else:
        filtered = all_articles

    stats = _stats(filtered)
    is_report = _wants_report(message)
    wants_docx = _wants_docx(message)

    # ── 3. PIPELINE ───────────────────────────────────────────────────
    if is_report:
        # MAP+REDUCE su max 40 articoli (i più rilevanti)
        to_analyze = filtered[:40]
        print(f"[SPIZ] MAP su {len(to_analyze)} articoli...")
        extracted = _map_articles(to_analyze, batch_size=5)
        print(f"[SPIZ] REDUCE...")
        report_text = _reduce_report(message, extracted, stats, history)
    else:
        # Risposta veloce senza MAP
        extracted   = []
        report_text = _quick_answer(message, filtered, stats, history)

    # ── 4. GENERA DOCX (se richiesto) ─────────────────────────────────
    docx_path = None
    if wants_docx and is_report:
        print("[SPIZ] Genero .docx...")
        docx_path = _generate_docx(
            report_text = report_text,
            stats       = stats,
            extracted   = extracted,
            topic       = topic or journalist or "Report",
        )

    return {
        "response":      report_text,
        "is_report":     is_report,
        "docx_path":     docx_path,
        "articles_used": len(filtered),
        "total_period":  len(all_articles),
        "source":        source,
    }