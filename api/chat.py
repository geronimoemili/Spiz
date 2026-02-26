"""
api/chat.py - SPIZ AI v6
Rewrite completo. Schema verificato su Supabase.
"""

import os
import re
from datetime import date, timedelta
from collections import Counter
from openai import OpenAI
from services.database import supabase

ai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

COLS = (
    "id, testata, data, giornalista, occhiello, titolo, sottotitolo, "
    "testo_completo, macrosettori, tipologia_articolo, tone, "
    "dominant_topic, reputational_risk, political_risk, ave, tipo_fonte"
)

# ─────────────────────────────────────────────────────────────────────
# PARSING TEMPORALE  (supporta: giorni/gg/g, settimane, mesi, anno)
# ─────────────────────────────────────────────────────────────────────
_TIME = [
    (r"oggi|odiern",                                                          0),
    (r"ultime?\s*24.?ore|ieri",                                               1),
    (r"ultim[ie]\s*[23]\s*(?:giorn|gg\b|g\b)",                               3),
    (r"ultim[ie]\s*[45]\s*(?:giorn|gg\b|g\b)",                               5),
    (r"ultim[ie]\s*(?:[67]\s*(?:giorn|gg\b|g\b)|settiman|7\s*(?:giorn|gg))", 7),
    (r"ultim[ie]\s*10\s*(?:giorn|gg\b|g\b)",                                10),
    (r"ultim[ie]\s*15\s*(?:giorn|gg\b|g\b)|due\s*settiman",                 15),
    (r"ultim[ie]\s*20\s*(?:giorn|gg\b|g\b)",                                20),
    (r"ultim[ie]\s*(?:30\s*(?:giorn|gg\b|g\b)?|30)\b|ultimo\s*mese|mese\s*scors|ultim[ie]\s*30", 30),
    (r"ultim[ie]\s*45\s*(?:giorn|gg\b|g\b)",                                45),
    (r"ultim[ie]\s*(?:60\s*(?:giorn|gg\b|g\b)|2\s*mesi)",                   60),
    (r"ultim[ie]\s*(?:90\s*(?:giorn|gg\b|g\b)|3\s*mesi)",                   90),
    (r"ultim[ie]\s*(?:6\s*mesi|180\s*(?:giorn|gg\b))",                     180),
    (r"ultimo\s*anno|ultim[ie]\s*12\s*mesi|ultim[ie]\s*365",               365),
]

def _parse_days(msg: str):
    m = msg.lower()
    for pattern, days in _TIME:
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


# ─────────────────────────────────────────────────────────────────────
# ESTRAZIONE TOPIC / GIORNALISTA
# ─────────────────────────────────────────────────────────────────────
_STOP = {
    "ultima", "ultim", "articol", "settim", "giorn", "mese", "anno",
    "realizza", "fammi", "dammi", "fai", "crea", "scrivi", "redigi",
    "due", "tre", "pagin", "vuoi", "voglio", "sapere", "succede",
    "quali", "interessant", "criticita", "quanti", "voglio", "utilizza",
}

def _extract_topic(message: str):
    msg = message.lower().strip()

    # Pattern espliciti con preposizioni/parole chiave
    patterns = [
        # "sul tema X", "il tema X", "tema X"
        r"(?:sul?|il|la|dello?|della|dei|degli|delle)\s+tema\s+([a-z0-9àèéìòùäöü\s\-&/]+?)(?:\s*[,?.!]|$|\s+(?:ultim|negli|nell|nel|degli|della|delle|voglio|dammi|fammi|quanti|utilizza|crea|redigi))",
        # "riguardante X", "relativo a X", "su X"
        r"(?:riguardant[ei]|relativ[oi]\s+a|parlano\s+di|riguardo)\s+([a-z0-9àèéìòùäöü\s\-&/]+?)(?:\s*[,?.!]|$|\s+(?:ultim|negli|nell|nel|degli|della|delle|voglio|dammi|fammi|quanti|utilizza))",
        # "documento/report/analisi su/sul/della X"
        r"(?:documento|report|analisi|sintesi|profilo|relazione|panoramica|overview)\s+(?:su|sul|sulla|sullo|sugli|di|del|della|dello|dei|sulle)\s+([a-z0-9àèéìòùäöü\s\-&/]+?)(?:\s*[,?.!]|$|\s+(?:ultim|negli|nell|nel|degli|della|delle|voglio|dammi|fammi|quanti|utilizza|crea|redigi))",
        # "scrivi di X", "parla di X", "mostrami X"
        r"(?:scrivi|parla|mostrami|cercami|trovami)\s+(?:di\s+)?([a-z0-9àèéìòùäöü\s\-&/]+?)(?:\s*[,?.!]|$|\s+(?:ultim|negli|nell|nel|degli|della|delle|voglio|dammi|fammi|quanti|utilizza))",
    ]

    for pat in patterns:
        m = re.search(pat, msg)
        if m:
            raw = m.group(1).strip().rstrip("., ")
            words = raw.split()
            # Rimuovi stopwords dalla fine
            while words and any(s in words[-1] for s in _STOP):
                words.pop()
            candidate = " ".join(words).strip()
            if len(candidate) >= 2:
                return candidate

    return None


def _extract_journalist(message: str):
    msg = message.lower()
    patterns = [
        r"articoli\s+(?:di|scritti\s+da|firmati\s+da)\s+([a-z]+(?:\s+[a-z]+){1,2})",
        r"scritti\s+da\s+([a-z]+(?:\s+[a-z]+){1,2})",
        r"firmati\s+da\s+([a-z]+(?:\s+[a-z]+){1,2})",
        r"cosa\s+ha\s+scritto\s+([a-z]+(?:\s+[a-z]+){1,2})",
        r"ultimi\s+\d+\s+articoli\s+(?:di|da)\s+([a-z]+(?:\s+[a-z]+){1,2})",
    ]
    for pat in patterns:
        m = re.search(pat, msg)
        if m:
            candidate = m.group(1).strip()
            if not any(b in candidate for b in _STOP):
                return candidate
    return None


# ─────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────
def _load(from_date: str, to_date: str, limit: int = 2000) -> list:
    try:
        res = (
            supabase.table("articles")
            .select(COLS)
            .gte("data", from_date)
            .lte("data", to_date)
            .order("data", desc=True)
            .limit(limit)
            .execute()
        )
        return res.data or []
    except Exception as e:
        print(f"[SPIZ] load error: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────
# RICERCA / FILTRO
# ─────────────────────────────────────────────────────────────────────
def _search(articles: list, query: str) -> list:
    """
    Full-text search con scoring.
    - Cerca ogni parola della query (>= 2 char) in tutti i campi.
    - Ordina per numero di parole trovate (decrescente).
    - Restituisce articoli con almeno 1 match.
    """
    if not query or not articles:
        return articles

    words = [w.lower() for w in re.split(r'\W+', query) if len(w) >= 2]
    if not words:
        return articles

    scored = []
    for a in articles:
        haystack = " ".join(filter(None, [
            a.get("titolo"),
            a.get("occhiello"),
            a.get("sottotitolo"),
            a.get("testo_completo"),
            a.get("macrosettori"),
            a.get("dominant_topic"),
        ])).lower()

        # Peso doppio per titolo/occhiello
        title_hay = " ".join(filter(None, [
            a.get("titolo"),
            a.get("occhiello"),
        ])).lower()

        score = sum(2 if w in title_hay else (1 if w in haystack else 0) for w in words)
        if score > 0:
            scored.append((score, a))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [a for _, a in scored]


def _filter_journalist(articles: list, name: str) -> list:
    nl = name.lower()
    return [a for a in articles if nl in (a.get("giornalista") or "").lower()]


# ─────────────────────────────────────────────────────────────────────
# FORMATTAZIONE
# ─────────────────────────────────────────────────────────────────────
def _stats(articles: list) -> str:
    if not articles:
        return "Nessun articolo."

    testate     = Counter(a.get("testata","") for a in articles if a.get("testata"))
    giornalisti = Counter(a.get("giornalista","") for a in articles if a.get("giornalista"))
    tones       = Counter(a.get("tone","") for a in articles if a.get("tone"))
    topics      = Counter(a.get("dominant_topic","") for a in articles if a.get("dominant_topic"))
    monthly     = Counter()
    settori_all = []

    for a in articles:
        d = a.get("data", "")
        if d and len(d) >= 7:
            monthly[d[:7]] += 1
        for s in (a.get("macrosettori") or "").split(","):
            s = s.strip()
            if s:
                settori_all.append(s)

    settori  = Counter(settori_all)
    tone_tot = sum(tones.values()) or 1
    dates    = [a.get("data","") for a in articles if a.get("data")]

    def top(c, n=15):
        return ", ".join(f"{k}({v})" for k, v in c.most_common(n) if k)

    return (
        f"TOTALE: {len(articles)} articoli\n"
        f"PERIODO: {min(dates) if dates else '?'} → {max(dates) if dates else '?'}\n"
        f"TESTATE: {top(testate)}\n"
        f"GIORNALISTI PIÙ ATTIVI: {top(giornalisti)}\n"
        f"MACROSETTORI: {top(settori)}\n"
        f"TOPIC DOMINANTI: {top(topics, 8)}\n"
        f"SENTIMENT: {', '.join(f'{k}: {round(v/tone_tot*100)}%' for k, v in tones.most_common() if k)}\n"
        f"ANDAMENTO MENSILE: {', '.join(f'{m}:{c}' for m, c in sorted(monthly.items()))}"
    )


def _corpus(articles: list, chars_per_article: int = 1500, max_articles: int = 120) -> str:
    if not articles:
        return "NESSUN ARTICOLO TROVATO NEL CORPUS."

    blocks = []
    for a in articles[:max_articles]:
        testo = (a.get("testo_completo") or "").strip()
        testo_t = testo[:chars_per_article] + ("…" if len(testo) > chars_per_article else "")

        parts = [
            f"[{a.get('data','')}] {a.get('testata','N/D')} | Firma: {a.get('giornalista','Anonimo')}",
            f"Tone: {a.get('tone','')} | Topic: {a.get('dominant_topic','')} | Settori: {a.get('macrosettori','')}",
            f"TITOLO: {a.get('titolo','')}",
        ]
        if a.get("occhiello"):
            parts.append(f"OCCHIELLO: {a['occhiello']}")
        if a.get("sottotitolo"):
            parts.append(f"SOTTOTITOLO: {a['sottotitolo']}")
        if testo_t:
            parts.append(f"TESTO: {testo_t}")

        blocks.append("\n".join(parts))

    header = f"[CORPUS: {len(articles)} articoli trovati, includo i primi {min(len(articles), max_articles)}]\n\n"
    return header + "\n\n---\n\n".join(blocks)


# ─────────────────────────────────────────────────────────────────────
# INTENT
# ─────────────────────────────────────────────────────────────────────
_REPORT_KW = [
    "report", "analisi completa", "profilo mediatico", "temi longevi",
    "sentiment", "sintesi strategica", "criticita", "reputazion",
    "documento", "redigi", "elabora", "due pagine", "tre pagine",
    "relazione", "panoramica", "overview", "sintesi", "rassegna",
]

def _is_report(message: str) -> bool:
    msg = message.lower()
    return any(kw in msg for kw in _REPORT_KW)


# ─────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────────────
_SYSTEM = """Sei SPIZ, analista senior di MAIM Public Diplomacy & Media Relations.

REGOLE FONDAMENTALI — NON DEROGABILI:
1. Rispondi ESCLUSIVAMENTE usando gli articoli del corpus fornito.
2. MAI usare conoscenza generale o inventare fatti, date, dichiarazioni.
3. Ogni affermazione deve citare: testata e data. Formato: (Testata, GG/MM/AAAA).
4. Se un'informazione non è nel corpus: scrivi esattamente "Non disponibile nel corpus".
5. Non inventare percentuali. Usa solo i dati delle STATISTICHE fornite.
6. Se il corpus contiene articoli, DEVI usarli: non dire mai che non hai dati.

FORMATO REPORT (se richiesto):
## 1. PROFILO MEDIATICO
## 2. TEMI PRINCIPALI
## 3. CRITICITÀ E RISCHI
## 4. SENTIMENT E TONO
## 5. GIORNALISTI E TESTATE CHIAVE
## 6. SINTESI STRATEGICA

Lingua: italiano professionale. Nessuna emoji. Citazioni specifiche obbligatorie.
"""


# ─────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────
def ask_spiz(message: str, history: list = None, context: str = "general") -> dict:
    if not message or len(message.strip()) < 2:
        return {"error": "Messaggio troppo corto."}

    # 1. Range temporale
    from_date, to_date = _date_range(context, message)

    # 2. Estrai target
    journalist = _extract_journalist(message)
    topic = None if journalist else _extract_topic(message)

    print(f"[SPIZ] context={context} from={from_date} to={to_date} journalist={journalist!r} topic={topic!r}")

    # 3. Carica dal DB
    all_articles = _load(from_date, to_date, limit=2000)
    print(f"[SPIZ] loaded={len(all_articles)} articoli dal DB")

    if not all_articles:
        return {
            "response": (
                f"Nessun articolo trovato nel database per il periodo "
                f"{from_date} → {to_date}.\n\n"
                "Verifica che il database contenga articoli per questo intervallo "
                "o amplia il periodo di ricerca."
            ),
            "is_report": False,
            "articles_used": 0,
            "total_period": 0,
        }

    # 4. Filtra/cerca
    if journalist:
        filtered = _filter_journalist(all_articles, journalist)
        print(f"[SPIZ] journalist filter: {len(filtered)} risultati")
        if not filtered:
            # Prova ricerca parziale
            parts = journalist.split()
            for part in parts:
                if len(part) > 3:
                    filtered = _filter_journalist(all_articles, part)
                    if filtered:
                        print(f"[SPIZ] partial match '{part}': {len(filtered)}")
                        break
        if not filtered:
            return {
                "response": (
                    f"Nessun articolo trovato per il giornalista '{journalist}' "
                    f"nel periodo {from_date} → {to_date}.\n\n"
                    f"Nel corpus ci sono {len(all_articles)} articoli totali. "
                    "Il nome potrebbe essere scritto diversamente nel database."
                ),
                "is_report": False,
                "articles_used": 0,
                "total_period": len(all_articles),
            }

    elif topic:
        filtered = _search(all_articles, topic)
        print(f"[SPIZ] topic search '{topic}': {len(filtered)} rilevanti su {len(all_articles)}")

        if not filtered:
            # Nessun match: usa tutto il corpus ma aggiungi nota nel sistema
            filtered = all_articles
            print(f"[SPIZ] no match for topic, using full corpus")
    else:
        filtered = all_articles

    # 5. Costruisci corpus
    is_report = _is_report(message)

    if is_report:
        chars    = 2000
        max_arts = 100
        max_tok  = 8000
    else:
        chars    = 1000
        max_arts = 80
        max_tok  = 2000

    corpus_txt = _corpus(filtered, chars_per_article=chars, max_articles=max_arts)
    stats_txt  = _stats(filtered)

    # 6. Build messages
    system_content = (
        _SYSTEM + "\n\n"
        "=== STATISTICHE DEL CORPUS ===\n" + stats_txt + "\n\n"
        "=== ARTICOLI ===\n" + corpus_txt
    )

    messages = [{"role": "system", "content": system_content}]
    for msg in (history or [])[-6:]:
        if msg.get("role") in ("user", "assistant") and msg.get("content"):
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": message})

    # 7. Call AI
    try:
        resp = ai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.1,
            max_tokens=max_tok,
        )
        return {
            "response": resp.choices[0].message.content.strip(),
            "is_report": is_report,
            "articles_used": len(filtered),
            "total_period": len(all_articles),
        }
    except Exception as e1:
        print(f"[SPIZ] gpt-4o error: {e1}")
        try:
            resp = ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                temperature=0.1,
                max_tokens=min(max_tok, 4000),
            )
            return {
                "response": resp.choices[0].message.content.strip(),
                "is_report": is_report,
                "articles_used": len(filtered),
                "total_period": len(all_articles),
            }
        except Exception as e2:
            return {"error": f"Errore AI: {e2}"}