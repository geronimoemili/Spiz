"""
api/chat.py - SPIZ AI v5
Schema reale verificato su Supabase.
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

SYSTEM_ROLE = """Sei SPIZ, analista senior di MAIM Public Diplomacy & Media Relations.

REGOLE ASSOLUTE:
1. Rispondi ESCLUSIVAMENTE usando gli articoli forniti nel corpus qui sotto.
2. Non usare MAI la tua conoscenza generale o inventare fatti.
3. Ogni affermazione deve citare testata e data dell'articolo da cui proviene.
4. Se un'informazione non e' nel corpus, dillo esplicitamente: "Non ho articoli su questo nel periodo."
5. Non inventare percentuali, dichiarazioni o dati non presenti.

STILE: Italiano professionale corporate. Nessuna emoji. Nessuna formula generica.

PER REPORT STRUTTURATI usa esattamente:
## 1. PROFILO MEDIATICO
## 2. INTERVISTE E PRESENZA VERTICI
## 3. TEMI LONGEVI
## 4. NOTIZIE FINANZIARIE E CORPORATE
## 5. GOVERNANCE E MANAGEMENT
## 6. FOCUS TERRITORIALE
## 7. CRITICITA' REPUTAZIONALI
## 8. ANALISI DEL SENTIMENT
## 9. COMUNICAZIONE ISTITUZIONALE
## 10. SINTESI STRATEGICA

Per ogni sezione cita articoli specifici con testata e data.
"""


def days_from_message(msg):
    m = msg.lower()
    if re.search(r"oggi|odiern", m): return 0
    if re.search(r"ultime?\s*24.?ore|ieri", m): return 1
    if re.search(r"ultim[ie]\s*[23]\s*giorn", m): return 3
    if re.search(r"ultim[ie]\s*[45]\s*giorn", m): return 5
    if re.search(r"ultim[ie]\s*[67]\s*giorn|ultima?\s*settiman|ultimi\s*7", m): return 7
    if re.search(r"ultim[ie]\s*10\s*giorn", m): return 10
    if re.search(r"ultim[ie]\s*15\s*giorn|due\s*settiman", m): return 15
    if re.search(r"ultim[ie]\s*[23]0\s*giorn|ultimo\s*mese|ultim[ie]\s*30", m): return 30
    if re.search(r"ultim[ie]\s*[23]\s*mesi|ultimi\s*[69]0\s*giorn", m): return 90
    if re.search(r"ultim[ie]\s*[46]\s*mesi", m): return 180
    if re.search(r"ultimo\s*anno|ultimi\s*12\s*mesi", m): return 365
    return None


def get_dates(context, message):
    days = days_from_message(message)
    if days is None:
        ctx = {"today": 0, "week": 7, "month": 30, "year": 365, "general": 90}
        days = ctx.get(context, 90)
    today = date.today()
    if days == 0:
        return today.isoformat(), today.isoformat()
    return (today - timedelta(days=days)).isoformat(), today.isoformat()


def load_all(from_date, to_date):
    try:
        res = (supabase.table("articles")
               .select(COLS)
               .gte("data", from_date)
               .lte("data", to_date)
               .order("data", desc=True)
               .limit(1000)
               .execute())
        return res.data or []
    except Exception as e:
        print("load_all error: " + str(e))
        return []


def filter_by_keyword(articles, keyword):
    kw = keyword.lower()
    result = []
    for a in articles:
        haystack = " ".join([
            a.get("titolo") or "",
            a.get("occhiello") or "",
            a.get("sottotitolo") or "",
            a.get("testo_completo") or "",
            a.get("macrosettori") or "",
            a.get("dominant_topic") or "",
        ]).lower()
        if kw in haystack:
            result.append(a)
    return result


def filter_by_journalist(articles, name):
    name_l = name.lower()
    return [a for a in articles if name_l in (a.get("giornalista") or "").lower()]


def extract_targets(message):
    """Estrae giornalista e/o keyword dal messaggio."""
    msg = message.lower()
    journalist = None
    keyword = None

    # Giornalista - richiede trigger espliciti
    m = re.search(
        r"(?:articoli\s+(?:di|scritti\s+da|firmati\s+da)|"
        r"scritti\s+da|firmati\s+da|"
        r"cosa\s+ha\s+scritto|"
        r"ultimi\s+\d*\s*articoli\s+(?:di|da))\s+"
        r"([a-z]+(?:\s+[a-z]+){1,2})", msg
    )
    if m:
        candidate = m.group(1).strip()
        bad = ["ultima", "ultim", "articol", "settim", "giorn", "mese", "anno",
               "realizza", "fammi", "dammi", "fai", "crea", "scrivi"]
        if not any(b in candidate for b in bad):
            journalist = candidate

    # Keyword/tema/azienda
    m = re.search(
        r"(?:su|tema|riguardante|relativo\s+a|argomento)\s+"
        r"([a-z0-9àèéìòù\s\-&]+?)"
        r"(?:\s+(?:ultim|negli|nell|oggi|della|base|usando|articol)|[?!.,]|$)",
        msg
    )
    if m and not journalist:
        candidate = m.group(1).strip().rstrip("?!., ")
        bad = ["ultima", "ultim", "articol", "settim", "giorn", "mese", "anno"]
        if len(candidate) > 2 and not any(b in candidate for b in bad):
            keyword = candidate

    return journalist, keyword


def fmt_corpus(articles, max_chars=700):
    if not articles:
        return "Nessun articolo trovato nel periodo selezionato."
    blocks = []
    for a in articles:
        testo = (a.get("testo_completo") or "").strip()
        testo_t = testo[:max_chars] + ("..." if len(testo) > max_chars else "")
        block = (
            "[" + str(a.get("data","")) + "] " + str(a.get("testata","N/D")) +
            " | Firma: " + str(a.get("giornalista","Anonimo")) +
            " | Tone: " + str(a.get("tone","")) +
            " | Topic: " + str(a.get("dominant_topic","")) +
            " | Settori: " + str(a.get("macrosettori","")) + "\n"
            "TITOLO: " + str(a.get("titolo",""))
        )
        if a.get("occhiello"):
            block += "\nOCCHIELLO: " + str(a["occhiello"])
        if testo_t:
            block += "\nTESTO: " + testo_t
        blocks.append(block)
    return "\n\n---\n\n".join(blocks)


def fmt_stats(articles):
    if not articles:
        return "Nessun articolo."
    testate = Counter(a.get("testata","") for a in articles if a.get("testata"))
    giornalisti = Counter(a.get("giornalista","") for a in articles if a.get("giornalista"))
    tones = Counter(a.get("tone","") for a in articles if a.get("tone"))
    topics = Counter(a.get("dominant_topic","") for a in articles if a.get("dominant_topic"))
    settori_all = []
    for a in articles:
        for s in (a.get("macrosettori") or "").split(","):
            s = s.strip()
            if s: settori_all.append(s)
    settori = Counter(settori_all)
    monthly = Counter()
    for a in articles:
        d = a.get("data","")
        if d and len(d) >= 7:
            monthly[d[:7]] += 1
    tone_tot = sum(tones.values()) or 1
    dates = [a.get("data","") for a in articles if a.get("data")]
    def top(c, n=20):
        return ", ".join(k + "(" + str(v) + ")" for k,v in c.most_common(n) if k)
    return (
        "TOTALE: " + str(len(articles)) + " articoli\n"
        "PERIODO: " + (min(dates) if dates else "?") + " -> " + (max(dates) if dates else "?") + "\n"
        "TESTATE: " + top(testate) + "\n"
        "GIORNALISTI PIU ATTIVI: " + top(giornalisti) + "\n"
        "MACROSETTORI: " + top(settori) + "\n"
        "TOPIC DOMINANTI: " + top(topics, 10) + "\n"
        "SENTIMENT: " + ", ".join(k + ": " + str(round(v/tone_tot*100)) + "%" for k,v in tones.most_common() if k) + "\n"
        "ANDAMENTO MENSILE: " + ", ".join(m + ":" + str(c) for m,c in sorted(monthly.items()))
    )


def ask_spiz(message, history=None, context="general"):
    if not message or len(message.strip()) < 2:
        return {"error": "Messaggio troppo corto."}

    from_date, to_date = get_dates(context, message)
    journalist, keyword = extract_targets(message)

    # Carica tutto il periodo
    all_articles = load_all(from_date, to_date)
    print("SPIZ loaded=" + str(len(all_articles)) + " from=" + from_date + " to=" + to_date)

    # Filtra se serve
    if journalist:
        filtered = filter_by_journalist(all_articles, journalist)
        print("Journalist filter '" + journalist + "': " + str(len(filtered)))
        if not filtered:
            filtered = all_articles  # fallback su tutto
    elif keyword:
        filtered = filter_by_keyword(all_articles, keyword)
        print("Keyword filter '" + keyword + "': " + str(len(filtered)))
        if not filtered:
            filtered = all_articles  # fallback su tutto
    else:
        filtered = all_articles

    stats = fmt_stats(all_articles)
    corpus = fmt_corpus(filtered[:200])

    system = (
        SYSTEM_ROLE + "\n\n"
        "=== STATISTICHE DEL CORPUS ===\n"
        + stats + "\n\n"
        "=== ARTICOLI (totale filtrati: " + str(len(filtered)) + ") ===\n\n"
        + corpus
    )

    messages = [{"role": "system", "content": system}]
    for msg in (history or [])[-10:]:
        if msg.get("role") in ("user", "assistant") and msg.get("content"):
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": message})

    msg_l = message.lower()
    is_report = any(kw in msg_l for kw in [
        "report", "analisi completa", "profilo mediatico", "temi longevi",
        "sentiment", "sintesi strategica", "criticita", "reputazion",
        "documento", "redigi", "elabora"
    ])
    max_tok = 8000 if is_report else 2000

    try:
        resp = ai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.1,
            max_tokens=max_tok
        )
        return {"response": resp.choices[0].message.content.strip(), "is_report": is_report}
    except Exception as e1:
        print("gpt-4o error: " + str(e1))
        try:
            resp = ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                temperature=0.1,
                max_tokens=min(max_tok, 4000)
            )
            return {"response": resp.choices[0].message.content.strip(), "is_report": is_report}
        except Exception as e2:
            return {"error": "Errore AI: " + str(e2)}