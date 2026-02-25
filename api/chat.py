"""
api/chat.py - SPIZ AI Analysis v4
Retrieval mirato con parsing temporale dal messaggio.
"""

import os
import re
from datetime import date, timedelta
from collections import Counter
from openai import OpenAI
from services.database import supabase

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

CONTEXT_DAYS = {"today": 0, "week": 7, "month": 30, "year": 365, "general": 90}

REPORT_KEYWORDS = [
    "report", "analisi completa", "sentiment", "profilo mediatico",
    "criticita", "sintesi strategica", "long running", "governance",
    "territoriale", "istituzional", "reputazion", "media narrative",
    "temi longevi", "temi ricorrenti", "comunicazione istituzionale",
    "focus territoriale", "analisi sentiment", "presenza dei vertici",
    "redigi", "elabora", "fai un report", "fai un analisi",
    "analizza la copertura", "analizza il periodo", "analisi reputazionale",
    "documento giornalistico", "documento su",
]

BASE_ROLE = (
    "Sei SPIZ, analista senior di MAIM Public Diplomacy & Media Relations.\n\n"
    "REGOLA ASSOLUTA: Rispondi SOLO usando i dati forniti qui sotto. "
    "Non usare MAI la tua conoscenza generale o inventare fatti. "
    "Cita sempre testata, data e titolo reali. "
    "Se un'informazione non e' nel corpus, dillo esplicitamente.\n\n"
)

REPORT_STRUCT = (
    "Struttura report:\n"
    "1. PROFILO MEDIATICO - ruolo attribuito, percezione per tipo stampa, temi frequenti, evoluzione cronologica\n"
    "2. INTERVISTE VERTICI - testata, data, firma, tema, tono, messaggio, esposizione reputazionale\n"
    "3. TEMI LONGEVI - per macro-area: persistenti/emergenti/in diminuzione\n"
    "4. NOTIZIE FINANZIARIE E CORPORATE\n"
    "5. GOVERNANCE E MANAGEMENT\n"
    "6. FOCUS TERRITORIALE - tabella territorio|attenzione|conflittualita'|trend\n"
    "7. CRITICITA' REPUTAZIONALI - testata|tema|tono|impatto|propagazione\n"
    "8. SENTIMENT - % positivo/neutro/negativo, driver, rischio\n"
    "9. COMUNICAZIONE ISTITUZIONALE\n"
    "10. SINTESI STRATEGICA - tabella territori|priorita'|azione; rischi; opportunita'\n\n"
    "Usa ## sezioni, ### sottosezioni, **grassetto** per evidenze, tabelle markdown. Nessuna emoji.\n\n"
)


# ─────────────────────────────────────────────────────────────────────────────
# PARSING TEMPORALE DAL MESSAGGIO
# ─────────────────────────────────────────────────────────────────────────────

def parse_time_from_message(message):
    msg = message.lower()
    today = date.today()
    patterns = [
        (r"oggi|odiern", 0),
        (r"ultime?\s*24\s*ore", 1),
        (r"ieri", 2),
        (r"ultim[ie]\s*[23]\s*giorn", 3),
        (r"ultim[ie]\s*[45]\s*giorn", 5),
        (r"ultim[ie]\s*[67]\s*giorn|ultima?\s*settiman|ultimi\s*7\s*giorn|ultime\s*7", 7),
        (r"ultim[ie]\s*10\s*giorn", 10),
        (r"ultim[ie]\s*15\s*giorn|ultime?\s*due\s*settiman", 15),
        (r"ultim[ie]\s*[23]0\s*giorn|ultimo\s*mese|ultim[ie]\s*30", 30),
        (r"ultim[ie]\s*[23]\s*mesi|ultimi\s*90\s*giorn", 90),
        (r"ultim[ie]\s*[46]\s*mesi", 180),
        (r"ultimo\s*anno|ultim[ie]\s*12\s*mesi", 365),
    ]
    for pattern, days in patterns:
        if re.search(pattern, msg):
            if days == 0:
                return today.isoformat(), today.isoformat()
            return (today - timedelta(days=days)).isoformat(), today.isoformat()
    return None, None


def date_range(context, message=""):
    from_msg, to_msg = parse_time_from_message(message)
    if from_msg:
        return from_msg, to_msg
    days = CONTEXT_DAYS.get(context, 90)
    today = date.today().isoformat()
    if days == 0:
        return today, today
    return (date.today() - timedelta(days=days)).isoformat(), today


# ─────────────────────────────────────────────────────────────────────────────
# INTENT DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def detect_intent(message):
    msg = message.lower().strip()

    if any(kw in msg for kw in REPORT_KEYWORDS):
        m = re.search(
            r"(?:report|analisi|documento|articolo|copertura)\s+(?:su|di|per)\s+"
            r"([a-z0-9\s\'\-&\.]+?)(?:\s+(?:sulla|usando|degli|dell|negli|nell|base|articol|ultima|dei)|[?!]|$)",
            msg
        )
        if m:
            entity = m.group(1).strip().rstrip("?!., ")
            if len(entity) > 2 and not any(b in entity for b in ["ultima", "ultim", "articol", "giorn"]):
                return "report", {"name": entity}
        return "report", {}

    # Giornalista
    j_triggers = [
        r"articoli\s+(?:di|scritti da|firmati da)\s+([a-z]+\s+[a-z]+)",
        r"(?:scritti|firmati)\s+da\s+([a-z]+\s+[a-z]+)",
        r"cosa\s+ha\s+scritto\s+([a-z]+\s+[a-z]+)",
        r"ultimi\s+\d*\s*articoli\s+(?:di|da)\s+([a-z]+\s+[a-z]+)",
    ]
    bad_j = ["ultima", "ultimi", "articol", "settiman", "giorni", "mese", "anno",
             "realizza", "fammi", "dammi", "fai", "crea", "scrivi", "produci", "usando"]
    for pat in j_triggers:
        m = re.search(pat, msg)
        if m:
            name = m.group(1).strip()
            if len(name) > 4 and not any(b in name for b in bad_j):
                return "journalist", {"name": name}

    # Tema generico (non cliente specifico)
    topic_kws = ["tema ", "argomento ", "notizie su ", "articoli su ", "trattato il tema",
                 "riguardante ", "relativo a ", "fotovoltaic", "rinnovabil", "banche",
                 "energia", "finanza", "politica", "sanita", "tecnologia"]
    for kw in topic_kws:
        if kw in msg:
            # Estrai il tema
            m = re.search(r"tema\s+([a-z0-9\s]+?)(?:\s+(?:negli|nell|ultim|oggi)|[?!]|$)", msg)
            if m:
                return "topic", {"name": m.group(1).strip()}
            m2 = re.search(r"(?:notizie|articoli)\s+su\s+([a-z0-9\s]+?)(?:\s+(?:negli|nell|ultim|oggi)|[?!]|$)", msg)
            if m2:
                return "topic", {"name": m2.group(1).strip()}
            # Estrai keyword generica
            for kw2 in ["fotovoltaic", "rinnovabil", "banche", "energia", "finanza"]:
                if kw2 in msg:
                    return "topic", {"name": kw2}

    # Cliente/azienda specifica
    c_patterns = [
        r"(?:report|analisi|notizie|copertura)\s+su\s+([a-z0-9\s\'\-&\.]+?)(?:\s+(?:nell|negli|degli|sulla|usando|base|articol|ultima)|[?!]|$)",
        r"cosa\s+(?:dicono|dice|ha detto la stampa)\s+.*?su\s+([a-z0-9\s]+?)(?:\s|$)",
    ]
    bad_e = ["ultima settimana", "ultimo mese", "oggi", "ieri", "articoli", "notizie", "ultim"]
    for pat in c_patterns:
        m = re.search(pat, msg)
        if m:
            entity = m.group(1).strip().rstrip("?!., ")
            if len(entity) > 2 and not any(b in entity for b in bad_e):
                return "client", {"name": entity}

    # Statistiche
    if any(kw in msg for kw in ["quanti articoli", "quali testate", "top giornalist",
                                  "pubblicato di piu", "chi ha scritto", "classifica", "ranking"]):
        return "stats", {}

    return "general", {}


# ─────────────────────────────────────────────────────────────────────────────
# FETCH FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_base(from_date, to_date, limit=800):
    try:
        res = (supabase.table("articles").select("*")
               .gte("data", from_date).lte("data", to_date)
               .order("data", desc=True).limit(limit).execute())
        return res.data or []
    except Exception as e:
        print("fetch_base error: " + str(e))
        return []


def fetch_journalist(name, from_date, to_date):
    try:
        res = (supabase.table("articles").select("*")
               .gte("data", from_date).lte("data", to_date)
               .ilike("giornalista", "%" + name + "%")
               .order("data", desc=True).limit(100).execute())
        return res.data or []
    except Exception as e:
        print("fetch_journalist error: " + str(e))
        return []


def fetch_entity(name, from_date, to_date):
    results, seen = [], set()
    try:
        for field in ["matched_client", "titolo"]:
            res = (supabase.table("articles").select("*")
                   .gte("data", from_date).lte("data", to_date)
                   .ilike(field, "%" + name + "%")
                   .order("data", desc=True).limit(200).execute())
            for a in (res.data or []):
                if a["id"] not in seen:
                    seen.add(a["id"])
                    results.append(a)
    except Exception as e:
        print("fetch_entity error: " + str(e))
    return sorted(results, key=lambda x: x.get("data", ""), reverse=True)


def fetch_topic(topic, from_date, to_date):
    """Cerca per tema nel titolo, testo completo e matched_client."""
    results, seen = [], set()
    try:
        for field in ["titolo", "testo_completo", "matched_client", "dominant_topic"]:
            res = (supabase.table("articles").select("*")
                   .gte("data", from_date).lte("data", to_date)
                   .ilike(field, "%" + topic + "%")
                   .order("data", desc=True).limit(200).execute())
            for a in (res.data or []):
                if a["id"] not in seen:
                    seen.add(a["id"])
                    results.append(a)
    except Exception as e:
        print("fetch_topic error: " + str(e))
    return sorted(results, key=lambda x: x.get("data", ""), reverse=True)


# ─────────────────────────────────────────────────────────────────────────────
# FORMATTING
# ─────────────────────────────────────────────────────────────────────────────

def fmt_articles(articles, max_text=700):
    lines = []
    for a in articles:
        testo = (a.get("testo_completo", "") or "").strip()
        testo_t = testo[:max_text] + ("..." if len(testo) > max_text else "")
        line = (
            "[" + str(a.get("data", "")) + "] "
            + str(a.get("testata", "N/D")) + " | "
            + "p." + str(a.get("pagina", "?")) + " | "
            + "Firma: " + str(a.get("giornalista", "Anonimo")) + " | "
            + "Cliente: " + str(a.get("matched_client", "")) + " | "
            + "Tone: " + str(a.get("tone", "")) + "\n"
            + "TITOLO: " + str(a.get("titolo", ""))
        )
        if testo_t:
            line += "\nTESTO: " + testo_t
        lines.append(line)
    return "\n\n---\n\n".join(lines)


def analytics(articles):
    if not articles:
        return "Nessun articolo."
    testate = Counter(a.get("testata", "") for a in articles if a.get("testata"))
    giornalisti = Counter(a.get("giornalista", "") for a in articles if a.get("giornalista"))
    tones = Counter(a.get("tone", "") for a in articles if a.get("tone"))
    clienti = Counter(a.get("matched_client", "") for a in articles if a.get("matched_client"))
    monthly = Counter()
    for a in articles:
        d = a.get("data", "")
        if d and len(d) >= 7:
            monthly[d[:7]] += 1
    tone_tot = sum(tones.values()) or 1
    tone_str = ", ".join(k + ": " + str(round(v/tone_tot*100)) + "%" for k, v in tones.most_common())
    def top(c, n=15):
        return ", ".join(k + " (" + str(v) + ")" for k, v in c.most_common(n))
    dates = [a.get("data","") for a in articles if a.get("data")]
    return (
        "TOTALE: " + str(len(articles)) + " articoli\n"
        + "PERIODO: " + (min(dates) if dates else "?") + " -> " + (max(dates) if dates else "?") + "\n"
        + "TESTATE: " + top(testate) + "\n"
        + "GIORNALISTI: " + top(giornalisti) + "\n"
        + "CLIENTI: " + top(clienti) + "\n"
        + "SENTIMENT: " + tone_str + "\n"
        + "MENSILE: " + ", ".join(m + ":" + str(c) for m, c in sorted(monthly.items()))
    )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def ask_spiz(message, history=None, context="general"):
    if not message or len(message.strip()) < 2:
        return {"error": "Messaggio troppo corto."}

    from_date, to_date = date_range(context, message)
    intent, entities   = detect_intent(message)

    print("SPIZ intent=" + intent + " from=" + from_date + " to=" + to_date + " entities=" + str(entities))

    if intent == "journalist":
        name    = entities.get("name", "")
        arts    = fetch_journalist(name, from_date, to_date)
        print("Giornalista '" + name + "': " + str(len(arts)) + " articoli")
        system  = (BASE_ROLE
                   + "Stai analizzando articoli di '" + name + "' nel periodo " + from_date + " - " + to_date + ".\n\n"
                   + "TROVATI " + str(len(arts)) + " articoli:\n\n"
                   + (fmt_articles(arts) if arts else "Nessun articolo trovato per questo giornalista."))
        max_tok = 2000

    elif intent == "topic":
        name    = entities.get("name", "")
        arts    = fetch_topic(name, from_date, to_date)
        print("Tema '" + name + "': " + str(len(arts)) + " articoli")
        system  = (BASE_ROLE
                   + "Stai analizzando articoli sul tema '" + name + "' nel periodo " + from_date + " - " + to_date + ".\n\n"
                   + analytics(arts) + "\n\n"
                   + "ARTICOLI (" + str(len(arts)) + "):\n\n"
                   + (fmt_articles(arts[:80]) if arts else "Nessun articolo trovato su questo tema."))
        max_tok = 3000

    elif intent == "client":
        name    = entities.get("name", "")
        arts    = fetch_entity(name, from_date, to_date)
        print("Entita' '" + name + "': " + str(len(arts)) + " articoli")
        system  = (BASE_ROLE + REPORT_STRUCT
                   + "Stai analizzando '" + name + "' nel periodo " + from_date + " - " + to_date + ".\n\n"
                   + analytics(arts) + "\n\n"
                   + "ARTICOLI:\n\n"
                   + (fmt_articles(arts[:100]) if arts else "Nessun articolo trovato."))
        max_tok = 6000

    elif intent == "report":
        name    = entities.get("name", "")
        if name:
            arts = fetch_entity(name, from_date, to_date)
            if len(arts) < 10:
                arts2 = fetch_topic(name, from_date, to_date)
                seen = {a["id"] for a in arts}
                for a in arts2:
                    if a["id"] not in seen:
                        arts.append(a)
        else:
            arts = fetch_base(from_date, to_date, limit=800)
        print("Report su '" + str(name) + "': " + str(len(arts)) + " articoli")
        system  = (BASE_ROLE + REPORT_STRUCT
                   + "Periodo: " + from_date + " - " + to_date
                   + ((" | Soggetto: " + name) if name else "") + "\n\n"
                   + analytics(arts) + "\n\n"
                   + "ARTICOLI:\n\n"
                   + (fmt_articles(arts[:150]) if arts else "Nessun articolo disponibile."))
        max_tok = 8000

    elif intent == "stats":
        arts    = fetch_base(from_date, to_date, limit=1000)
        print("Stats: " + str(len(arts)) + " articoli")
        system  = (BASE_ROLE
                   + "Rispondi a domande statistiche. Dati reali:\n\n" + analytics(arts))
        max_tok = 1500

    else:
        arts    = fetch_base(from_date, to_date, limit=300)
        print("General: " + str(len(arts)) + " articoli")
        system  = (BASE_ROLE
                   + "Periodo: " + from_date + " - " + to_date + "\n\n"
                   + analytics(arts) + "\n\n"
                   + "ARTICOLI:\n\n" + fmt_articles(arts[:60]))
        max_tok = 2000

    messages = [{"role": "system", "content": system}]
    for msg in (history or [])[-10:]:
        if msg.get("role") in ("user", "assistant") and msg.get("content"):
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": message})

    try:
        resp = client.chat.completions.create(
            model="gpt-4o", messages=messages, temperature=0.1, max_tokens=max_tok
        )
        return {"response": resp.choices[0].message.content.strip(), "is_report": intent in ("report", "client")}
    except Exception as e1:
        print("gpt-4o error: " + str(e1))
        try:
            resp = client.chat.completions.create(
                model="gpt-4o-mini", messages=messages, temperature=0.1, max_tokens=min(max_tok, 4000)
            )
            return {"response": resp.choices[0].message.content.strip(), "is_report": intent in ("report", "client")}
        except Exception as e2:
            return {"error": "Errore AI: " + str(e2)}