"""
api/chat.py - SPIZ AI Analysis v3
Retrieval mirato: analizza la domanda, query specifica al DB, risposta grounded.
"""

import os
import re
from datetime import date, timedelta
from collections import Counter
from openai import OpenAI
from services.database import supabase

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

REPORT_KEYWORDS = [
    "report", "analisi completa", "sentiment", "profilo mediatico",
    "criticita", "sintesi strategica", "long running", "governance",
    "territoriale", "istituzional", "reputazion", "media narrative",
    "temi longevi", "temi ricorrenti", "comunicazione istituzionale",
    "focus territoriale", "analisi sentiment", "presenza dei vertici",
    "redigi", "elabora", "produci", "fai un report", "fai un analisi",
    "analizza la copertura", "analizza il periodo", "analisi reputazionale",
]

CONTEXT_DAYS = {
    "today": 0,
    "week": 7,
    "month": 30,
    "year": 365,
    "general": 90,
}


# ── RETRIEVAL MIRATO ──────────────────────────────────────────────────────────

def date_range_from_context(context):
    days = CONTEXT_DAYS.get(context, 90)
    today = date.today().isoformat()
    if days == 0:
        return today, today
    from_date = (date.today() - timedelta(days=days)).isoformat()
    return from_date, today


def fetch_articles_base(from_date, to_date, limit=800):
    """Query base senza filtri extra."""
    try:
        res = (supabase.table("articles")
               .select("*")
               .gte("data", from_date)
               .lte("data", to_date)
               .order("data", desc=True)
               .limit(limit)
               .execute())
        return res.data or []
    except Exception as e:
        print("fetch_articles_base error: " + str(e))
        return []


def detect_intent(message):
    """
    Riconosce il tipo di domanda e estrae entità rilevanti.
    Ritorna: (intent, entities)
    intent: 'journalist' | 'client' | 'topic' | 'stats' | 'report' | 'general'
    """
    msg = message.lower()

    # Report strutturato
    if any(kw in msg for kw in REPORT_KEYWORDS):
        return "report", {}

    # Domanda su giornalista specifico
    journalist_patterns = [
        r"articoli di ([a-z]+ [a-z]+)",
        r"scritti da ([a-z]+ [a-z]+)",
        r"firma di ([a-z]+ [a-z]+)",
        r"([a-z]+ [a-z]+) ha scritto",
        r"giornalista ([a-z]+ [a-z]+)",
        r"ultimi articoli.*?([a-z]+ [a-z]+)",
        r"([a-z]+ [a-z]+).*?articoli",
    ]
    for pat in journalist_patterns:
        m = re.search(pat, msg)
        if m:
            name = m.group(1).strip()
            if len(name) > 4:
                return "journalist", {"name": name}

    # Domanda su cliente/azienda specifica
    client_patterns = [
        r"report su ([a-z0-9 ]+)",
        r"analisi su ([a-z0-9 ]+)",
        r"copertura di ([a-z0-9 ]+)",
        r"articoli su ([a-z0-9 ]+)",
        r"notizie su ([a-z0-9 ]+)",
        r"cosa (dicono|ha detto|dice) .* su ([a-z0-9 ]+)",
        r"su ([a-z0-9]+) .* articol",
    ]
    for pat in client_patterns:
        m = re.search(pat, msg)
        if m:
            entity = m.group(m.lastindex).strip()
            if len(entity) > 2:
                return "client", {"name": entity}

    # Statistiche semplici
    stats_kws = ["quanti", "quali testate", "top giornalist", "copertura oggi",
                 "articoli oggi", "pubblicato di piu", "più articoli"]
    if any(kw in msg for kw in stats_kws):
        return "stats", {}

    return "general", {}


def fetch_for_journalist(name, from_date, to_date):
    """Cerca articoli per giornalista con matching flessibile."""
    try:
        # Prova match esatto prima
        res = (supabase.table("articles")
               .select("*")
               .gte("data", from_date)
               .lte("data", to_date)
               .ilike("giornalista", "%" + name + "%")
               .order("data", desc=True)
               .limit(50)
               .execute())
        return res.data or []
    except Exception as e:
        print("fetch_for_journalist error: " + str(e))
        return []


def fetch_for_entity(name, from_date, to_date):
    """Cerca articoli per cliente/azienda: prima in matched_client, poi nel testo."""
    results = []
    try:
        # 1. matched_client
        res = (supabase.table("articles")
               .select("*")
               .gte("data", from_date)
               .lte("data", to_date)
               .ilike("matched_client", "%" + name + "%")
               .order("data", desc=True)
               .limit(200)
               .execute())
        results = res.data or []

        # 2. Se pochi risultati, cerca nel titolo
        if len(results) < 5:
            res2 = (supabase.table("articles")
                    .select("*")
                    .gte("data", from_date)
                    .lte("data", to_date)
                    .ilike("titolo", "%" + name + "%")
                    .order("data", desc=True)
                    .limit(200)
                    .execute())
            seen = {r["id"] for r in results}
            for a in (res2.data or []):
                if a["id"] not in seen:
                    results.append(a)

    except Exception as e:
        print("fetch_for_entity error: " + str(e))
    return results


# ── FORMATTAZIONE ARTICOLI PER IL PROMPT ─────────────────────────────────────

def format_articles_for_prompt(articles, max_text=600):
    """Formatta gli articoli in testo compatto per il prompt."""
    lines = []
    for a in articles:
        testo = (a.get("testo_completo", "") or "").strip()
        testo_trunc = testo[:max_text] + ("..." if len(testo) > max_text else "")
        line = (
            "[" + str(a.get("data","")) + "] "
            + str(a.get("testata","N/D")) + " | "
            + "p." + str(a.get("pagina","?")) + " | "
            + str(a.get("tipologia_articolo","")) + " | "
            + "Firma: " + str(a.get("giornalista","Anonimo")) + " | "
            + "Cliente: " + str(a.get("matched_client","")) + " | "
            + "Tone: " + str(a.get("tone","")) + " | "
            + "Topic: " + str(a.get("dominant_topic","")) + "\n"
            + "TITOLO: " + str(a.get("titolo","")) + "\n"
        )
        if testo_trunc:
            line += "TESTO: " + testo_trunc
        lines.append(line)
    return "\n\n---\n\n".join(lines)


def build_analytics_summary(articles):
    """Costruisce un sommario statistico del corpus."""
    if not articles:
        return "Nessun articolo disponibile."

    testate    = Counter(a.get("testata","") for a in articles if a.get("testata"))
    giornalist = Counter(a.get("giornalista","") for a in articles if a.get("giornalista"))
    tones      = Counter(a.get("tone","") for a in articles if a.get("tone"))
    clienti    = Counter(a.get("matched_client","") for a in articles if a.get("matched_client"))
    monthly    = Counter()
    for a in articles:
        d = a.get("data","")
        if d and len(d) >= 7:
            monthly[d[:7]] += 1

    tone_total = sum(tones.values()) or 1
    tone_str = ", ".join(
        k + ": " + str(round(v/tone_total*100)) + "%"
        for k,v in sorted(tones.items(), key=lambda x: -x[1])
    )

    def top(c, n=15):
        return ", ".join(k + " (" + str(v) + ")" for k,v in c.most_common(n))

    summary = (
        "TOTALE: " + str(len(articles)) + " articoli\n"
        "PERIODO: " + (min(a.get("data","") for a in articles if a.get("data")) or "?")
        + " → " + (max(a.get("data","") for a in articles if a.get("data")) or "?") + "\n"
        "TESTATE: " + top(testate) + "\n"
        "GIORNALISTI: " + top(giornalist) + "\n"
        "CLIENTI: " + top(clienti) + "\n"
        "SENTIMENT: " + tone_str + "\n"
        "ANDAMENTO: " + ", ".join(m + ":" + str(c) for m,c in sorted(monthly.items()))
    )
    return summary


# ── SYSTEM PROMPTS ────────────────────────────────────────────────────────────

BASE_ROLE = (
    "Sei SPIZ, analista senior di MAIM Public Diplomacy & Media Relations, "
    "specializzato in media monitoring e comunicazione istituzionale.\n\n"
    "REGOLA ASSOLUTA: Rispondi SOLO ed ESCLUSIVAMENTE usando i dati forniti qui sotto. "
    "Non usare MAI la tua conoscenza generale. "
    "Se un'informazione non e' presente negli articoli forniti, dillo esplicitamente. "
    "Non inventare articoli, testate, dichiarazioni o dati che non vedi nel corpus.\n\n"
)

REPORT_INSTRUCTIONS = (
    "Per report strutturati usa questa struttura con sezioni numerate:\n"
    "1. PROFILO MEDIATICO - ruolo attribuito, percezione (economica/generalista/locale), "
    "temi frequenti con testate, evoluzione per fasi cronologiche\n"
    "2. INTERVISTE E PRESENZA VERTICI - per ogni intervento: testata, data, firma, tema, "
    "tono, messaggio principale, esposizione reputazionale\n"
    "3. TEMI LONGEVI - per macro-area, classificati: persistenti/emergenti/in diminuzione\n"
    "4. NOTIZIE FINANZIARIE E CORPORATE\n"
    "5. GOVERNANCE E MANAGEMENT\n"
    "6. FOCUS TERRITORIALE - tabella territorio|attenzione|conflittualita'|trend\n"
    "7. CRITICITA' REPUTAZIONALI - testata|tema|tono|impatto|propagazione\n"
    "8. SENTIMENT - % positivo/neutro/negativo, driver, rischio complessivo\n"
    "9. COMUNICAZIONE ISTITUZIONALE\n"
    "10. SINTESI STRATEGICA - tabella territori|priorita'|azione, rischi, opportunita'\n\n"
    "Usa ## per sezioni, ### sottosezioni, **grassetto** per evidenze, tabelle markdown. "
    "Nessuna emoji. Linguaggio professionale corporate.\n\n"
)


def build_prompt_journalist(name, articles, from_date, to_date):
    corpus = format_articles_for_prompt(articles, max_text=800)
    return (
        BASE_ROLE
        + "Stai analizzando gli articoli scritti da '" + name + "' "
        + "nel periodo " + from_date + " - " + to_date + ".\n\n"
        + "ARTICOLI TROVATI (" + str(len(articles)) + "):\n\n"
        + (corpus if corpus else "Nessun articolo trovato per questo giornalista nel periodo.")
        + "\n\nRispondi basandoti SOLO su questi articoli."
    )


def build_prompt_entity(name, articles, from_date, to_date):
    summary = build_analytics_summary(articles)
    corpus  = format_articles_for_prompt(articles[:100], max_text=700)
    return (
        BASE_ROLE
        + REPORT_INSTRUCTIONS
        + "Stai analizzando la copertura mediatica di '" + name + "' "
        + "nel periodo " + from_date + " - " + to_date + ".\n\n"
        + "SOMMARIO STATISTICO:\n" + summary + "\n\n"
        + "ARTICOLI (" + str(len(articles)) + " totali, mostro i primi 100 con testo):\n\n"
        + (corpus if corpus else "Nessun articolo trovato.")
        + "\n\nRispondi basandoti SOLO su questi dati reali."
    )


def build_prompt_stats(articles, from_date, to_date):
    summary = build_analytics_summary(articles)
    return (
        BASE_ROLE
        + "Stai rispondendo a domande statistiche sulla rassegna stampa "
        + "nel periodo " + from_date + " - " + to_date + ".\n\n"
        + "DATI REALI:\n" + summary
        + "\n\nRispondi in modo diretto e preciso usando solo questi dati."
    )


def build_prompt_report(articles, from_date, to_date):
    summary = build_analytics_summary(articles)
    corpus  = format_articles_for_prompt(articles[:150], max_text=700)
    return (
        BASE_ROLE
        + REPORT_INSTRUCTIONS
        + "Periodo di riferimento: " + from_date + " - " + to_date + "\n\n"
        + "SOMMARIO STATISTICO:\n" + summary + "\n\n"
        + "ARTICOLI CON TESTO (" + str(len(articles)) + " totali, mostro i primi 150):\n\n"
        + corpus
        + "\n\nRedigi il report richiesto basandoti ESCLUSIVAMENTE su questi articoli reali."
    )


def build_prompt_general(articles, from_date, to_date):
    summary = build_analytics_summary(articles)
    corpus  = format_articles_for_prompt(articles[:80], max_text=500)
    return (
        BASE_ROLE
        + "Periodo di riferimento: " + from_date + " - " + to_date + "\n\n"
        + "DATI DISPONIBILI:\n" + summary + "\n\n"
        + "ARTICOLI:\n\n" + corpus
        + "\n\nRispondi in modo diretto basandoti SOLO su questi dati reali."
    )


# ── MAIN FUNCTION ─────────────────────────────────────────────────────────────

def ask_spiz(message, history=None, context="general"):
    if not message or len(message.strip()) < 2:
        return {"error": "Messaggio troppo corto."}

    from_date, to_date = date_range_from_context(context)
    intent, entities   = detect_intent(message)

    print("ask_spiz intent=" + intent + " context=" + context + " from=" + from_date)

    # Retrieval mirato in base all'intent
    if intent == "journalist":
        name     = entities.get("name", "")
        articles = fetch_for_journalist(name, from_date, to_date)
        print("Giornalista '" + name + "': " + str(len(articles)) + " articoli")
        system   = build_prompt_journalist(name, articles, from_date, to_date)
        max_tok  = 2000

    elif intent == "client":
        name     = entities.get("name", "")
        articles = fetch_for_entity(name, from_date, to_date)
        print("Entita' '" + name + "': " + str(len(articles)) + " articoli")
        system   = build_prompt_entity(name, articles, from_date, to_date)
        max_tok  = 6000

    elif intent == "stats":
        articles = fetch_articles_base(from_date, to_date, limit=1000)
        print("Stats: " + str(len(articles)) + " articoli")
        system   = build_prompt_stats(articles, from_date, to_date)
        max_tok  = 1500

    elif intent == "report":
        articles = fetch_articles_base(from_date, to_date, limit=800)
        print("Report: " + str(len(articles)) + " articoli")
        system   = build_prompt_report(articles, from_date, to_date)
        max_tok  = 8000

    else:
        articles = fetch_articles_base(from_date, to_date, limit=400)
        print("General: " + str(len(articles)) + " articoli")
        system   = build_prompt_general(articles, from_date, to_date)
        max_tok  = 2000

    messages = [{"role": "system", "content": system}]
    if history:
        for msg in (history or [])[-10:]:
            if msg.get("role") in ("user", "assistant") and msg.get("content"):
                messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": message})

    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.1,
            max_tokens=max_tok
        )
        return {"response": resp.choices[0].message.content.strip(), "is_report": intent == "report"}
    except Exception as e1:
        print("gpt-4o error: " + str(e1))
        try:
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                temperature=0.1,
                max_tokens=min(max_tok, 4000)
            )
            return {"response": resp.choices[0].message.content.strip(), "is_report": intent == "report"}
        except Exception as e2:
            return {"error": "Errore AI: " + str(e2)}
