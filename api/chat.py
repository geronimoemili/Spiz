"""
api/chat.py - SPIZ AI Analysis
Analista senior di comunicazione istituzionale e media monitoring.
"""

import os
from datetime import date, timedelta
from collections import Counter
from openai import OpenAI
from services.database import supabase

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

REPORT_KEYWORDS = [
    "report", "analisi completa", "sentiment", "profilo mediatico",
    "criticita", "sintesi strategica", "long running", "governance",
    "territoriale", "istituzional", "reputazion", "media narrative",
    "rassegna", "monitoraggio", "temi ricorrenti", "interviste", "vertici",
    "management", "finanzia", "corporate", "redigi", "elabora", "produci",
    "dammi un report", "fai un report", "fai un analisi", "analizza la copertura",
    "analisi reputazionale", "temi longevi", "criticita reputazionali",
    "comunicazione istituzionale", "focus territoriale", "analisi sentiment",
    "presenza dei vertici", "analizza il periodo",
]


def get_context_articles(context="general"):
    try:
        today = date.today().isoformat()
        if context == "today":
            from_date = today
        elif context == "week":
            from_date = (date.today() - timedelta(days=7)).isoformat()
        elif context == "month":
            from_date = (date.today() - timedelta(days=30)).isoformat()
        elif context == "year":
            from_date = (date.today() - timedelta(days=365)).isoformat()
        else:
            from_date = (date.today() - timedelta(days=90)).isoformat()

        res = supabase.table("articles").select(
            "id, titolo, testata, giornalista, data, testo_completo, "
            "macrosettori, tone, dominant_topic, tipologia_articolo, pagina, "
            "occhiello, matched_client"
        ).gte("data", from_date).order("data", desc=True).limit(800).execute()

        return res.data or []
    except Exception as e:
        print("Errore caricamento contesto: " + str(e))
        return []


def build_analytics(articles):
    if not articles:
        return {}

    testate     = [a.get("testata", "") for a in articles if a.get("testata")]
    giornalisti = [a.get("giornalista", "") for a in articles if a.get("giornalista")]
    tones       = [a.get("tone", "") for a in articles if a.get("tone")]
    topics      = [a.get("dominant_topic", "") for a in articles if a.get("dominant_topic")]
    clienti     = [a.get("matched_client", "") for a in articles if a.get("matched_client")]
    tipologie   = [a.get("tipologia_articolo", "") for a in articles if a.get("tipologia_articolo")]
    settori     = []
    for a in articles:
        ms = a.get("macrosettori", "")
        if ms:
            settori.extend([s.strip() for s in ms.split(",") if s.strip()])

    tone_dist = Counter(tones)
    monthly   = Counter()
    for a in articles:
        d = a.get("data", "")
        if d and len(d) >= 7:
            monthly[d[:7]] += 1

    # Analisi per cliente
    client_analysis = {}
    for a in articles:
        cl = a.get("matched_client", "")
        if not cl:
            continue
        if cl not in client_analysis:
            client_analysis[cl] = {"count": 0, "tones": [], "testate": [], "giornalisti": [], "titoli": []}
        client_analysis[cl]["count"] += 1
        if a.get("tone"):
            client_analysis[cl]["tones"].append(a["tone"])
        if a.get("testata"):
            client_analysis[cl]["testate"].append(a["testata"])
        if a.get("giornalista"):
            client_analysis[cl]["giornalisti"].append(a["giornalista"])
        if a.get("titolo"):
            entry = "[" + a.get("data","") + "] " + a.get("testata","") + ": " + a["titolo"]
            client_analysis[cl]["titoli"].append(entry)

    with_text = [a for a in articles if a.get("testo_completo") and len(a.get("testo_completo", "")) > 80]
    with_text_sorted = sorted(with_text, key=lambda x: len(x.get("testo_completo", "")), reverse=True)

    text_sample = []
    for a in with_text_sorted[:150]:
        testo = (a.get("testo_completo", "") or "").strip()
        testo_trunc = testo[:800] + ("..." if len(testo) > 800 else "")
        line = (
            "[" + a.get("data","") + "] " + a.get("testata","N/D") + " | "
            "p." + str(a.get("pagina","?")) + " | " + str(a.get("tipologia_articolo","")) + " | "
            "Firma: " + a.get("giornalista","Anonimo") + " | Cliente: " + a.get("matched_client","") + " | "
            "Tone: " + a.get("tone","") + " | Topic: " + a.get("dominant_topic","") + "\n"
            "TITOLO: " + a.get("titolo","") + "\n"
            "TESTO: " + testo_trunc
        )
        text_sample.append(line)

    titles_index = []
    for a in articles:
        if not a.get("testo_completo"):
            line = (
                "[" + a.get("data","") + "] " + a.get("testata","N/D") + " | "
                + a.get("giornalista","Anonimo") + " | Cliente: " + a.get("matched_client","") + " | "
                + "Tone: " + a.get("tone","") + " | " + a.get("titolo","")
            )
            titles_index.append(line)

    return {
        "totale":           len(articles),
        "con_testo":        len(with_text),
        "top_testate":      Counter(testate).most_common(30),
        "top_giornalisti":  Counter(giornalisti).most_common(25),
        "top_topics":       Counter(topics).most_common(15),
        "top_clienti":      Counter(clienti).most_common(30),
        "top_settori":      Counter(settori).most_common(15),
        "top_tipologie":    Counter(tipologie).most_common(10),
        "tone_dist":        dict(tone_dist),
        "monthly":          dict(sorted(monthly.items())),
        "text_sample":      text_sample,
        "titles_index":     titles_index[:200],
        "client_analysis":  client_analysis,
        "date_min":         min((a.get("data","") for a in articles if a.get("data")), default=""),
        "date_max":         max((a.get("data","") for a in articles if a.get("data")), default=""),
    }


def build_system_prompt(articles, context):
    context_labels = {
        "today":   "di oggi",
        "week":    "dell'ultima settimana",
        "month":   "dell'ultimo mese",
        "year":    "dell'ultimo anno",
        "general": "degli ultimi 3 mesi",
    }
    label = context_labels.get(context, "recenti")

    if not articles:
        return (
            "Sei SPIZ, l'assistente AI senior di MAIM Public Diplomacy & Media Relations. "
            "Specializzato in analisi di comunicazione istituzionale, reputazione e media monitoring. "
            "Al momento non ci sono articoli nel database per il periodo selezionato. "
            "Suggerisci all'utente di caricare CSV dalla Dashboard o di cambiare il periodo."
        )

    an = build_analytics(articles)

    def fmt(c, n=20):
        return ", ".join(k + " (" + str(v) + ")" for k, v in c[:n]) if c else "N/D"

    tone_total = sum(an["tone_dist"].values()) or 1
    tone_str   = ", ".join(
        k + ": " + str(round(v / tone_total * 100)) + "%"
        for k, v in sorted(an["tone_dist"].items(), key=lambda x: -x[1])
    )

    monthly_str = "\n".join("  " + m + ": " + str(c) + " articoli" for m, c in an["monthly"].items())

    client_summary = ""
    for cl, data in sorted(an["client_analysis"].items(), key=lambda x: -x[1]["count"])[:15]:
        tone_c = Counter(data["tones"])
        top_t  = Counter(data["testate"]).most_common(5)
        top_j  = Counter(data["giornalisti"]).most_common(5)
        last_5 = data["titoli"][-5:]
        client_summary += (
            "\n### " + cl + " (" + str(data["count"]) + " articoli)\n"
            "Sentiment: " + ", ".join(k + ":" + str(v) for k, v in tone_c.most_common()) + "\n"
            "Top testate: " + ", ".join(k + "(" + str(v) + ")" for k, v in top_t) + "\n"
            "Top giornalisti: " + ", ".join(k + "(" + str(v) + ")" for k, v in top_j) + "\n"
            "Ultimi titoli:\n" + "\n".join("  - " + t for t in last_5) + "\n"
        )

    titles_str = "\n".join(an["titles_index"])
    sample_str = "\n\n---\n\n".join(an["text_sample"])

    prompt = (
        "Sei SPIZ, analista senior di comunicazione istituzionale, reputazione e media monitoring "
        "di MAIM Public Diplomacy & Media Relations.\n\n"
        "Il tuo standard di output e' quello di un report professionale destinato alla Direzione "
        "Comunicazione di un'azienda corporate quotata. Ragiona, sintetizza, interpreta. "
        "Cita sempre testate, giornalisti, titoli e date specifici quando supportano l'analisi.\n\n"

        "CORPUS: " + str(an["totale"]) + " articoli " + label + " | "
        + an["date_min"] + " -> " + an["date_max"] + " | Con testo: " + str(an["con_testo"]) + "\n\n"

        "DISTRIBUZIONE MENSILE:\n" + monthly_str + "\n\n"
        "TESTATE: " + fmt(an["top_testate"], 30) + "\n\n"
        "GIORNALISTI: " + fmt(an["top_giornalisti"], 25) + "\n\n"
        "CLIENTI/SOGGETTI: " + fmt(an["top_clienti"], 30) + "\n\n"
        "TOPIC: " + fmt(an["top_topics"], 15) + "\n\n"
        "SETTORI: " + fmt(an["top_settori"], 15) + "\n\n"
        "TIPOLOGIE: " + fmt(an["top_tipologie"], 10) + "\n\n"
        "SENTIMENT: " + tone_str + "\n\n"
        "ANALISI PER CLIENTE:\n" + client_summary + "\n\n"
        "INDICE ARTICOLI SENZA TESTO:\n" + titles_str + "\n\n"
        "ARTICOLI CON TESTO COMPLETO:\n" + sample_str + "\n\n"

        "=== ISTRUZIONI ===\n\n"
        "STILE: Linguaggio professionale corporate. Nessuna emoji. Tono da analista senior.\n\n"

        "REPORT STRUTTURATO: Quando l'utente chiede un report o usa termini come 'profilo mediatico', "
        "'sentiment', 'temi longevi', 'criticita', 'governance', 'territoriale', 'istituzionale', "
        "segui ESATTAMENTE questa struttura con sezioni numerate:\n\n"

        "1. PROFILO MEDIATICO (media narrative profile)\n"
        "   - Ruolo attribuito alla societa'\n"
        "   - Percezione generale (per tipo di stampa: economica / generalista / locale)\n"
        "   - Temi associati piu' frequentemente con testate prevalenti\n"
        "   - Evoluzione del racconto per fasi cronologiche con registro narrativo\n\n"

        "2. INTERVISTE E PRESENZA DEI VERTICI\n"
        "   Per ogni intervento: testata, data, firma, tema, tono, messaggio principale, "
        "esposizione reputazionale (alta/media/bassa, positiva/negativa/rischiosa)\n\n"

        "3. TEMI LONGEVI (long running issues) per macro-area\n"
        "   Classificati: persistenti / emergenti / in diminuzione\n\n"

        "4. NOTIZIE FINANZIARIE E CORPORATE\n"
        "   Risultati, guidance, investimenti, operazioni straordinarie, titolo, analisti\n\n"

        "5. CAMBI DI MANAGEMENT E GOVERNANCE\n"
        "   Nomine, rinnovi, equilibri azionari\n\n"

        "6. FOCUS TERRITORIALE E TEMI LOCALI SENSIBILI\n"
        "   Tabella: territorio | attenzione mediatica | conflittualita' | trend\n"
        "   Analisi per ogni territorio critico. Indice di sensibilita' territoriale.\n\n"

        "7. CRITICITA' REPUTAZIONALI\n"
        "   Per ogni caso: testata/territorio | tema | tono | impatto | propagazione\n\n"

        "8. ANALISI DEL SENTIMENT\n"
        "   % positivo/neutro/negativo con nota metodologica.\n"
        "   Driver positivi, driver negativi, rischio reputazionale complessivo.\n\n"

        "9. COMUNICAZIONE ISTITUZIONALE\n"
        "   Rapporti Governo/UE, sicurezza strategica, posizionamento istituzionale, "
        "effetto sul posizionamento.\n\n"

        "10. SINTESI STRATEGICA FINALE\n"
        "    Tabella territori da presidiare con priorita' e azione raccomandata.\n"
        "    Priorita' comunicative, rischi emergenti, opportunita' narrative.\n\n"

        "FORMATTAZIONE: ## sezioni, ### sottosezioni, **grassetto** per evidenze, "
        "tabelle markdown per comparativi, citazioni tra virgolette con fonte e data.\n"
        "Per report completi: profondita' e dettaglio sono prioritari rispetto alla brevita'.\n\n"

        "DOMANDE SEMPLICI: Risposta diretta e concisa senza struttura da report.\n\n"

        "REGOLA ASSOLUTA: Cita solo dati reali dal corpus. "
        "Se un'informazione non e' presente, segnalalo esplicitamente."
    )

    return prompt


def ask_spiz(message, history=None, context="general"):
    if not message or len(message.strip()) < 2:
        return {"error": "Messaggio troppo corto."}

    articles      = get_context_articles(context)
    system_prompt = build_system_prompt(articles, context)
    messages      = [{"role": "system", "content": system_prompt}]

    if history:
        for msg in history[-12:]:
            if msg.get("role") in ("user", "assistant") and msg.get("content"):
                messages.append({"role": msg["role"], "content": msg["content"]})

    messages.append({"role": "user", "content": message})

    msg_lower  = message.lower()
    is_report  = any(kw in msg_lower for kw in REPORT_KEYWORDS)
    max_tokens = 8000 if is_report else 2000

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.15,
            max_tokens=max_tokens
        )
        reply = response.choices[0].message.content.strip()
        return {"response": reply, "is_report": is_report}
    except Exception as e1:
        print("gpt-4o fallback: " + str(e1))
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                temperature=0.15,
                max_tokens=6000 if is_report else 1500
            )
            reply = response.choices[0].message.content.strip()
            return {"response": reply, "is_report": is_report}
        except Exception as e2:
            print("Errore OpenAI: " + str(e2))
            return {"error": "Errore AI: " + str(e2)}