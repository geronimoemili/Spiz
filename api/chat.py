"""
api/chat.py - SPIZ AI Analysis (Enhanced)
Risponde a domande sugli articoli nel database usando OpenAI.
Modalità:
- Chat normale
- Report strategico Direzione Comunicazione
"""

import os
from datetime import date, timedelta
from collections import Counter
from openai import OpenAI
from services.database import supabase


client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# ==========================================================
# CARICAMENTO ARTICOLI
# ==========================================================

def get_context_articles(context: str = "general", limit: int = 200) -> list:
    """Carica articoli dal DB in base al contesto selezionato."""
    try:
        today = date.today()

        if context == "today":
            from_date = today
        elif context == "week":
            from_date = today - timedelta(days=7)
        elif context == "month":
            from_date = today - timedelta(days=30)
        else:
            from_date = today - timedelta(days=90)

        res = (
            supabase
            .table("articles")
            .select(
                "titolo, testata, giornalista, data, testo_completo, "
                "macrosettori, tone, dominant_topic, reputational_risk, political_risk"
            )
            .gte("data", from_date.isoformat())
            .order("data", desc=True)
            .limit(limit)
            .execute()
        )

        return res.data or []

    except Exception as e:
        print(f"Errore caricamento contesto: {e}")
        return []


# ==========================================================
# COSTRUZIONE PROMPT
# ==========================================================

def build_chat_prompt(articles: list, context: str) -> str:
    """Prompt modalità chat normale."""

    if not articles:
        return (
            "Sei SPIZ, l'assistente AI di MAIM Public Diplomacy & Media Relations. "
            "Al momento non ci sono articoli nel database. "
            "Suggerisci all'utente di caricare dei CSV dalla Dashboard."
        )

    testate = list(set(a.get("testata", "") for a in articles if a.get("testata")))
    giornalisti = list(set(a.get("giornalista", "") for a in articles if a.get("giornalista")))
    topics = list(set(a.get("dominant_topic", "") for a in articles if a.get("dominant_topic")))

    sample = articles[:50]

    articles_text = "\n".join([
        f"- [{a.get('data','')}] {a.get('testata','N/D')} | "
        f"{a.get('giornalista','Anonimo')} | {a.get('titolo','')}"
        for a in sample
    ])

    return f"""
Sei SPIZ, assistente AI di MAIM Public Diplomacy & Media Relations.
Hai accesso al database rassegna stampa.

DATI DISPONIBILI:
- Articoli: {len(articles)}
- Testate principali: {', '.join(testate[:15])}
- Giornalisti unici: {len(giornalisti)}
- Topic principali: {', '.join(topics[:10])}

LISTA ARTICOLI RECENTI:
{articles_text}

ISTRUZIONI:
- Rispondi in italiano.
- Linguaggio professionale e conciso.
- Usa esclusivamente i dati forniti.
- Puoi usare tabelle markdown.
- Evita preamboli inutili.
"""


def build_report_prompt(report_data: dict) -> str:
    """Prompt modalità report strategico."""

    return f"""
Agisci come analista senior di comunicazione istituzionale, reputazione e media intelligence.

DATABASE ANALIZZATO:
- Articoli totali: {report_data['total']}
- Distribuzione tone (%): {report_data['tone_distribution']}
- Top topic: {report_data['top_topics']}
- Top testate: {report_data['top_testate']}
- Articoli critici (negativi o con rischio reputazionale): {report_data['critical_count']}

CAMPIONE QUALITATIVO ARTICOLI:
{report_data['sample_text']}

Redigi un report strutturato per Direzione Comunicazione.

SEZIONI OBBLIGATORIE:

1. Profilo mediatico della società
2. Interviste e presenza dei vertici
3. Temi longevi
4. Notizie finanziarie e corporate
5. Cambi di management e governance
6. Criticità reputazionali
7. Analisi del sentiment
8. Comunicazione istituzionale
9. Sintesi strategica finale

REQUISITI:
- Linguaggio professionale
- Approfondito ma non prolisso
- Niente emoji
- Struttura chiara con titoli numerati
- Basati esclusivamente sui dati del database
"""


# ==========================================================
# FUNZIONE PRINCIPALE
# ==========================================================

def ask_spiz(message: str, history: list = None, context: str = "general") -> dict:

    if not message or len(message.strip()) < 2:
        return {"error": "Messaggio troppo corto."}

    # Rilevamento modalità report
    is_report = any(keyword in message.lower() for keyword in [
        "report",
        "analisi completa",
        "direzione comunicazione",
        "analisi strategica"
    ])

    # Caricamento articoli
    if is_report:
        articles = get_context_articles(context, limit=800)
    else:
        articles = get_context_articles(context, limit=200)

    if not articles:
        return {"response": "Non sono presenti articoli nel database per il periodo selezionato."}

    # ======================================================
    # MODALITÀ REPORT
    # ======================================================
    if is_report:

        total_articles = len(articles)

        tone_counts = Counter(a.get("tone", "Neutral") for a in articles)
        topic_counts = Counter(a.get("dominant_topic", "Altro") for a in articles)
        testata_counts = Counter(a.get("testata", "N/D") for a in articles)

        tone_distribution = {
            k: round((v / total_articles) * 100, 1)
            for k, v in tone_counts.items()
        }

        critical_articles = [
            a for a in articles
            if a.get("tone") == "Negative"
            or a.get("reputational_risk") not in (None, "None")
        ]

        # Campione qualitativo limitato per evitare overflow token
        sample_articles = articles[:30]

        sample_text = "\n\n".join([
            f"""
TESTATA: {a.get('testata')}
DATA: {a.get('data')}
TITOLO: {a.get('titolo')}
TONE: {a.get('tone')}
TOPIC: {a.get('dominant_topic')}
TESTO:
{(a.get('testo_completo') or '')[:1200]}
"""
            for a in sample_articles
        ])

        report_data = {
            "total": total_articles,
            "tone_distribution": tone_distribution,
            "top_topics": topic_counts.most_common(10),
            "top_testate": testata_counts.most_common(10),
            "critical_count": len(critical_articles),
            "sample_text": sample_text
        }

        system_prompt = build_report_prompt(report_data)

        messages = [{"role": "system", "content": system_prompt}]
        messages.append({"role": "user", "content": message})

    # ======================================================
    # MODALITÀ CHAT NORMALE
    # ======================================================
    else:

        system_prompt = build_chat_prompt(articles, context)
        messages = [{"role": "system", "content": system_prompt}]

        if history:
            for msg in history[-10:]:
                if msg.get("role") in ("user", "assistant") and msg.get("content"):
                    messages.append({"role": msg["role"], "content": msg["content"]})

        messages.append({"role": "user", "content": message})

    # ======================================================
    # CHIAMATA OPENAI
    # ======================================================

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.3,
            max_tokens=1200
        )

        reply = response.choices[0].message.content.strip()
        return {"response": reply}

    except Exception as e:
        print(f"Errore OpenAI chat: {e}")
        return {"error": f"Errore AI: {str(e)}"}