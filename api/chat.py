"""
api/chat.py - SPIZ AI Analysis
Risponde a domande sugli articoli nel database usando OpenAI.
"""

import os
from datetime import date, timedelta
from openai import OpenAI
from services.database import supabase

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def get_context_articles(context: str = "general") -> list:
    """Carica articoli dal DB in base al contesto selezionato."""
    try:
        today = date.today().isoformat()

        if context == "today":
            from_date = today
        elif context == "week":
            from_date = (date.today() - timedelta(days=7)).isoformat()
        elif context == "month":
            from_date = (date.today() - timedelta(days=30)).isoformat()
        else:
            from_date = (date.today() - timedelta(days=90)).isoformat()

        res = supabase.table("articles").select(
            "titolo, testata, giornalista, data, testo_completo, macrosettori, tone, dominant_topic"
        ).gte("data", from_date).order("data", desc=True).limit(200).execute()

        return res.data or []
    except Exception as e:
        print(f"Errore caricamento contesto: {e}")
        return []


def build_system_prompt(articles: list, context: str) -> str:
    """Costruisce il system prompt con il contesto degli articoli."""
    context_labels = {
        "today": "di oggi",
        "week": "dell'ultima settimana",
        "month": "dell'ultimo mese",
        "general": "degli ultimi 3 mesi"
    }
    label = context_labels.get(context, "recenti")

    if not articles:
        return (
            "Sei SPIZ, l'assistente AI di MAIM Public Diplomacy & Media Relations. "
            "Al momento non ci sono articoli nel database. "
            "Suggerisci all'utente di caricare dei CSV dalla Dashboard."
        )

    # Costruisci sommario degli articoli
    testate = list(set(a.get("testata", "") for a in articles if a.get("testata")))
    giornalisti = list(set(a.get("giornalista", "") for a in articles if a.get("giornalista")))
    topics = list(set(a.get("dominant_topic", "") for a in articles if a.get("dominant_topic")))

    # Primi 50 articoli come sample per l'AI
    sample = articles[:50]
    articles_text = "\n".join([
        f"- [{a.get('data','')}] {a.get('testata','N/D')} | {a.get('giornalista','Anonimo')} | {a.get('titolo','')}"
        for a in sample
    ])

    return f"""Sei SPIZ, l'assistente AI di MAIM Public Diplomacy & Media Relations.
Hai accesso al database rassegna stampa con {len(articles)} articoli {label}.

STATISTICHE CONTESTO:
- Articoli disponibili: {len(articles)}
- Testate presenti: {', '.join(testate[:15])}
- Giornalisti: {len(giornalisti)} firme uniche
- Topic principali: {', '.join(topics[:10])}

LISTA ARTICOLI (ultimi {len(sample)}):
{articles_text}

ISTRUZIONI:
- Rispondi in italiano, in modo professionale e conciso
- Usa i dati reali del database per rispondere
- Se ti chiedono classifiche o analisi, basati sui dati forniti
- Puoi usare tabelle markdown per dati strutturati
- Sii diretto e utile, evita preamboli inutili
"""


def ask_spiz(message: str, history: list = None, context: str = "general") -> dict:
    """
    Risponde a una domanda sugli articoli nel database.
    """
    if not message or len(message.strip()) < 2:
        return {"error": "Messaggio troppo corto."}

    # Carica articoli per il contesto
    articles = get_context_articles(context)
    system_prompt = build_system_prompt(articles, context)

    # Costruisci la history per OpenAI
    messages = [{"role": "system", "content": system_prompt}]

    if history:
        for msg in history[-10:]:  # Ultimi 10 messaggi per non superare i token
            if msg.get("role") in ("user", "assistant") and msg.get("content"):
                messages.append({"role": msg["role"], "content": msg["content"]})

    messages.append({"role": "user", "content": message})

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.3,
            max_tokens=1000
        )
        reply = response.choices[0].message.content.strip()
        return {"response": reply}

    except Exception as e:
        print(f"Errore OpenAI chat: {e}")
        return {"error": f"Errore AI: {str(e)}"}