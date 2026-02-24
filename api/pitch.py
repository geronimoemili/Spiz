"""
api/pitch.py — SPIZ Pitch Advisor
Analizza un comunicato stampa e suggerisce i giornalisti più adatti.
"""

import os
from openai import OpenAI
from services.database import supabase

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ─── STEP 1: Analizza il comunicato con OpenAI ─────────────────────────────────

def analizza_comunicato(testo: str) -> dict:
    """Estrae tema, settori, tono e keyword dal comunicato."""
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": (
                    "Sei un esperto di comunicazione e media relations italiano. "
                    "Analizza il comunicato stampa e restituisci SOLO un JSON con questi campi:\n"
                    "- tema: stringa, tema principale del comunicato (max 10 parole)\n"
                    "- settori: lista di max 5 settori/macrosettori rilevanti\n"
                    "- keywords: lista di max 10 parole chiave importanti\n"
                    "- tono: uno tra 'istituzionale', 'economico', 'tecnico', 'sociale', 'politico'\n"
                    "- sintesi: stringa, sintesi del comunicato in 2 righe\n"
                    "Rispondi SOLO con il JSON, nessun testo aggiuntivo."
                )},
                {"role": "user", "content": testo[:4000]}
            ],
            temperature=0,
            max_tokens=500
        )
        import json, re
        raw = response.choices[0].message.content
        raw = re.sub(r'```json|```', '', raw).strip()
        return json.loads(raw)
    except Exception as e:
        print(f"Errore analisi comunicato: {e}")
        return {
            "tema": "N/D",
            "settori": [],
            "keywords": [],
            "tono": "istituzionale",
            "sintesi": testo[:200]
        }


# ─── STEP 2: Carica giornalisti dal DB ────────────────────────────────────────

def carica_giornalisti(giorni: int = 180) -> list[dict]:
    """Carica tutti gli articoli recenti raggruppati per giornalista."""
    try:
        from datetime import date, timedelta
        from_date = (date.today() - timedelta(days=giorni)).isoformat()

        res = supabase.table("articles").select(
            "giornalista, testata, titolo, macrosettori, tipologia_articolo, data"
        ).gte("data", from_date).execute()

        articles = res.data or []

        # Raggruppa per giornalista
        giornalisti = {}
        for a in articles:
            g = (a.get('giornalista') or '').strip()
            if not g or g in ('N.D.', 'N/D', 'Redazione', 'Autore non indicato', ''):
                continue
            if g not in giornalisti:
                giornalisti[g] = {
                    "nome": g,
                    "testata": a.get('testata', 'N/D'),
                    "articoli": [],
                    "macrosettori": set(),
                    "titoli": []
                }
            giornalisti[g]["articoli"].append(a)
            for macro in (a.get('macrosettori') or '').split(','):
                m = macro.strip()
                if m:
                    giornalisti[g]["macrosettori"].add(m)
            giornalisti[g]["titoli"].append(a.get('titolo', ''))

        # Converti set in lista
        for g in giornalisti.values():
            g["macrosettori"] = list(g["macrosettori"])

        return list(giornalisti.values())
    except Exception as e:
        print(f"Errore caricamento giornalisti: {e}")
        return []


# ─── STEP 3: Scoring affinità ─────────────────────────────────────────────────

def calcola_score(giornalista: dict, analisi: dict) -> float:
    """Calcola punteggio di affinità tra giornalista e comunicato."""
    score = 0.0
    keywords = [k.lower() for k in analisi.get('keywords', [])]
    settori  = [s.lower() for s in analisi.get('settori', [])]

    # Match macrosettori (peso alto)
    for macro in giornalista.get('macrosettori', []):
        for s in settori:
            if s in macro.lower() or macro.lower() in s:
                score += 3.0

    # Match keyword nei titoli (peso medio)
    titoli_text = ' '.join(giornalista.get('titoli', [])).lower()
    for kw in keywords:
        if kw in titoli_text:
            score += 1.5

    # Bonus volume articoli (max 2 punti)
    n_articoli = len(giornalista.get('articoli', []))
    score += min(n_articoli / 10, 2.0)

    return round(score, 2)


# ─── STEP 4: Genera spiegazione con AI ───────────────────────────────────────

def genera_spiegazione(giornalista: dict, analisi: dict, score: float) -> str:
    """Genera una spiegazione breve del perché il giornalista è adatto."""
    try:
        macrosettori = ', '.join(giornalista['macrosettori'][:5]) or 'vari settori'
        n = len(giornalista['articoli'])
        titoli_sample = '; '.join(giornalista['titoli'][:3])

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": (
                    "Sei un esperto di media relations. Scrivi UNA sola frase (max 25 parole) "
                    "che spiega perché questo giornalista è adatto per il comunicato. "
                    "Sii specifico e concreto. Solo la frase, nessun preambolo."
                )},
                {"role": "user", "content": (
                    f"Comunicato su: {analisi.get('tema')}\n"
                    f"Settori comunicato: {', '.join(analisi.get('settori', []))}\n"
                    f"Giornalista: {giornalista['nome']} ({giornalista['testata']})\n"
                    f"Scrive di: {macrosettori}\n"
                    f"Articoli recenti: {titoli_sample}\n"
                    f"Numero articoli: {n}"
                )}
            ],
            temperature=0.4,
            max_tokens=60
        )
        return response.choices[0].message.content.strip()
    except Exception:
        n = len(giornalista['articoli'])
        return f"Ha scritto {n} articoli su {', '.join(giornalista['macrosettori'][:2]) or 'temi affini'}."


# ─── FUNZIONE PRINCIPALE ──────────────────────────────────────────────────────

def pitch_advisor(testo_comunicato: str, top_n: int = 10) -> dict:
    """
    Analizza il comunicato e restituisce i top N giornalisti più adatti.
    """
    if not testo_comunicato or len(testo_comunicato.strip()) < 50:
        return {"error": "Comunicato troppo corto. Inserisci almeno 50 caratteri."}

    # 1. Analizza comunicato
    print("[PITCH] Analisi comunicato...")
    analisi = analizza_comunicato(testo_comunicato)
    print(f"[PITCH] Tema: {analisi.get('tema')} | Settori: {analisi.get('settori')}")

    # 2. Carica giornalisti
    print("[PITCH] Caricamento giornalisti dal DB...")
    giornalisti = carica_giornalisti(giorni=180)
    print(f"[PITCH] {len(giornalisti)} giornalisti trovati")

    if not giornalisti:
        return {"error": "Nessun giornalista nel database. Carica prima dei CSV."}

    # 3. Calcola score
    scored = []
    for g in giornalisti:
        s = calcola_score(g, analisi)
        if s > 0:
            scored.append((g, s))

    scored.sort(key=lambda x: -x[1])
    top = scored[:top_n]

    if not top:
        return {"error": "Nessun giornalista affine trovato. Prova ad arricchire il database con più CSV."}

    # 4. Genera spiegazioni per i top
    print(f"[PITCH] Generazione spiegazioni per top {len(top)}...")
    risultati = []
    for g, score in top:
        spiegazione = genera_spiegazione(g, analisi, score)
        # Prendi i 3 articoli più recenti come prova
        articoli_recenti = sorted(
            g['articoli'], key=lambda x: x.get('data', ''), reverse=True
        )[:3]
        risultati.append({
            "nome":             g['nome'],
            "testata":          g['testata'],
            "score":            score,
            "n_articoli":       len(g['articoli']),
            "macrosettori":     g['macrosettori'][:5],
            "spiegazione":      spiegazione,
            "articoli_recenti": [
                {"titolo": a.get('titolo',''), "data": a.get('data','')}
                for a in articoli_recenti
            ]
        })

    return {
        "analisi":    analisi,
        "risultati":  risultati
    }