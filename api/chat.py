"""
api/chat.py  —  SPIZ AI v16
═══════════════════════════════════════════════════════════════════

DUE TIPI DI REPORT:

  posizionamento_giornalisti
    → Benchmark: MPS_Report_Maim_2026.docx
    → Struttura:
        1. Executive Summary
        2. Analisi Quantitativa — Testate e Volumi
        3. Analisi Quantitativa — Giornalisti e Frequenza
        4. Mappatura Sentiment — Amici / Neutrali / Critici
        5. Il Contesto Narrativo — La Tesi da Difendere
        6. Raccomandazione Strategica — A Chi Puntare
        7. Valutazioni Aggiuntive e Rischi
        8. Piano d'Azione Sintetico
    → Richiede: refinement (tesi del cliente)
    → Statistiche Python pre-calcolate sono il cuore del report.
       L'AI classifica i giornalisti rispetto alla tesi, non in astratto.

  analisi_narrazione
    → Paesaggio mediatico, temi, frame, spazi di posizionamento, pitch
    → refinement opzionale (contesto aggiuntivo)

ARCHITETTURA:
  ≤ 40 articoli  →  DIRECT (testo completo a GPT-4o, nessun map)
  > 40 articoli  →  MAP-REDUCE (map ricco → sintesi → reduce)
"""

import os, re, json
from dotenv import load_dotenv
load_dotenv()

from datetime import date, timedelta
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
from services.database import supabase

ai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

DB_COLS = (
    "id, testata, data, giornalista, occhiello, titolo, sottotitolo, "
    "testo_completo, macrosettori, tipologia_articolo, tone, "
    "dominant_topic, reputational_risk, political_risk, ave, tipo_fonte"
)

DIRECT_THRESHOLD  = 40
DIRECT_TEXT_CHARS = 3000
MAP_TEXT_CHARS    = 2000
MAP_BATCH_SIZE    = 5
MAP_MAX_WORKERS   = 6

SKIP_GIORNALISTI = {"", "redazione", "n.d.", "n/d", "autore non indicato", "anonimo"}


# ══════════════════════════════════════════════════════════════════════
# PARSING TEMPORALE
# ══════════════════════════════════════════════════════════════════════

_TIME_RULES = [
    (r"oggi|odiern",                                               0),
    (r"ultime?\s*24.?ore|ieri",                                    1),
    (r"ultim[ie]\s*(?:[23]\s*(?:giorn|gg\b|g\b))",                3),
    (r"ultim[ie]\s*(?:[67]\s*(?:giorn|gg\b|g\b)|settiman|7\s*(?:giorn|gg))", 7),
    (r"ultim[ie]\s*(?:15\s*(?:giorn|gg\b)|due\s*settiman)",       15),
    (r"ultim[ie]\s*(?:30\s*(?:giorn|gg\b|g\b)?)\b|ultimo\s*mese|mese\s*scors", 30),
    (r"ultim[ie]\s*(?:[23]\s*mesi|[69]0\s*giorn)",                90),
    (r"ultim[ie]\s*(?:[46]\s*mesi)",                             180),
    (r"ultimo\s*anno|ultim[ie]\s*12\s*mesi",                     365),
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
# STATISTICHE PYTHON — DATI DURI, NESSUNA AI
# Calcola tutto in Python. L'AI riceve fatti, non deve indovinarli.
# ══════════════════════════════════════════════════════════════════════

def _compute_stats(articles: list) -> dict:
    if not articles:
        return {}

    testate_count     = Counter()
    testate_ave       = defaultdict(float)
    giornalisti_count = Counter()
    giornalisti_test  = {}   # giornalista → testata
    giornalisti_tone  = defaultdict(Counter)  # giornalista → {tone: n}
    tones             = Counter()
    rep_risks         = Counter()
    topics            = Counter()
    monthly           = Counter()
    dates             = []

    for a in articles:
        testata     = (a.get("testata")     or "").strip()
        giornalista = (a.get("giornalista") or "").strip()
        data        = (a.get("data")        or "").strip()
        tone        = (a.get("tone")        or "").strip()
        risk        = (a.get("reputational_risk") or "").strip()
        topic       = (a.get("dominant_topic")    or "").strip()
        ave_raw     = a.get("ave")

        testate_count[testata] += 1
        if ave_raw:
            try:
                testate_ave[testata] += float(ave_raw)
            except (ValueError, TypeError):
                pass

        if giornalista.lower() not in SKIP_GIORNALISTI:
            giornalisti_count[giornalista] += 1
            if giornalista not in giornalisti_test:
                giornalisti_test[giornalista] = testata
            if tone:
                giornalisti_tone[giornalista][tone] += 1

        if tone:    tones[tone]   += 1
        if risk:    rep_risks[risk] += 1
        if topic:   topics[topic]   += 1
        if data:
            dates.append(data)
            monthly[data[:7]] += 1

    tone_tot = sum(tones.values()) or 1

    # Righe formattate per i prompt
    testate_rows = []
    for t, cnt in testate_count.most_common(20):
        ave_s = f"{testate_ave[t]:,.0f}€" if testate_ave[t] else "N/D"
        testate_rows.append(f"  {t}: {cnt} art. | AVE: {ave_s}")

    giorn_rows = []
    for g, cnt in giornalisti_count.most_common(25):
        test = giornalisti_test.get(g, "")
        # Tone prevalente per giornalista
        tone_dist = giornalisti_tone.get(g, Counter())
        tone_s = ", ".join(f"{k}:{v}" for k, v in tone_dist.most_common(3)) if tone_dist else "N/D"
        giorn_rows.append(f"  {g} ({test}): {cnt} art. | tone: {tone_s}")

    trend_rows = [f"  {m}: {cnt} art." for m, cnt in sorted(monthly.items())]

    return {
        "totale":           len(articles),
        "periodo_da":       min(dates) if dates else "",
        "periodo_a":        max(dates) if dates else "",
        "testate_count":    dict(testate_count.most_common(20)),
        "testate_ave":      {k: round(v, 2) for k, v in testate_ave.items()},
        "giornalisti_count":dict(giornalisti_count.most_common(30)),
        "giornalisti_test": giornalisti_test,
        "giornalisti_tone": {g: dict(c) for g, c in giornalisti_tone.items()},
        "sentiment":        {k: round(v/tone_tot*100) for k,v in tones.items() if k},
        "reputational":     dict(rep_risks.most_common()),
        "topics":           dict(topics.most_common(8)),
        "ave_totale":       round(sum(testate_ave.values()), 2),
        "monthly":          dict(sorted(monthly.items())),
        # Stringhe pronte per i prompt
        "_testate_block":   "\n".join(testate_rows),
        "_giornalisti_block": "\n".join(giorn_rows),
        "_trend_block":     "\n".join(trend_rows),
    }


def _stats_prompt_block(stats: dict) -> str:
    return (
        f"TOTALE ARTICOLI: {stats.get('totale', 0)}\n"
        f"PERIODO: {stats.get('periodo_da','')} → {stats.get('periodo_a','')}\n"
        f"AVE TOTALE STIMATO: {stats.get('ave_totale', 0):,.0f}€\n\n"
        f"TESTATE (articoli | AVE stimato):\n{stats.get('_testate_block','')}\n\n"
        f"GIORNALISTI con firma (articoli | tone distribuzione):\n{stats.get('_giornalisti_block','')}\n\n"
        f"TREND MENSILE:\n{stats.get('_trend_block','')}\n\n"
        f"SENTIMENT CORPUS: {', '.join(f'{k}: {v}%' for k,v in stats.get('sentiment',{}).items())}\n"
        f"RISCHIO REPUTAZIONALE: {', '.join(f'{k}: {v}' for k,v in stats.get('reputational',{}).items())}\n"
        f"TEMI DOMINANTI: {', '.join(f'{k}({v})' for k,v in list(stats.get('topics',{}).items())[:6])}"
    )


def _article_block(a: dict, max_chars: int) -> str:
    testo = (a.get("testo_completo") or "")[:max_chars]
    return (
        f"TESTATA: {a.get('testata','')} | DATA: {a.get('data','')} | "
        f"AVE: {a.get('ave','')} | TIPO: {a.get('tipo_fonte','')}\n"
        f"GIORNALISTA: {a.get('giornalista','')}\n"
        f"TONE: {a.get('tone','')} | RISCHIO_REP: {a.get('reputational_risk','')} | "
        f"TOPIC: {a.get('dominant_topic','')}\n"
        f"TITOLO: {a.get('titolo','')}\n"
        f"OCCHIELLO: {a.get('occhiello','')}\n"
        f"TESTO: {testo}"
    )


# ══════════════════════════════════════════════════════════════════════
# PROMPT — POSIZIONAMENTO GIORNALISTI
# Benchmark: MPS_Report_Maim_2026.docx
# ══════════════════════════════════════════════════════════════════════

def _build_posizionamento_system(
    client_name: str,
    topic_name: str,
    refinement: str,
) -> str:
    subject = client_name or topic_name or "il soggetto del cliente"
    return f"""Sei SPIZ, analista senior di MAIM Public Diplomacy & Media Relations.
Produci un report professionale riservato destinato al team di comunicazione del cliente.
Il documento sarà usato per guidare le relazioni con i media nelle prossime settimane.

══════ SOGGETTO E TESI ══════

SOGGETTO PRINCIPALE: {subject}

CONTESTO E TESI DEL CLIENTE:
{refinement}

Questa tesi è l'asse centrale di tutto il report.
"Amico", "neutrale", "critico" significa: rispetto a questa tesi specifica,
non in astratto. Un giornalista può essere tecnicamente neutrale sul settore
ma critico rispetto a questa tesi — e va classificato come critico.

══════ REGOLE NON NEGOZIABILI ══════

1. Ogni classificazione di un giornalista DEVE essere motivata con titoli
   specifici tratti dal corpus. Non opinioni generali: cita il titolo esatto,
   la testata, la data.
2. Non inventare orientamenti. Se il corpus non contiene abbastanza articoli
   firmati da un giornalista per classificarlo, scrivilo esplicitamente.
3. Le statistiche quantitative (volumi, AVE, conteggi) ti vengono fornite
   già calcolate. Usale esatte, non arrotondarle né reinterpretarle.
4. Sezione 6 — A Chi Puntare: ogni raccomandazione deve spiegare PERCHÉ
   quel giornalista è ricettivo a questa tesi ADESSO, basandosi su titoli
   reali del corpus. Non su caratteristiche generali della testata.
5. Sezione 8 — Piano d'Azione: ogni azione deve essere concreta e datata.
   Non "organizzare un briefing": "briefing con [nome] del [testata] entro
   [data], con focus su [argomento specifico emerso dal corpus]".
6. Italiano professionale corporate. Nessuna emoji. Nessun aggettivo
   non motivato da un dato.
7. Nota riservata in fondo: il documento è ad uso interno del cliente.

══════ STRUTTURA OBBLIGATORIA ══════

## 1. EXECUTIVE SUMMARY

Massimo 200 parole. Risponde a: quanta copertura, con quale orientamento
prevalente rispetto alla tesi del cliente, qual è la sfida comunicativa
principale, chi sono i 2-3 interlocutori chiave da attivare subito.
Non descrivere il documento: sintetizza le conclusioni operative.

## 2. ANALISI QUANTITATIVA — TESTATE E VOLUMI

Usa le statistiche fornite. Presenta:
- Tabella testata / n. articoli / AVE stimato / orientamento generale
  rispetto alla tesi del cliente (non generico: rispetto a questa tesi)
- Trend mensile se il periodo è > 2 settimane
- 3-4 righe di interpretazione: quali testate guidano la narrazione
  e in che direzione, rispetto alla tesi

## 3. ANALISI QUANTITATIVA — GIORNALISTI E FREQUENZA

Usa le statistiche fornite. Presenta:
- Tabella: giornalista / testata / n. articoli / orientamento rispetto
  alla tesi (Favorevole / Neutrale / Critico)
- Escludi le voci "Redazione" e anonimi
- Sotto la tabella, 2-3 righe: chi domina la copertura firmata e
  cosa significa per la strategia

## 4. MAPPATURA DEL SENTIMENT — AMICI, NEUTRALI, CRITICI

Per ogni giornalista rilevante (quelli con più articoli nel corpus)
produci una scheda individuale con questo formato esatto:

### [NOME COGNOME] — [Testata] ([N] articoli)
**Classificazione**: FAVOREVOLE / NEUTRALE / CRITICO rispetto alla tesi

**Evidenze dal corpus**: cita 2-3 titoli reali con data che giustificano
la classificazione. Non parafrasi: titoli esatti come appaiono negli articoli.

**Linguaggio rivelatore**: 2-3 parole o frasi specifiche usate da questo
giornalista che rivelano il suo frame narrativo rispetto alla tesi.

**Profilo audience**: lettori stimati della testata, tipo di pubblico
(istituzionale/finanziario/generalista/politico).

**Approccio raccomandato**: una frase secca su come gestire questo giornalista.

Raggruppa le schede in tre blocchi: 4.1 FAVOREVOLI, 4.2 NEUTRALI, 4.3 CRITICI.

## 5. IL CONTESTO NARRATIVO — LA TESI DA DIFENDERE

5.1 Ripeti la tesi del cliente in un box chiaramente delimitato.

5.2 FRAME DOMINANTE NEL CORPUS: come la maggior parte dei media sta
inquadrando la vicenda ADESSO. Cita testate e titoli specifici.
Spiega perché questo frame è problematico per la tesi del cliente.

5.3 CONTRO-ARGOMENTI DA PRESIDIARE: 3-5 punti specifici che il corpus
segnala come vulnerabilità della tesi. Per ognuno, indica quale testata
o giornalista li sta amplificando, e come si potrebbe rispondere
con dati o fatti già presenti nel corpus.

## 6. RACCOMANDAZIONE STRATEGICA — A CHI PUNTARE

Tre livelli di priorità:

### 6.1 Priorità Alta — Target Primari
Per ogni target (massimo 3):
- Nome giornalista, testata, lettori stimati
- Perché ORA: quale elemento del corpus rende questo il momento giusto
- Approccio suggerito: forma del contatto (briefing / intervista /
  documento tecnico / off-the-record), argomento specifico, angolo
- Obiettivo: che tipo di articolo si vuole ottenere, con quale titolo ideale

### 6.2 Priorità Media — Target Secondari
Formato più sintetico. 2-3 nomi con motivazione breve.

### 6.3 Da Non Approcciare
Testate o giornalisti da evitare nella fase attuale, con motivazione
basata su titoli specifici del corpus.

## 7. VALUTAZIONI AGGIUNTIVE E RISCHI

Segnala 2-4 rischi o elementi rilevanti emersi dal corpus che il cliente
deve monitorare nelle prossime settimane. Ogni punto deve essere ancorato
a qualcosa di specifico nel corpus (testata, giornalista, titolo, dinamica).

Esempi di categorie: rischio giudiziario come amplificatore mediatico,
copertura internazionale, dinamiche tra fazioni di giornalisti,
possibili escalation narrative.

## 8. PIANO D'AZIONE SINTETICO

Azioni concrete, con orizzonte temporale esplicito (es. "Settimana 1",
"entro il [data]"). Ogni azione deve indicare: cosa fare, con chi,
con quale obiettivo specifico. Massimo 6-8 azioni.

---

Chiudi SEMPRE con questa riga separata:
**CORPUS:** [N] articoli · [testate principali con conteggio] · [DATA_DA] → [DATA_A]
**DOCUMENTO RISERVATO** — Uso interno Agenzia MAIM e cliente. Non distribuire.
"""


# ══════════════════════════════════════════════════════════════════════
# PROMPT — ANALISI NARRAZIONE
# ══════════════════════════════════════════════════════════════════════

def _build_narrazione_system(
    client_name: str,
    topic_name: str,
    refinement: str,
) -> str:
    subject = client_name or topic_name or ""
    focus_block = ""
    if subject:
        focus_block = f"\nSOGGETTO/ARGOMENTO: {subject}\n"
    if refinement:
        focus_block += f"CONTESTO AGGIUNTIVO: {refinement}\n"

    return f"""Sei SPIZ, analista senior di comunicazione e media intelligence di MAIM.
{focus_block}
══════ REGOLE NON NEGOZIABILI ══════

1. Ogni affermazione deve essere ricavabile dal corpus. Cita sempre:
   testata, nome giornalista, data, titolo.
2. Se un elemento non è nel corpus: "Non emergono elementi su questo
   punto nel corpus."
3. Nessuna genericità. Parole come "stabilità", "trasparenza",
   "innovazione" senza articolo specifico a supporto = fallimento.
4. Ogni spazio narrativo e ogni pitch nascono da un fatto SPECIFICO
   presente nel corpus. Non da temi generali di settore.
5. Italiano professionale. Nessuna emoji. Nessun aggettivo non motivato.

══════ STRUTTURA OBBLIGATORIA ══════

## 1. PAESAGGIO MEDIATICO

15-20 righe. Non descrivere: interpretare.
- Tono prevalente e cosa rivela sull'atteggiamento reale dei media
- Quali testate guidano la narrazione e con quale angolo
- Fratture narrative tra testate: quali e perché
- Giornalisti che ritornano: cosa stanno costruendo
- Dove si sta evolvendo il dibattito

## 2. ANALISI TEMI E NARRATIVE

Per ciascun tema rilevante (3-5 temi):

### [Nome tema]
- **Fatto**: evento specifico dal corpus (testata, giornalista, data, titolo)
- **Chi lo dice e come**: angolazione, tono, frame narrativo
- **Attori e ruoli**: protagonista, antagonista, arbitro
- **Tensione**: conflitto sottostante
- **Dove porta**: evoluzione probabile

## 3. SPAZI NARRATIVI — OPPORTUNITÀ DI POSIZIONAMENTO

4-6 spazi. Per ciascuno, minimo 10 righe:

### [Titolo sintetico del frame]

**Fatto di innesco**: quale articolo/evento specifico del corpus apre
questo spazio. Testata, giornalista, data, titolo. Non inventare.

**Perché ora**: cosa sta succedendo ADESSO nel corpus che rende
questo spazio rilevante oggi e non fra un mese.

**Cosa fare/dire**: azione o messaggio concreto. Non "comunicare la
propria posizione": scrivi esattamente cosa e in quale forma.

**Titolo come uscirebbe**: costruisci il titolo come un redattore,
non come un ufficio stampa.

**Giornalista target e perché**: nome dal corpus, cosa ha già scritto,
perché questo lo rende il profilo giusto.

## 4. PITCH PRONTI ORA

3 pitch concreti proponibili nei prossimi 7 giorni:

### Pitch [N]
- **Titolo come uscirebbe**: calibrato sulla testata, non generico
- **Testata e giornalista**: con motivazione dalla copertura nel corpus
- **Taglio**: notizia / analisi / intervista / dossier / commento
- **Gancio di attualità**: perché una redazione lo aprirebbe OGGI
- **Angolo inedito**: cosa c'è che non è già stato scritto

---

**CORPUS:** [N] articoli · [testate principali] · [DATA_DA] → [DATA_A]
"""


# ══════════════════════════════════════════════════════════════════════
# PERCORSO DIRETTO — ≤ 40 articoli
# ══════════════════════════════════════════════════════════════════════

def _direct_report(
    articles: list,
    stats: dict,
    system_prompt: str,
) -> str:
    corpus_blocks = "\n\n════\n\n".join(
        _article_block(a, DIRECT_TEXT_CHARS) for a in articles
    )
    resp = ai.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": (
                f"STATISTICHE PRE-CALCOLATE:\n{_stats_prompt_block(stats)}\n\n"
                f"CORPUS COMPLETO ({len(articles)} articoli):\n\n{corpus_blocks}"
            )},
        ],
        temperature=0.05,
        max_tokens=6000,
    )
    return resp.choices[0].message.content.strip()


# ══════════════════════════════════════════════════════════════════════
# MAP — > 40 articoli
# ══════════════════════════════════════════════════════════════════════

_MAP_SYSTEM = """Sei un analista di media intelligence. Leggi gli articoli e per ciascuno
estrai un oggetto JSON. Restituisci SOLO un JSON valido con struttura: {"articoli": [...]}

Per ogni articolo:
- "testata": nome testata
- "data": data
- "giornalista": nome (o "Redazione")
- "titolo": titolo esatto dell'articolo
- "tone": valore pre-calcolato ricevuto
- "reputational_risk": valore pre-calcolato
- "ave": valore AVE ricevuto
- "storia": la notizia in UNA riga — soggetto + verbo + oggetto concreti.
  Non "l'articolo parla di X". Es: "BCE contesta i nuovi vertici di MPS
  sulla solidità del piano" oppure "Lovaglio presenta lista concorrente"
- "frame": attacco / difesa / indagine / elogio / allarme / analisi / cronaca
- "linguaggio": array di 3-5 parole o brevi frasi ESATTE usate dall'articolo
  che rivelano il frame narrativo del giornalista. Es: ["sfogo del banchiere",
  "impallinato", "guerra di potere"]
- "citazioni": array di max 2 citazioni dirette con speaker.
  {"speaker": "nome", "testo": "citazione esatta"} — solo se presenti nel testo.
  Altrimenti: []
- "fatto_chiave": il dato, numero, dichiarazione o accusa più concreta
  e verificabile. Non generalizzazioni.
Nessun testo fuori dal JSON."""


def _map_batch(batch: list, idx: int) -> tuple:
    blocks = "\n\n════\n\n".join(_article_block(a, MAP_TEXT_CHARS) for a in batch)
    try:
        resp = ai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": _MAP_SYSTEM},
                {"role": "user",   "content": blocks},
            ],
            temperature=0.0,
            max_tokens=4000,
            response_format={"type": "json_object"},
        )
        parsed = json.loads(resp.choices[0].message.content)
        items  = parsed.get("articoli", []) if isinstance(parsed, dict) else []
        return idx, items if isinstance(items, list) else []
    except Exception as e:
        print(f"[MAP v16] batch {idx} error: {e}")
        return idx, []


def _map_parallel(articles: list) -> list:
    batches = [articles[i:i+MAP_BATCH_SIZE] for i in range(0, len(articles), MAP_BATCH_SIZE)]
    results = [None] * len(batches)
    with ThreadPoolExecutor(max_workers=MAP_MAX_WORKERS) as ex:
        futs = {ex.submit(_map_batch, b, i): i for i, b in enumerate(batches)}
        for f in as_completed(futs):
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

def _reduce_report(
    extracted: list,
    stats: dict,
    system_prompt: str,
) -> str:
    extracted_txt = json.dumps(extracted, ensure_ascii=False, separators=(',', ':'))
    if len(extracted_txt) > 18000:
        extracted_txt = extracted_txt[:18000] + "...]"

    resp = ai.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": (
                f"STATISTICHE PRE-CALCOLATE:\n{_stats_prompt_block(stats)}\n\n"
                f"ARTICOLI ESTRATTI (strutturati, {len(extracted)} articoli):\n{extracted_txt}"
            )},
        ],
        temperature=0.05,
        max_tokens=6000,
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
    report_type: str = "posizionamento_giornalisti",
    refinement: str = "",
) -> dict:
    client_name  = (client_name  or "").strip()
    topic_name   = (topic_name   or "").strip()
    refinement   = (refinement   or "").strip()
    report_type  = (report_type  or "posizionamento_giornalisti").strip()

    print(f"[SPIZ v16] report_type={report_type} client={client_name} topic={topic_name}")

    # ── 1. ARTICOLI ──────────────────────────────────────────────────
    if preloaded_articles:
        articles = preloaded_articles
        print(f"[SPIZ v16] preloaded: {len(articles)} articoli")
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
            print(f"[SPIZ v16] search error: {e}")
            articles = []
        if not articles:
            try:
                res = (supabase.table("articles")
                       .select(DB_COLS)
                       .gte("data", from_date)
                       .lte("data", to_date)
                       .order("ave", desc=True)
                       .limit(150)
                       .execute())
                articles = res.data or []
            except Exception as e:
                print(f"[SPIZ v16] fallback error: {e}")

    if not articles:
        return {"error": "Nessun articolo trovato.", "is_report": False}

    # ── 2. STATISTICHE ───────────────────────────────────────────────
    stats = _compute_stats(articles)
    n     = len(articles)
    print(f"[SPIZ v16] {n} articoli | soglia direct={DIRECT_THRESHOLD}")

    # ── 3. SCEGLI PROMPT ─────────────────────────────────────────────
    if report_type == "posizionamento_giornalisti":
        system_prompt = _build_posizionamento_system(client_name, topic_name, refinement)
    else:
        system_prompt = _build_narrazione_system(client_name, topic_name, refinement)

    # ── 4. GENERA ────────────────────────────────────────────────────
    if n <= DIRECT_THRESHOLD:
        print("[SPIZ v16] → DIRECT path")
        report = _direct_report(articles, stats, system_prompt)
    else:
        print(f"[SPIZ v16] → MAP-REDUCE path ({n} articoli)")
        extracted = _map_parallel(articles[:80])
        print(f"[SPIZ v16] map: {len(extracted)} estratti")
        report = _reduce_report(extracted, stats, system_prompt)

    # ── 5. RISPOSTA ──────────────────────────────────────────────────
    return {
        "response":      report,
        "is_report":     True,
        "articles_used": n,
        "period_from":   stats.get("periodo_da", ""),
        "period_to":     stats.get("periodo_a", ""),
        "articles_list": [
            {
                "id":          a.get("id", ""),
                "testata":     a.get("testata", ""),
                "data":        a.get("data", ""),
                "titolo":      a.get("titolo", ""),
                "giornalista": a.get("giornalista", ""),
                "tone":        a.get("tone", ""),
                "ave":         a.get("ave", ""),
            }
            for a in articles
        ],
    }


# ══════════════════════════════════════════════════════════════════════
# DIGEST GIORNALIERO — formato WhatsApp
# ══════════════════════════════════════════════════════════════════════

def _digest_article_block(a: dict, max_chars: int = 500) -> str:
    testo = (a.get("testo_completo") or "")[:max_chars]
    gior  = a.get("giornalista") or "Redazione"
    return (
        f"[{a.get('testata','')} | {a.get('data','')} | {gior}]\n"
        f"TITOLO: {a.get('titolo','')}\n"
        f"TESTO: {testo}"
    )


def generate_digest(articles_today: list, clients: list) -> dict:
    """
    Genera il digest mattutino in testo piano per WhatsApp.

    Architettura:
      - articles_today : articoli leggeri (NO testo_completo) — solo per conteggio e temi
      - Per ogni cliente → query Supabase ilike su titolo+occhiello+testo_completo
        così si trovano citazioni ovunque nell'articolo, senza limite di 300
      - GPT: 1 chiamata per i temi + 1 chiamata per cliente (sezione ordinata)
      - Output: una sezione per cliente, tutti i suoi articoli raggruppati

    articles_today : lista leggera (titolo, testata, giornalista, data, tone, ave)
    clients        : lista di dict con campo 'name'
    """
    if not articles_today:
        return {
            "error": "Nessun articolo trovato per oggi.",
            "text": "",
            "articles_today": 0,
            "client_mentions": 0,
        }

    today         = date.today().isoformat()
    today_str     = date.today().strftime("%d/%m/%Y")
    n_art         = len(articles_today)
    client_names  = [c.get("name", "").strip() for c in (clients or []) if c.get("name")]

    _SYS = (
        "Sei il sistema MAIM Intelligence. Produci contenuto per il digest "
        "mattutino interno dell'agenzia, da condividere su WhatsApp.\n"
        "FORMATO: testo semplice, *grassetto* solo per titoli sezione e nomi clienti, "
        "separatori ————————————————————, italiano professionale, no emoji eccessive."
    )

    # ══════════════════════════════════════════════════════════════════
    # CHIAMATA 1 — TEMI DEL GIORNO
    # Input: solo [Testata] Titolo di tutti gli articoli — token minimi
    # ══════════════════════════════════════════════════════════════════
    elenco_titoli = "\n".join(
        f"[{a.get('testata', '')}] {a.get('titolo', '')}"
        for a in articles_today
    )

    print("[DIGEST] Chiamata 1 — temi del giorno")
    resp1 = ai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": _SYS},
            {"role": "user", "content": (
                f"Data: {today_str} | Totale articoli: {n_art}\n\n"
                f"ELENCO ARTICOLI DI OGGI:\n{elenco_titoli}\n\n"
                f"Produci SOLO la sezione temi, in questo formato esatto:\n\n"
                f"*TEMI DEL GIORNO*\n\n"
                f"Identifica 4-6 temi ricorrenti. "
                f"Per ogni tema: nome breve in *grassetto*, poi 2-3 righe di spiegazione. "
                f"Testo fluente, no elenchi puntati. Cita testata quando rilevante."
            )},
        ],
        temperature=0.1,
        max_tokens=1200,
    )
    sezione_temi = resp1.choices[0].message.content.strip()

    # ══════════════════════════════════════════════════════════════════
    # QUERY SUPABASE PER CLIENTE — ilike su testo_completo+titolo+occhiello
    # Trova citazioni ovunque nell'articolo, senza limite di volume
    # ══════════════════════════════════════════════════════════════════
    # client_articles: { nome_cliente: [articoli con testo_completo] }
    client_articles: dict = {}
    total_citazioni = 0

    for name in client_names:
        try:
            res = (
                supabase.table("articles")
                .select(
                    "id, testata, data, giornalista, titolo, occhiello, "
                    "testo_completo, tone, ave"
                )
                .eq("data", today)
                .or_(
                    f"titolo.ilike.%{name}%,"
                    f"occhiello.ilike.%{name}%,"
                    f"testo_completo.ilike.%{name}%"
                )
                .order("ave", desc=True)
                .execute()
            )
            arts = res.data or []
            if arts:
                client_articles[name] = arts
                total_citazioni += len(arts)
                print(f"[DIGEST] {name}: {len(arts)} articoli trovati")
            else:
                print(f"[DIGEST] {name}: nessuna citazione oggi")
        except Exception as e:
            print(f"[DIGEST] errore query per {name}: {e}")

    print(f"[DIGEST] totale citazioni clienti: {total_citazioni}")

    # ══════════════════════════════════════════════════════════════════
    # CHIAMATA 2+ — UNA PER CLIENTE
    # Per ogni cliente: tutti i suoi articoli → sezione ordinata
    # testo_completo troncato a 1500 chars per articolo
    # ══════════════════════════════════════════════════════════════════
    def _art_block_cliente(a: dict) -> str:
        testo = (a.get("testo_completo") or "")[:1500]
        return (
            f"Testata: {a.get('testata', '')}\n"
            f"Giornalista: {a.get('giornalista') or 'Redazione'}\n"
            f"Data: {a.get('data', '')}\n"
            f"Titolo: {a.get('titolo', '')}\n"
            f"Testo:\n{testo}"
        )

    _ISTR_CLIENTE = (
        "Produci una voce per ogni articolo in questo formato esatto:\n\n"
        "[Testata] Giornalista — Data\n"
        "Titolo articolo\n"
        "→ 2-3 righe: cosa dice sul cliente, tono (positivo/neutro/critico)\n\n"
        "Separa ogni voce con una riga vuota.\n"
        "Produci SOLO le voci, senza intestazioni."
    )

    sezioni_clienti = []

    for nome_cliente, arts in client_articles.items():
        n_arts_cliente = len(arts)
        print(f"[DIGEST] GPT sezione cliente: {nome_cliente} ({n_arts_cliente} art.)")

        # Batch da 8 articoli se sono molti
        BATCH = 8
        parti = []
        for i in range(0, n_arts_cliente, BATCH):
            batch = arts[i:i + BATCH]
            blocchi = "\n\n---\n\n".join(_art_block_cliente(a) for a in batch)
            msg = (
                f"Cliente: *{nome_cliente}*\n"
                f"Articoli che lo citano oggi ({n_arts_cliente} totali):\n\n"
                f"{blocchi}\n\n"
                f"{_ISTR_CLIENTE}"
            )
            resp = ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": _SYS},
                    {"role": "user",   "content": msg},
                ],
                temperature=0.0,
                max_tokens=1500,
            )
            parti.append(resp.choices[0].message.content.strip())

        voci = "\n\n".join(parti)
        sezioni_clienti.append(
            f"*{nome_cliente.upper()}* — {n_arts_cliente} "
            f"{'articolo' if n_arts_cliente == 1 else 'articoli'}\n\n"
            f"{voci}"
        )

    if sezioni_clienti:
        sezione_clienti = ("\n\n" + "·" * 20 + "\n\n").join(sezioni_clienti)
    else:
        sezione_clienti = "Nessun cliente citato oggi nei media monitorati."

    # ── Assemblaggio finale (nessuna AI) ─────────────────────────────
    text = (
        f"*MAIM DIGEST — {today_str}*\n"
        f"{n_art} articoli raccolti oggi\n\n"
        f"————————————————————\n\n"
        f"{sezione_temi}\n\n"
        f"————————————————————\n\n"
        f"*I TUOI CLIENTI SUI MEDIA*\n\n"
        f"{sezione_clienti}\n\n"
        f"————————————————————\n"
        f"_MAIM Intelligence — uso interno_"
    )

    return {
        "text":            text,
        "articles_today":  n_art,
        "client_mentions": total_citazioni,
    }