import feedparser
import hashlib
import datetime
import requests
from bs4 import BeautifulSoup
from services.database import supabase


def clean_text(s):
    return ' '.join(str(s or '').strip().lower().split())


def make_hash(title: str, url: str) -> str:
    key = f"{clean_text(title)}|{url.strip()}"
    return hashlib.sha256(key.encode('utf-8')).hexdigest()


def load_sources() -> list[dict]:
    try:
        res = supabase.table("monitored_sources").select("*").eq("active", True).execute()
        return res.data or []
    except Exception as e:
        print(f"Errore caricamento sorgenti: {e}")
        return []


def load_clients() -> list[dict]:
    try:
        res = supabase.table("clients").select("id, name, keywords").execute()
        return res.data or []
    except Exception as e:
        print(f"Errore caricamento clienti: {e}")
        return []


def parse_keywords(raw: str) -> list[str]:
    if not raw:
        return []
    return [k.strip().lower() for k in raw.replace('\n', ',').split(',') if k.strip()]


def match_clients(text: str, clients: list[dict]) -> tuple[str, str]:
    """Restituisce (nomi_clienti_matchati, keyword_trovate)"""
    text_low = text.lower()
    matched_clients, matched_kws = [], []
    for client in clients:
        keywords = parse_keywords(client.get('keywords', ''))
        hits = [kw for kw in keywords if kw in text_low]
        if hits:
            matched_clients.append(client['name'])
            matched_kws.extend(hits)
    return ', '.join(matched_clients), ', '.join(set(matched_kws))


def fetch_rss(source: dict, clients: list[dict]) -> list[dict]:
    """Scarica e processa un feed RSS"""
    records = []
    try:
        feed = feedparser.parse(source['url'])
        for entry in feed.entries:
            title   = entry.get('title', '')
            summary = entry.get('summary', '')
            link    = entry.get('link', '')
            text    = f"{title} {summary}"

            matched_client, matched_kws = match_clients(text, clients)
            if not matched_client:
                continue  # nessun match, salta

            # Data pubblicazione
            published = datetime.date.today().isoformat()
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                try:
                    published = datetime.date(*entry.published_parsed[:3]).isoformat()
                except Exception:
                    pass

            content_hash = make_hash(title, link)

            records.append({
                'source_name':       source['name'],
                'source_url':        source['url'],
                'title':             title,
                'url':               link,
                'published_at':      published,
                'summary':           BeautifulSoup(summary, 'html.parser').get_text()[:1000],
                'full_text':         '',
                'matched_client':    matched_client,
                'matched_keywords':  matched_kws,
                'content_hash':      content_hash,
                'tone':              'Neutral',
                'reputational_risk': 'None',
            })
    except Exception as e:
        print(f"Errore RSS {source['url']}: {e}")
    return records


def fetch_scrape(source: dict, clients: list[dict]) -> list[dict]:
    """Scraping base per siti senza RSS"""
    records = []
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(source['url'], headers=headers, timeout=10)
        soup = BeautifulSoup(resp.text, 'html.parser')
        links = soup.find_all('a', href=True)

        for a in links:
            title = a.get_text(strip=True)
            link  = a['href']
            if not title or len(title) < 20:
                continue
            if not link.startswith('http'):
                continue

            matched_client, matched_kws = match_clients(title, clients)
            if not matched_client:
                continue

            content_hash = make_hash(title, link)
            records.append({
                'source_name':       source['name'],
                'source_url':        source['url'],
                'title':             title,
                'url':               link,
                'published_at':      datetime.date.today().isoformat(),
                'summary':           '',
                'full_text':         '',
                'matched_client':    matched_client,
                'matched_keywords':  matched_kws,
                'content_hash':      content_hash,
                'tone':              'Neutral',
                'reputational_risk': 'None',
            })
    except Exception as e:
        print(f"Errore scraping {source['url']}: {e}")
    return records


def run_monitoring() -> dict:
    """Funzione principale — chiamata dallo scheduler"""
    print(f"[MONITOR] Avvio scansione: {datetime.datetime.now().isoformat()}")

    sources = load_sources()
    clients = load_clients()

    if not sources:
        print("[MONITOR] Nessuna sorgente attiva.")
        return {'status': 'ok', 'found': 0}

    if not clients:
        print("[MONITOR] Nessun cliente con keyword.")
        return {'status': 'ok', 'found': 0}

    all_records = []
    for source in sources:
        if source.get('type') == 'scrape':
            records = fetch_scrape(source, clients)
        else:
            records = fetch_rss(source, clients)
        print(f"[MONITOR] {source['name']}: {len(records)} match trovati")
        all_records.extend(records)

    if not all_records:
        print("[MONITOR] Nessun nuovo articolo trovato.")
        return {'status': 'ok', 'found': 0}

    # Deduplicazione interna
    seen, deduped = set(), []
    for r in all_records:
        if r['content_hash'] not in seen:
            seen.add(r['content_hash'])
            deduped.append(r)

    # Upsert su Supabase
    try:
        result = supabase.table("web_mentions").upsert(
            deduped, on_conflict="content_hash"
        ).execute()
        inserted = len(result.data) if result.data else 0
        print(f"[MONITOR] Inseriti: {inserted} | Già presenti ignorati: {len(deduped)-inserted}")
        return {'status': 'ok', 'found': inserted}
    except Exception as e:
        print(f"[MONITOR] Errore upsert: {e}")
        return {'status': 'error', 'message': str(e)}