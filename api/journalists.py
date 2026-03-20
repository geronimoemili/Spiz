from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import date, timedelta, datetime, timezone
from collections import Counter

router = APIRouter()


def _sb():
    from services.database import supabase
    return supabase


def _deduce_tipo(testata: str) -> str:
    if not testata:
        return "altro"
    t = testata.lower()
    if any(x in t for x in ["ansa", "adnkronos", "askanews", "agenzia", "dire "]):
        return "agenzia"
    if any(x in t for x in [".it", "web", "online", "blog", "news"]):
        return "web"
    if any(x in t for x in ["radio", "tv", "rai", "mediaset", "tg", "tele"]):
        return "radio_tv"
    if any(x in t for x in ["settimana", "mensile", "rivista", "magazine"]):
        return "periodico"
    return "quotidiano"


class JournalistModel(BaseModel):
    nome:               str
    sigla:              Optional[str] = None
    testata_principale: Optional[str] = None
    tipo_testata:       Optional[str] = None
    email:              Optional[str] = None
    cellulare:          Optional[str] = None
    note:               Optional[str] = None
    clienti_associati:  Optional[str] = None
    ruolo:              Optional[str] = None
    linkedin:           Optional[str] = None
    instagram:          Optional[str] = None
    x_twitter:          Optional[str] = None

class JournalistUpdate(BaseModel):
    nome:               Optional[str] = None
    sigla:              Optional[str] = None
    testata_principale: Optional[str] = None
    tipo_testata:       Optional[str] = None
    email:              Optional[str] = None
    cellulare:          Optional[str] = None
    note:               Optional[str] = None
    clienti_associati:  Optional[str] = None
    ruolo:              Optional[str] = None
    linkedin:           Optional[str] = None
    instagram:          Optional[str] = None
    x_twitter:          Optional[str] = None

class ManualArticleModel(BaseModel):
    giornalista:        str
    titolo:             str
    testata:            Optional[str] = None
    data:               Optional[str] = None
    testo_completo:     Optional[str] = None
    tone:               Optional[str] = None
    tipologia_articolo: Optional[str] = None
    macrosettori:       Optional[str] = None
    ave:                Optional[float] = None


@router.get("/api/journalists/list")
async def list_journalists(
    client_id:    Optional[str] = None,
    tipo_testata: Optional[str] = None,
    macro_id:     Optional[str] = None,
    period:       Optional[str] = "all",
    q:            Optional[str] = None,
):
    supabase = _sb()
    try:
        SKIP = {"", "n.d.", "n/d", "redazione", "autore non indicato"}
        today = date.today()
        from_date = None
        if period == "today":       from_date = today.isoformat()
        elif period == "yesterday": from_date = (today - timedelta(days=1)).isoformat(); today = today - timedelta(days=1)
        elif period == "7days":     from_date = (today - timedelta(days=7)).isoformat()
        elif period == "30days":    from_date = (today - timedelta(days=30)).isoformat()
        elif period == "6months":   from_date = (today - timedelta(days=180)).isoformat()

        crm_res = supabase.table("journalists").select("*").order("nome").execute()
        crm = {j["nome"].strip().lower(): j for j in (crm_res.data or [])}
        crm_names_set = set(crm.keys())
        # Aggiungi anche sigla come chiave per il lookup
        for j in (crm_res.data or []):
            if j.get("sigla"): crm[j["sigla"].strip().lower()] = j

        arts_q = supabase.table("articles").select("giornalista, testata, macrosettori, titolo, occhiello, testo_completo, data")
        if from_date:
            arts_q = arts_q.gte("data", from_date).lte("data", today.isoformat())
        articles = arts_q.execute().data or []

        clients_res = supabase.table("clients").select("id, name, keywords_press").execute()
        client_kws = {}
        for c in (clients_res.data or []):
            kws = [k.strip().lower() for k in (c.get("keywords_press") or "").split(",") if k.strip()]
            if kws: client_kws[c["name"]] = kws

        # Mappa sigla → nome_lower per giornalisti con sigla nel CRM
        sigla_map = {}  # sigla_lower → nome_lower
        for j in (crm_res.data or []):
            if j.get("sigla") and j.get("nome"):
                sigla_map[j["sigla"].strip().lower()] = j["nome"].strip().lower()

        art_count, art_testate, art_clienti = {}, {}, {}
        for a in articles:
            g = (a.get("giornalista") or "").strip()
            if not g or g.lower() in SKIP: continue
            gl = g.lower()
            # Se è una sigla nota, normalizza al nome principale
            if gl in sigla_map:
                gl = sigla_map[gl]
            art_count[gl] = art_count.get(gl, 0) + 1
            art_testate.setdefault(gl, Counter())[a.get("testata","") or ""] += 1
            txt = f"{a.get('titolo','')} {a.get('occhiello','')} {a.get('testo_completo','')}".lower()
            for cname, kws in client_kws.items():
                if any(kw in txt for kw in kws):
                    art_clienti.setdefault(gl, set()).add(cname)

        client_journalists = None
        if client_id:
            cl_res = supabase.table("clients").select("keywords_press").eq("id", client_id).execute()
            if cl_res.data:
                kws = [k.strip().lower() for k in (cl_res.data[0].get("keywords_press") or "").split(",") if k.strip()]
                client_journalists = set()
                for a in articles:
                    g = (a.get("giornalista") or "").strip()
                    if not g or g.lower() in SKIP: continue
                    txt = f"{a.get('titolo','')} {a.get('occhiello','')} {a.get('testo_completo','')}".lower()
                    if any(kw in txt for kw in kws):
                        client_journalists.add(g.lower())

        macro_journalists = None
        if macro_id:
            lnk = supabase.table("macro_group_links").select("official_macro_id").eq("macro_group_id", macro_id).execute()
            oids = [l["official_macro_id"] for l in (lnk.data or [])]
            if oids:
                mac = supabase.table("official_macrosectors").select("name").in_("id", oids).execute()
                mac_names = [m["name"].upper() for m in (mac.data or [])]
                macro_journalists = set()
                for a in articles:
                    g = (a.get("giornalista") or "").strip()
                    if not g or g.lower() in SKIP: continue
                    if any(mn in (a.get("macrosettori") or "").upper() for mn in mac_names):
                        macro_journalists.add(g.lower())

        result = []
        for gl in set(art_count.keys()) | set(crm.keys()):
            if client_journalists is not None and gl not in client_journalists: continue
            if macro_journalists is not None and gl not in macro_journalists: continue
            if q and q.lower() not in gl: continue
            crm_entry = crm.get(gl, {})
            testata_arch = art_testate[gl].most_common(1)[0][0] if gl in art_testate else ""
            tipo = crm_entry.get("tipo_testata") or _deduce_tipo(crm_entry.get("testata_principale") or testata_arch)
            if tipo_testata and tipo != tipo_testata: continue
            citati = set(art_clienti.get(gl, set()))
            for c in (crm_entry.get("clienti_associati") or "").split(","):
                cs = c.strip()
                if cs: citati.add(cs)
            result.append({
                "id":                 crm_entry.get("id"),
                "nome":               crm_entry.get("nome") or gl.title(),
                "testata_principale": crm_entry.get("testata_principale") or testata_arch,
                "tipo_testata":       tipo,
                "email":              crm_entry.get("email"),
                "cellulare":          crm_entry.get("cellulare"),
                "note":               crm_entry.get("note"),
                "clienti_citati":     ", ".join(sorted(citati)),
                "n_articoli":         art_count.get(gl, 0),
                "in_crm":             (gl in crm_names_set),
            })
        result.sort(key=lambda x: (-x["n_articoli"], x["nome"]))
        return result
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/journalists/{journalist_id}")
async def get_journalist(journalist_id: str):
    supabase = _sb()
    try:
        res = supabase.table("journalists").select("*").eq("id", journalist_id).execute()
        if not res.data: raise HTTPException(status_code=404, detail="Non trovato")
        return res.data[0]
    except HTTPException: raise
    except Exception as e: return {"error": str(e)}


@router.post("/api/journalists")
async def create_journalist(data: JournalistModel):
    supabase = _sb()
    try:
        res = supabase.table("journalists").insert({
            "nome": data.nome.strip(), "sigla": data.sigla,
            "testata_principale": data.testata_principale,
            "tipo_testata": data.tipo_testata, "email": data.email,
            "cellulare": data.cellulare, "note": data.note,
            "clienti_associati": data.clienti_associati,
            "ruolo": data.ruolo,
            "linkedin": data.linkedin, "instagram": data.instagram,
            "x_twitter": data.x_twitter,
        }).execute()
        return {"success": True, "journalist": res.data[0] if res.data else {}}
    except Exception as e: return {"error": str(e)}


@router.put("/api/journalists/{journalist_id}")
async def update_journalist(journalist_id: str, data: JournalistUpdate):
    supabase = _sb()
    try:
        update = {k: v for k, v in data.dict().items() if v is not None}
        update["updated_at"] = datetime.now(timezone.utc).isoformat()
        res = supabase.table("journalists").update(update).eq("id", journalist_id).execute()
        return {"success": True, "journalist": res.data[0] if res.data else {}}
    except Exception as e: return {"error": str(e)}


@router.delete("/api/journalists/{journalist_id}")
async def delete_journalist(journalist_id: str):
    supabase = _sb()
    try:
        supabase.table("journalists").delete().eq("id", journalist_id).execute()
        return {"success": True}
    except Exception as e: return {"error": str(e)}


@router.post("/api/journalists/sync-from-articles")
async def sync_journalists_from_articles():
    supabase = _sb()
    try:
        SKIP = {"", "n.d.", "n/d", "redazione", "autore non indicato"}
        arts = supabase.table("articles").select("giornalista, testata").execute().data or []
        existing = {j["nome"].strip().lower() for j in (supabase.table("journalists").select("nome").execute().data or [])}
        testate = {}
        for a in arts:
            g = (a.get("giornalista") or "").strip()
            if not g or g.lower() in SKIP: continue
            testate.setdefault(g.lower(), Counter())[a.get("testata","") or ""] += 1
        inserted = 0
        for gl, tc in testate.items():
            if gl in existing: continue
            tm = tc.most_common(1)[0][0]
            try:
                supabase.table("journalists").insert({
                    "nome": gl.title(),
                    "testata_principale": tm,
                    "tipo_testata": _deduce_tipo(tm),
                }).execute()
                inserted += 1
            except Exception:
                pass
        return {"success": True, "inserted": inserted}
    except Exception as e: return {"error": str(e)}


@router.get("/api/giornalista-articoli")
async def giornalista_articoli(nome: str = Query(...), period: str = Query("all"), limit: int = Query(200)):
    supabase = _sb()
    try:
        today = date.today()
        days_map = {"today": 0, "7days": 7, "30days": 30, "6months": 180, "year": 365, "all": None}
        days = days_map.get(period)
        # Recupera sigla dal CRM se esiste
        sigla = None
        try:
            crm = supabase.table("journalists").select("sigla").ilike("nome", nome).limit(1).execute()
            if crm.data and crm.data[0].get("sigla"):
                sigla = crm.data[0]["sigla"].strip()
        except Exception:
            pass

        arts = []
        for search_nome in ([nome, sigla] if sigla else [nome]):
            q = supabase.table("articles").select(
                "id, titolo, testata, data, giornalista, tone, dominant_topic, macrosettori"
            ).eq("giornalista", search_nome)
            if days is not None:
                fd = today.isoformat() if days == 0 else (today - timedelta(days=days)).isoformat()
                q = q.gte("data", fd)
            res = q.order("data", desc=True).limit(limit).execute().data or []
            arts.extend(res)

        # Deduplica per id e riordina per data
        seen = set()
        deduped = []
        for a in arts:
            if a["id"] not in seen:
                seen.add(a["id"])
                deduped.append(a)
        deduped.sort(key=lambda x: x.get("data",""), reverse=True)
        return deduped[:limit]
    except Exception: return []


@router.get("/api/top-giornalisti")
async def top_giornalisti(period: str = Query("30days"), limit: int = Query(20)):
    supabase = _sb()
    try:
        today = date.today()
        days = {"today": 0, "7days": 7, "30days": 30, "6months": 180, "year": 365}.get(period, 30)
        fd = today.isoformat() if days == 0 else (today - timedelta(days=days)).isoformat()
        res = supabase.table("articles").select("giornalista").gte("data", fd).lte("data", today.isoformat()).execute()
        SKIP = {"", "N.D.", "N/D", "Redazione", "Autore non indicato", "redazione"}
        c = Counter(a.get("giornalista","") for a in (res.data or []) if a.get("giornalista") and a["giornalista"] not in SKIP)
        return [{"nome": n, "articoli": v} for n, v in c.most_common(limit)]
    except Exception: return []


@router.get("/api/top-giornalisti-ave")
async def top_giornalisti_ave(period: str = Query("today"), limit: int = Query(15)):
    supabase = _sb()
    try:
        today = date.today()
        days = {"today": 0, "7days": 7, "30days": 30, "6months": 180}.get(period, 0)
        fd = today.isoformat() if days == 0 else (today - timedelta(days=days)).isoformat()
        SKIP = {"", "N.D.", "N/D", "Redazione", "Autore non indicato", "redazione"}
        res = supabase.table("articles").select("giornalista, testata, titolo, ave").gte("data", fd).lte("data", today.isoformat()).execute()
        best = {}
        for a in res.data or []:
            g = a.get("giornalista","")
            if not g or g in SKIP: continue
            ave = float(a.get("ave") or 0)
            if g not in best or ave > best[g]["ave"]:
                best[g] = {"nome": g, "testata": a.get("testata",""), "titolo": a.get("titolo",""), "ave": ave}
        return sorted(best.values(), key=lambda x: x["ave"], reverse=True)[:limit]
    except Exception: return []


@router.get("/api/journalists-bubble-data")
async def journalists_bubble_data(client_id: Optional[str] = None, macro_id: Optional[str] = None):
    supabase = _sb()
    try:
        SKIP = {"", "n.d.", "n/d", "redazione", "autore non indicato"}
        articles = supabase.table("articles").select("giornalista, testata, macrosettori, titolo, occhiello, testo_completo").execute().data or []
        client_art_ids = None
        if client_id:
            cl = supabase.table("clients").select("keywords_press").eq("id", client_id).execute()
            if cl.data:
                kws = [k.strip().lower() for k in (cl.data[0].get("keywords_press") or "").split(",") if k.strip()]
                if kws:
                    client_art_ids = {i for i, a in enumerate(articles)
                        if any(kw in f"{a.get('titolo','')} {a.get('occhiello','')} {a.get('testo_completo','')}".lower() for kw in kws)}
        mac_names = []
        if macro_id:
            lnk = supabase.table("macro_group_links").select("official_macro_id").eq("macro_group_id", macro_id).execute()
            oids = [l["official_macro_id"] for l in (lnk.data or [])]
            if oids:
                mac = supabase.table("official_macrosectors").select("name").in_("id", oids).execute()
                mac_names = [m["name"].upper() for m in (mac.data or [])]
        testata_data = {}
        for i, a in enumerate(articles):
            if client_art_ids is not None and i not in client_art_ids: continue
            if mac_names and not any(mn in (a.get("macrosettori") or "").upper() for mn in mac_names): continue
            t = (a.get("testata") or "").strip()
            g = (a.get("giornalista") or "").strip()
            if not t: continue
            if t not in testata_data: testata_data[t] = {"testata": t, "count": 0, "giornalisti": {}}
            testata_data[t]["count"] += 1
            if g and g.lower() not in SKIP:
                testata_data[t]["giornalisti"][g] = testata_data[t]["giornalisti"].get(g, 0) + 1
        nodes = sorted([{"testata": t, "count": d["count"],
            "giornalisti": sorted([{"nome": n, "count": c} for n, c in d["giornalisti"].items()], key=lambda x: -x["count"])}
            for t, d in testata_data.items()], key=lambda x: -x["count"])
        return {"nodes": nodes}
    except Exception as e: return {"error": str(e)}


@router.post("/api/journalists/add-article")
async def add_article_manual(data: ManualArticleModel):
    """Aggiunge un articolo manuale associato a un giornalista."""
    import hashlib
    supabase = _sb()
    try:
        art_date = data.data or date.today().isoformat()
        h = hashlib.md5(f"{data.giornalista}|{data.titolo}|{art_date}".encode()).hexdigest()
        res = supabase.table("articles").insert({
            "giornalista":        data.giornalista,
            "titolo":             data.titolo,
            "testata":            data.testata or "",
            "data":               art_date,
            "testo_completo":     data.testo_completo or "",
            "tone":               data.tone or "Neutral",
            "tipologia_articolo": data.tipologia_articolo or "",
            "macrosettori":       data.macrosettori or "",
            "ave":                data.ave,
            "content_hash":       h,
            "fonte":              "manuale",
        }).execute()
        return {"success": True, "article": res.data[0] if res.data else {}}
    except Exception as e:
        return {"error": str(e)}