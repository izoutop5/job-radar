import csv
import json
import os
import re
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Set, Tuple

import requests
import feedparser
import yaml
from rapidfuzz import fuzz

SEEN_FILE = "seen.json"


def load_config() -> dict:
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_seen() -> set:
    if not os.path.exists(SEEN_FILE):
        return set()
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()


def save_seen(seen: set) -> None:
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(list(seen)), f, ensure_ascii=False, indent=2)


def norm(s: str) -> str:
    return (s or "").strip().lower()


def strip_html(s: str) -> str:
    s = s or ""
    s = re.sub("<[^<]+?>", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def normalize_company_name(s: str) -> str:
    s = (s or "").lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\b(inc|ltd|llc|plc|gmbh|ag|sa|s a|s\.a|nv|bv|spa|pte|pte\.|co|company|corp|corporation|holdings|holding)\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def http_get_text(url: str, params: Optional[dict] = None, timeout: int = 45) -> Optional[str]:
    try:
        headers = {"User-Agent": "job-radar/2.0"}
        r = requests.get(url, headers=headers, params=params, timeout=timeout)
        if r.status_code >= 400:
            return None
        return r.text
    except requests.RequestException:
        return None


def get_json(url: str, params: Optional[dict] = None) -> Optional[Any]:
    txt = http_get_text(url, params=params)
    if not txt:
        return None
    try:
        return json.loads(txt)
    except Exception:
        return None


# -----------------------
# Company universe (Top 1000 global + Top 100 Brazil) via CompaniesMarketCap CSV
# -----------------------
def parse_companiesmarketcap_csv(csv_text: str) -> List[str]:
    csv_text = csv_text.strip()
    if not csv_text:
        return []
    # Garante que parece CSV do site
    if "Name" not in csv_text.splitlines()[0]:
        return []
    out = []
    reader = csv.DictReader(csv_text.splitlines())
    for row in reader:
        name = (row.get("Name") or "").strip()
        if name:
            out.append(name)
    return out


def fetch_companies_from_templates(templates: List[str], pages: int = 1) -> List[str]:
    names: List[str] = []
    for page in range(1, pages + 1):
        page_ok = False
        for tpl in templates:
            url = tpl.format(page=page)
            txt = http_get_text(url)
            if not txt:
                continue
            parsed = parse_companiesmarketcap_csv(txt)
            if parsed:
                names.extend(parsed)
                page_ok = True
                break
        if not page_ok:
            # Se uma página falha, seguimos mesmo assim (não derruba o job)
            continue
    return names


def load_company_universe(cfg: dict) -> Tuple[Set[str], Set[str], Set[str]]:
    cu = cfg.get("company_universe") or {}
    if not cu.get("enabled", True):
        return set(), set(), set()

    top_global = int(cu.get("global_top_n", 1000))
    top_brazil = int(cu.get("brazil_top_n", 100))
    cmc = (cu.get("companiesmarketcap") or {})
    global_tpls = cmc.get("global_url_templates") or []
    brazil_tpls = cmc.get("brazil_url_templates") or []
    extra = cu.get("extra_companies") or []

    # Global: normalmente 100 por página no CSV -> 10 páginas = ~1000
    global_pages = max(1, (top_global + 99) // 100)
    global_names = fetch_companies_from_templates(global_tpls, pages=global_pages)[:top_global]

    # Brasil: uma única página CSV costuma trazer todas; se não, pega o que vier
    brazil_names = fetch_companies_from_templates(brazil_tpls, pages=1)[:top_brazil]

    # Normaliza
    global_set = {normalize_company_name(x) for x in global_names if x}
    brazil_set = {normalize_company_name(x) for x in brazil_names if x}
    extra_set = {normalize_company_name(x) for x in extra if x}

    return global_set, brazil_set, extra_set


# -----------------------
# Fetchers: Adzuna (broad coverage)
# -----------------------
def fetch_adzuna(cfg: dict) -> List[Dict[str, Any]]:
    src = (cfg.get("sources") or {}).get("adzuna") or {}
    queries = src.get("queries") or []
    if not queries:
        return []

    app_id = os.environ.get("ADZUNA_APP_ID")
    app_key = os.environ.get("ADZUNA_APP_KEY")
    if not app_id or not app_key:
        return []

    pages_per_query = int(src.get("pages_per_query", 1))
    results_per_page = int(src.get("results_per_page", 50))

    out = []
    for q in queries:
        country = norm(q.get("country", "br"))
        what = q.get("what", "")
        where = q.get("where", "")

        for page in range(1, pages_per_query + 1):
            url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
            params = {
                "app_id": app_id,
                "app_key": app_key,
                "results_per_page": results_per_page,
                "what": what,
                "content-type": "application/json",
            }
            if where:
                params["where"] = where

            data = get_json(url, params=params)
            if not data:
                continue

            for j in data.get("results", []) or []:
                company = (j.get("company") or {}).get("display_name", "") if isinstance(j.get("company"), dict) else ""
                location = (j.get("location") or {}).get("display_name", "") if isinstance(j.get("location"), dict) else ""
                out.append({
                    "title": j.get("title", "") or "",
                    "company": company or "",
                    "location": location or "",
                    "apply_url": j.get("redirect_url", "") or "",
                    "description": j.get("description", "") or "",
                    "source": f"adzuna:{country}",
                    "date_posted": j.get("created", "") or "",
                })

    return out


# -----------------------
# Remote sources
# -----------------------
def fetch_remotive(urls: List[str]) -> List[Dict[str, Any]]:
    out = []
    for url in urls:
        data = get_json(url)
        if not data:
            continue
        for j in data.get("jobs", []) or []:
            out.append({
                "title": j.get("title", "") or "",
                "company": j.get("company_name", "") or "",
                "location": j.get("candidate_required_location", "") or "",
                "apply_url": j.get("url", "") or "",
                "description": j.get("description", "") or "",
                "source": "remotive",
                "date_posted": j.get("publication_date", "") or "",
            })
    return out


def fetch_remoteok(urls: List[str]) -> List[Dict[str, Any]]:
    out = []
    for url in urls:
        data = get_json(url)
        if not data or not isinstance(data, list):
            continue
        jobs = [x for x in data if isinstance(x, dict) and x.get("id")]
        for j in jobs:
            out.append({
                "title": j.get("position", "") or "",
                "company": j.get("company", "") or "",
                "location": j.get("location", "") or "",
                "apply_url": j.get("url", "") or "",
                "description": j.get("description", "") or "",
                "source": "remoteok",
                "date_posted": j.get("date", "") or "",
            })
    return out


def fetch_wwr_rss(urls: List[str]) -> List[Dict[str, Any]]:
    out = []
    for url in urls:
        feed = feedparser.parse(url)
        for e in getattr(feed, "entries", []) or []:
            out.append({
                "title": e.get("title", "") or "",
                "company": "",
                "location": "",
                "apply_url": e.get("link", "") or "",
                "description": e.get("summary", "") or "",
                "source": "weworkremotely",
                "date_posted": e.get("published", "") or "",
            })
    return out


# -----------------------
# GH / Lever
# -----------------------
def fetch_greenhouse(boards: List[str]) -> List[Dict[str, Any]]:
    out = []
    for b in boards:
        b = (b or "").strip()
        if not b:
            continue
        url = f"https://boards-api.greenhouse.io/v1/boards/{b}/jobs?content=true"
        data = get_json(url)
        if not data:
            continue
        for j in data.get("jobs", []) or []:
            loc = ""
            if isinstance(j.get("location"), dict):
                loc = j["location"].get("name", "") or ""
            out.append({
                "title": j.get("title", "") or "",
                "company": b,
                "location": loc,
                "apply_url": j.get("absolute_url", "") or "",
                "description": j.get("content", "") or "",
                "source": f"greenhouse:{b}",
                "date_posted": j.get("updated_at", "") or j.get("created_at", "") or "",
            })
    return out


def fetch_lever(companies: List[str]) -> List[Dict[str, Any]]:
    out = []
    for c in companies:
        c = (c or "").strip()
        if not c:
            continue
        url = f"https://api.lever.co/v0/postings/{c}?mode=json"
        data = get_json(url)
        if not data or not isinstance(data, list):
            continue
        for j in data:
            categories = j.get("categories", {}) or {}
            loc = categories.get("location", "") or ""
            out.append({
                "title": j.get("text", "") or "",
                "company": c,
                "location": loc,
                "apply_url": j.get("hostedUrl", "") or j.get("applyUrl", "") or "",
                "description": j.get("description", "") or "",
                "source": f"lever:{c}",
                "date_posted": str(j.get("createdAt", "") or ""),
            })
    return out


# -----------------------
# Filtering / scoring
# -----------------------
def dedupe(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for it in items:
        url = (it.get("apply_url") or "").strip()
        if not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(it)
    return out


def is_brazil_job(location: str, cfg: dict) -> bool:
    loc = norm(location)
    return any(norm(k) in loc for k in cfg.get("brazil_location_keywords", []) or [])


def is_remote_job(job: Dict[str, Any], cfg: dict) -> bool:
    src = (job.get("source") or "").lower()
    if src in {"remotive", "remoteok", "weworkremotely"}:
        return True

    text = " ".join([
        norm(job.get("location", "")),
        norm(job.get("title", "")),
        norm(strip_html(job.get("description", ""))),
    ])
    return any(norm(k) in text for k in cfg.get("remote_keywords", []) or [])


def should_exclude_title(title: str, cfg: dict) -> bool:
    t = norm(title)
    return any(norm(b) in t for b in cfg.get("exclude_title_keywords", []) or [])


def must_be_finance_domain(title: str, cfg: dict) -> bool:
    t = norm(title)
    if "cfo" in t or "chief financial officer" in t:
        return True
    must = cfg.get("must_contain_any_of", []) or []
    return any(norm(k) in t for k in must)


def title_match_score(title: str, cfg: dict) -> int:
    t = norm(title)

    for k in cfg.get("target_title_keywords", []) or []:
        kk = norm(k)
        if kk and kk in t:
            return 78

    best = 0
    for k in cfg.get("target_title_keywords", []) or []:
        kk = norm(k)
        if kk:
            best = max(best, fuzz.partial_ratio(kk, t))

    if best >= 92:
        return 68
    if best >= 88:
        return 58
    if best >= 84:
        return 48
    return 0


def company_bonus(company: str, br: bool, global_set: Set[str], brazil_set: Set[str], extra_set: Set[str], cfg: dict) -> int:
    cu = cfg.get("company_universe") or {}
    if not cu.get("enabled", True):
        return 0

    c = normalize_company_name(company)
    bonus = 0

    if c in extra_set:
        bonus += int(cu.get("bonus_extra", 14))

    if c in global_set:
        bonus += int(cu.get("bonus_global", 10))

    if br and c in brazil_set:
        bonus += int(cu.get("bonus_brazil", 8))

    return bonus


def score_job(job: Dict[str, Any], cfg: dict, global_set: Set[str], brazil_set: Set[str], extra_set: Set[str]) -> int:
    title = job.get("title", "") or ""
    desc = strip_html(job.get("description", "") or "")
    loc = job.get("location", "") or ""
    company = job.get("company", "") or ""

    if should_exclude_title(title, cfg):
        return 0
    if not must_be_finance_domain(title, cfg):
        return 0

    br = is_brazil_job(loc, cfg)
    remote = is_remote_job(job, cfg)

    if cfg.get("require_remote_outside_brazil", True) and (not br) and (not remote):
        return 0

    score = title_match_score(title, cfg)
    if score == 0:
        return 0

    d = norm(desc)
    kw_bonus = 0
    for k in cfg.get("nice_keywords_desc", []) or []:
        if norm(k) in d:
            kw_bonus += 3
    score += min(kw_bonus, 24)

    score += company_bonus(company, br, global_set, brazil_set, extra_set, cfg)

    if br:
        score += 6
    if remote:
        score += 6

    return max(0, min(score, 100))


def format_message(new_jobs: List[Dict[str, Any]]) -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not new_jobs:
        return f"🛰️ Job Radar ({today}): nada novo acima do filtro hoje."

    lines = [f"🛰️ Job Radar ({today}) — {len(new_jobs)} vaga(s) nova(s):\n"]
    for j in new_jobs:
        score = j.get("score", 0)
        title = (j.get("title") or "").strip()
        company = (j.get("company") or "").strip()
        loc = (j.get("location") or "").strip()
        url = (j.get("apply_url") or "").strip()
        src = (j.get("source") or "").strip()

        header = f"• [{score}] {title}"
        if company:
            header += f" — {company}"
        if loc:
            header += f" ({loc})"
        if src:
            header += f" [{src}]"

        lines.append(header)
        lines.append(url)
        lines.append("")

    return "\n".join(lines).strip()


def send_telegram(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("Faltam TELEGRAM_BOT_TOKEN e/ou TELEGRAM_CHAT_ID nos Secrets do GitHub.")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}

    r = requests.post(url, json=payload, timeout=45)
    if r.status_code >= 400:
        raise RuntimeError(f"Telegram erro {r.status_code}: {r.text}")


def main():
    cfg = load_config()
    seen = load_seen()

    # Carrega universo de empresas
    global_set, brazil_set, extra_set = load_company_universe(cfg)

    # Coleta vagas
    sources = cfg.get("sources") or {}
    jobs: List[Dict[str, Any]] = []

    jobs.extend(fetch_adzuna(cfg))

    jobs.extend(fetch_remotive(sources.get("remotive", []) or []))
    jobs.extend(fetch_remoteok(sources.get("remoteok", []) or []))
    jobs.extend(fetch_wwr_rss(sources.get("weworkremotely_rss", []) or []))

    watch = cfg.get("company_watchlist") or {}
    jobs.extend(fetch_greenhouse(watch.get("greenhouse_boards", []) or []))
    jobs.extend(fetch_lever(watch.get("lever_companies", []) or []))

    jobs = dedupe(jobs)

    scored = []
    for j in jobs:
        s = score_job(j, cfg, global_set, brazil_set, extra_set)
        if s <= 0:
            continue
        j["score"] = s
        scored.append(j)

    scored.sort(key=lambda x: x.get("score", 0), reverse=True)

    min_score = int(cfg.get("min_score_to_send", 74))
    max_items = int(cfg.get("max_items_per_day", 20))

    new_jobs = []
    for j in scored:
        url = (j.get("apply_url") or "").strip()
        if not url or url in seen:
            continue
        if j.get("score", 0) < min_score:
            continue
        new_jobs.append(j)
        if len(new_jobs) >= max_items:
            break

    msg = format_message(new_jobs)
    send_telegram(msg)

    for j in new_jobs:
        url = (j.get("apply_url") or "").strip()
        if url:
            seen.add(url)
    save_seen(seen)

    print(f"Fetched: {len(jobs)} | Scored: {len(scored)} | Sent: {len(new_jobs)}")
    print(f"Company universe loaded: global={len(global_set)} brazil={len(brazil_set)} extra={len(extra_set)}")


if __name__ == "__main__":
    main()
