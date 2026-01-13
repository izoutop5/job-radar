import json
import os
import re
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import requests
import feedparser
import yaml
from rapidfuzz import fuzz

SEEN_FILE = "seen.json"

REMOTE_SOURCE_PREFIXES = ("remotive", "remoteok", "weworkremotely")


def load_config() -> dict:
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_seen() -> set:
    if not os.path.exists(SEEN_FILE):
        return set()
    with open(SEEN_FILE, "r", encoding="utf-8") as f:
        return set(json.load(f))


def save_seen(seen: set) -> None:
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(list(seen)), f, ensure_ascii=False, indent=2)


def norm(s: str) -> str:
    return (s or "").strip().lower()


def strip_html(s: str) -> str:
    s = s or ""
    s = re.sub("<[^<]+?>", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def http_get(url: str, params: Optional[dict] = None) -> Optional[requests.Response]:
    try:
        headers = {"User-Agent": "job-radar/1.2"}
        r = requests.get(url, headers=headers, params=params, timeout=45)
        # Não quebrar o job por 404/429/5xx — só ignora e segue
        if r.status_code >= 400:
            return None
        return r
    except requests.RequestException:
        return None


def get_json(url: str, params: Optional[dict] = None) -> Optional[Any]:
    r = http_get(url, params=params)
    if not r:
        return None
    try:
        return r.json()
    except Exception:
        return None


# -----------------------
# Fetchers: Remote boards
# -----------------------
def fetch_remotive(urls: List[str]) -> List[Dict[str, Any]]:
    out = []
    for url in urls:
        data = get_json(url)
        if not data:
            continue
        for j in data.get("jobs", []):
            out.append({
                "title": j.get("title", ""),
                "company": j.get("company_name", ""),
                "location": j.get("candidate_required_location", ""),
                "apply_url": j.get("url", ""),
                "description": j.get("description", ""),
                "source": "remotive",
                "date_posted": j.get("publication_date", ""),
            })
    return out


def fetch_remoteok(urls: List[str]) -> List[Dict[str, Any]]:
    out = []
    for url in urls:
        data = get_json(url)
        if not data:
            continue
        jobs = [x for x in data if isinstance(x, dict) and "id" in x]
        for j in jobs:
            out.append({
                "title": j.get("position", ""),
                "company": j.get("company", ""),
                "location": j.get("location", ""),
                "apply_url": j.get("url", ""),
                "description": j.get("description", ""),
                "source": "remoteok",
                "date_posted": j.get("date", ""),
            })
    return out


def fetch_wwr_rss(urls: List[str]) -> List[Dict[str, Any]]:
    out = []
    for url in urls:
        feed = feedparser.parse(url)
        for e in getattr(feed, "entries", []) or []:
            out.append({
                "title": e.get("title", ""),
                "company": "",
                "location": "",
                "apply_url": e.get("link", ""),
                "description": e.get("summary", ""),
                "source": "weworkremotely",
                "date_posted": e.get("published", ""),
            })
    return out


# -----------------------
# Fetchers: Company ATS
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
        for j in data.get("jobs", []):
            loc = ""
            if isinstance(j.get("location"), dict):
                loc = j["location"].get("name", "")
            out.append({
                "title": j.get("title", ""),
                "company": b,
                "location": loc,
                "apply_url": j.get("absolute_url", ""),
                "description": j.get("content", "") or "",
                "source": f"greenhouse:{b}",
                "date_posted": j.get("updated_at", "") or j.get("created_at", ""),
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
            desc = j.get("description", "") or ""
            out.append({
                "title": j.get("text", ""),
                "company": c,
                "location": loc,
                "apply_url": j.get("hostedUrl", "") or j.get("applyUrl", "") or "",
                "description": desc,
                "source": f"lever:{c}",
                "date_posted": j.get("createdAt", ""),
            })
    return out


# -----------------------
# Core logic
# -----------------------
def dedupe(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen_urls = set()
    out = []
    for it in items:
        url = (it.get("apply_url") or "").strip()
        if not url:
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)
        out.append(it)
    return out


def is_brazil_job(location: str, cfg: dict) -> bool:
    loc = norm(location)
    for k in cfg.get("brazil_location_keywords", []):
        if k in loc:
            return True
    return False


def is_remote_job(job: Dict[str, Any], cfg: dict) -> bool:
    # Se vem de boards remotos, consideramos remoto por default
    src = (job.get("source") or "").lower()
    if src.startswith(REMOTE_SOURCE_PREFIXES):
        return True

    text = " ".join([
        norm(job.get("location", "")),
        norm(job.get("title", "")),
        norm(strip_html(job.get("description", ""))),
    ])

    for k in cfg.get("remote_keywords", []):
        if norm(k) in text:
            return True
    return False


def should_exclude_title(title: str, cfg: dict) -> bool:
    t = norm(title)
    for b in cfg.get("exclude_title_keywords", []):
        if norm(b) in t:
            return True
    return False


def must_be_finance_domain(title: str, cfg: dict) -> bool:
    t = norm(title)
    # CFO sempre entra
    if "cfo" in t or "chief financial officer" in t:
        return True

    must = cfg.get("must_contain_any_of", [])
    if not must:
        return True

    return any(norm(k) in t for k in must)


def title_match_score(title: str, cfg: dict) -> int:
    t = norm(title)
    best = 0

    # Match direto (rápido e forte)
    for k in cfg.get("target_title_keywords", []):
        kk = norm(k)
        if kk and kk in t:
            return 75

    # Fuzzy fallback
    for k in cfg.get("target_title_keywords", []):
        kk = norm(k)
        if not kk:
            continue
        best = max(best, fuzz.partial_ratio(kk, t))

    if best >= 90:
        return 65
    if best >= 85:
        return 55
    if best >= 80:
        return 45
    return 0


def score_job(job: Dict[str, Any], cfg: dict) -> int:
    title = job.get("title", "") or ""
    desc = strip_html(job.get("description", "") or "")
    loc = job.get("location", "") or ""

    if should_exclude_title(title, cfg):
        return 0

    # Anti-ruído: Director/Head/VP/SVP só se for área certa
    if not must_be_finance_domain(title, cfg):
        return 0

    br = is_brazil_job(loc, cfg)
    remote = is_remote_job(job, cfg)

    if cfg.get("require_remote_outside_brazil", True) and (not br) and (not remote):
        return 0

    # Precisa parecer cargo-alvo
    score = title_match_score(title, cfg)
    if score == 0:
        return 0

    d = norm(desc)
    bonus = 0
    for k in cfg.get("nice_keywords_desc", []):
        if norm(k) in d:
            bonus += 3
    score += min(bonus, 25)

    if br:
        score += 6
    if remote:
        score += 6

    return min(max(score, 0), 100)


def format_message(new_jobs: List[Dict[str, Any]], cfg: dict) -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not new_jobs:
        return f"🛰️ Job Radar ({today}): nada novo acima do seu filtro hoje."

    lines = [f"🛰️ Job Radar ({today}) — {len(new_jobs)} vaga(s) nova(s):\n"]
    for j in new_jobs[: cfg.get("max_items_per_day", 12)]:
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
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()


def main():
    cfg = load_config()
    seen = load_seen()

    items: List[Dict[str, Any]] = []

    src = cfg.get("sources", {})
    items += fetch_remotive(src.get("remotive", []))
    items += fetch_remoteok(src.get("remoteok", []))
    items += fetch_wwr_rss(src.get("weworkremotely_rss", []))

    watch = cfg.get("company_watchlist", {})
    items += fetch_greenhouse(watch.get("greenhouse_boards", []))
    items += fetch_lever(watch.get("lever_companies", []))

    items = dedupe(items)

    fresh = [j for j in items if j.get("apply_url") and j["apply_url"] not in seen]

    scored = []
    for j in fresh:
        s = score_job(j, cfg)
        j["score"] = s
        if s >= cfg.get("min_score_to_send", 72):
            scored.append(j)

    scored.sort(key=lambda x: x["score"], reverse=True)

    send_telegram(format_message(scored, cfg))

    for j in items:
        url = (j.get("apply_url") or "").strip()
        if url:
            seen.add(url)
    save_seen(seen)


if __name__ == "__main__":
    main()

