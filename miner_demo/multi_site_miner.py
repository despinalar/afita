from __future__ import annotations
 
import csv
import json
import time
import random
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
 
import requests
from bs4 import BeautifulSoup
 
 
# ----------------------------
# Configuration
# ----------------------------
 
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MultiSiteMiner/1.0; +https://example.com/bot)",
    "Accept-Language": "en-US,en;q=0.9",
}
 
@dataclass
class Target:
    name: str
    mode: str  # "html" or "json"
    start_urls: List[str]
    parser: Callable[[requests.Response, str], List[Dict[str, Any]]]
    # Optional: pagination / discovery
    discover_next_urls: Optional[Callable[[requests.Response, str], List[str]]] = None
 
 
# ----------------------------
# HTTP client with retries + rate limit
# ----------------------------
 
class Fetcher:
    def __init__(
        self,
        session: Optional[requests.Session] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
        min_delay: float = 0.75,
        max_delay: float = 1.75,
        max_retries: int = 4,
        backoff_factor: float = 1.7,
    ):
        self.session = session or requests.Session()
        self.session.headers.update(headers or DEFAULT_HEADERS)
        self.timeout = timeout
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
 
    def _sleep_jitter(self):
        time.sleep(random.uniform(self.min_delay, self.max_delay))
 
    def get(self, url: str) -> requests.Response:
        delay = 0.0
        for attempt in range(1, self.max_retries + 1):
            if delay:
                time.sleep(delay)
            self._sleep_jitter()
 
            try:
                resp = self.session.get(url, timeout=self.timeout)
                # Retry on transient server errors or rate limiting
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"Transient HTTP {resp.status_code}", response=resp)
                resp.raise_for_status()
                return resp
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
                if attempt == self.max_retries:
                    raise
                delay = max(0.5, self.backoff_factor ** (attempt - 1))
        raise RuntimeError("Unreachable")
 
 
# ----------------------------
# Optional robots.txt check
# ----------------------------
 
def robots_allows(url: str, user_agent: str = "") -> bool:
    """
    Minimal robots.txt check.
    This is intentionally simple. For more complete parsing, use:
      pip install robotexclusionrulesparser
    """
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    try:
        r = requests.get(robots_url, headers=DEFAULT_HEADERS, timeout=10)
        if r.status_code != 200:
            return True  # if robots is missing/unreachable, default to allow (policy-dependent)
        lines = r.text.splitlines()
        ua_block = False
        disallows: List[str] = []
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("user-agent:"):
                ua = line.split(":", 1)[1].strip()
                ua_block = (ua == "" or ua.lower() == user_agent.lower())
            elif ua_block and line.lower().startswith("disallow:"):
                path = line.split(":", 1)[1].strip()
                disallows.append(path)
 
        path = parsed.path or "/"
        for d in disallows:
            if d == "":
                continue
            if path.startswith(d):
                return False
        return True
    except Exception:
        return True
 
 
# ----------------------------
# Parsers (examples)
# ----------------------------
 
def parse_example_html(resp: requests.Response, base_url: str) -> List[Dict[str, Any]]:
    """
    Example HTML parser:
    Extracts items from a list page where each item has:
      - title in <h2 class="title">
      - link in <a class="detail-link">
      - price in <span class="price">
    Change selectors to match your target site.
    """
    soup = BeautifulSoup(resp.text, "lxml")
    results: List[Dict[str, Any]] = []
 
    for card in soup.select(".card, .item, article"):
        title_el = card.select_one("h2.title, h2, .title")
        link_el = card.select_one("a.detail-link, a")
        price_el = card.select_one(".price, .amount")
 
        title = title_el.get_text(strip=True) if title_el else None
        href = urljoin(base_url, link_el["href"]) if link_el and link_el.get("href") else None
        price = price_el.get_text(strip=True) if price_el else None
 
        if title or href or price:
            results.append({
                "title": title,
                "url": href,
                "price": price,
                "source": base_url,
            })
 
    return results
 
 
def discover_next_example_html(resp: requests.Response, base_url: str) -> List[str]:
    """
    Example next-page discovery (pagination):
    Looks for a "next" link.
    """
    soup = BeautifulSoup(resp.text, "lxml")
    next_link = soup.select_one("a[rel='next'], a.next, .pagination a.next")
    if next_link and next_link.get("href"):
        return [urljoin(base_url, next_link["href"])]
    return []
 
 
def parse_example_json(resp: requests.Response, base_url: str) -> List[Dict[str, Any]]:
    """
    Example JSON parser:
    Expects something like:
      {"items": [{"id":..., "name":..., "value":...}, ...]}
    Adjust keys to match your API.
    """
    data = resp.json()
    items = data.get("items", data if isinstance(data, list) else [])
    results: List[Dict[str, Any]] = []
    for it in items:
        results.append({
            "id": it.get("id"),
            "name": it.get("name"),
            "value": it.get("value"),
            "source": base_url,
        })
    return results
 
 
# ----------------------------
# Orchestrator
# ----------------------------
 
def mine_target(
    fetcher: Fetcher,
    target: Target,
    max_pages: int = 10,
    respect_robots: bool = True,
) -> List[Dict[str, Any]]:
    """
    Mine data for a given target.
    """
    collected: List[Dict[str, Any]] = []
    seen_urls = set()
    queue = list(target.start_urls)
 
    while queue and len(seen_urls) < max_pages:
        url = queue.pop(0)
        if url in seen_urls:
            continue
        seen_urls.add(url)
 
        if respect_robots and not robots_allows(url):
            print(f"[{target.name}] Skipping (robots): {url}")
            continue
 
        print(f"[{target.name}] Fetching: {url}")
        resp = fetcher.get(url)
 
        base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        items = target.parser(resp, base_url)
        collected.extend(items)
 
        if target.discover_next_urls:
            next_urls = target.discover_next_urls(resp, base_url)
            for nu in next_urls:
                if nu not in seen_urls:
                    queue.append(nu)
 
    return collected
 
 
def write_json(path: str, rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
 
 
def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    # union of all keys
    keys = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)
 
 
def main():
    # Define the targets you want to mine.
    # Replace start_urls and CSS selectors / JSON keys in parsers.
    targets: List[Target] = [
        Target(
            name="ExampleHTMLSite",
            mode="html",
            start_urls=["https://www.tesla.com"],
            parser=parse_example_html,
            discover_next_urls=discover_next_example_html,
        ),
        Target(
            name="ExampleJSONAPI",
            mode="json",
            start_urls=["https://example.com/api/items?page=1"],
            parser=parse_example_json,
            discover_next_urls=None,  # or implement pagination discovery for APIs
        ),
    ]
 
    fetcher = Fetcher(
        headers=DEFAULT_HEADERS,
        min_delay=0.8,
        max_delay=2.0,
        max_retries=4,
    )
 
    all_rows: List[Dict[str, Any]] = []
    for t in targets:
        rows = mine_target(fetcher, t, max_pages=10, respect_robots=True)
        all_rows.extend(rows)
 
    # Output
    write_json("mined_data.json", all_rows)
    write_csv("mined_data.csv", all_rows)
    print(f"Done. Collected {len(all_rows)} records -> mined_data.json / mined_data.csv")
 
 
if __name__ == "__main__":
    main()