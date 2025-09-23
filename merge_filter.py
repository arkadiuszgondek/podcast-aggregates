import hashlib, re, sys
from datetime import datetime, timedelta, timezone

import feedparser, requests, yaml
from dateutil import parser as dateparser
from lxml import etree

OUTPUT_FILE = "feed.xml"
UA = {"User-Agent": "Onet-Podcast-Aggregator/1.5 (+github actions)"}

def load_config():
    with open("feeds.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def fetch(url):
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    return r.content

def parse_feed(url):
    return feedparser.parse(fetch(url))

def rfc822(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")

def pick_date(e):
    # najpierw pola tekstowe ISO, potem *_parsed
    for k in ("published", "updated", "created"):
        if e.get(k):
            try:
                return dateparser.parse(e[k])
            except Exception:
                pass
    for k in ("published_parsed", "updated_parsed", "created_parsed"):
        if e.get(k):
            return datetime(*e[k][:6], tzinfo=timezone.utc)
    return None

def first_html(e):
    """Zwraca opis HTML bez znaczników <img> (często wstrzykiwanych w newsach)."""
    html = ""
    if e.get("content") and len(e["content"]) and e["content"][0].get("value"):
        html = e["content"][0]["value"]
    elif e.get("summary"):
        html = e["summary"]
    elif e.get("description"):
        html = e["description"]
    # usuń wszystkie <img ...> (także samozamykające)
    html = re.sub(r"<img[^>]*>", "", html, flags=re.IGNORECASE)
    return html.strip()

def normalize_protocol(url: str) -> str:
    # //ocdn.eu/... -> https://ocdn.eu/...
    if url and url.startswith("//"):
        return "https:" + url
    return url

def normalize_url(url: str) -> str:
    return normalize_protocol(url) if url else url

def pick_link(e):
    # RSS: e.link zwykle OK
    if e.get("link"):
        return normalize_url(e["link"])
    # Atom: wybierz rel=alternate (prefer text/html)
    best = ""
    for L in e.get("links", []):
        rel = (L.get("rel") or "").lower()
        typ = (L.get("type") or "").lower()
        href = normalize_url(L.get("href"))
        if rel == "alternate" and ("text/html" in typ or not typ):
            return href
        if not best and rel == "alternate":
            best = href
    if best:
        return best
    if e.get("links"):
        return normalize_url(e["links"][0].get("href"))
    return ""

def pick_image_enclosure(e):
    for en in e.get("enclosures", []):
        typ = (en.get("type") or "").lower()
        if typ.startswith("image/"):
            return normalize_url(en.get("href") or en.get("url") or "")
    for L in e.get("links", []):
        if (L.get("rel") or "").lower() == "enclosure":
            typ = (L.get("type") or "").lower()
            if typ.startswith("image/"):
                return normalize_url(L.get("href") or "")
    return None

def normalize_guid_value(val: str) -> str:
    """Usuwa prefiks 'urn:uuid:' i zwraca czysty GUID; gdy pusty → ''."""
    if not val:
        return ""
    s = str(val).strip()
    s = re.sub(r"^\s*urn:uuid:\s*", "", s, flags=re.IGNORECASE)
    return s.strip()

def guid_for(e):
    # Preferuj id/guid; fallback hash(link+title) — wszystko po normalizacji
    for k in ("id", "guid"):
        if e.get(k):
            v = normalize_guid_value(e[k])
            if v:
                return v
    base = (e.get("link") or "") + "||" + (e.get("title") or "")
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def matches(e, terms, title_only=False):
    if title_only:
        hay = (e.get("title") or "").lower()
    else:
        parts = []
        if e.get("title"): parts.append(str(e["title"]))
        if e.get("summary"): parts.append(str(e["summary"]))
        if e.get("description"): parts.append(str(e["description"]))
        if e.get("content") and len(e["content"]) and e["content"][0].get("value"):
            parts.append(re.sub("<[^>]+>", " ", e["content"][0]["value"]))
        hay = " ".join(parts).lower()
    return any(t.lower() in hay for t in terms)

def main():
    cfg = load_config()
    window_days = int(cfg.get("window_days", 7))
    max_items = int(cfg.get("max_items", 400))
    ch = cfg.get("channel", {})
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)

    collected, seen = [], set()

    for src in cfg.get("sources", []):
        url = src["url"]
        label = src["label"]
        terms = src.get("match", [])
        take_img = bool(src.get("take_image_enclosure", False))
        title_only = bool(src.get("title_only", False))

        try:
            feed = parse_feed(url)
        except Exception as ex:
            print(f"[WARN] Nie pobrano {url}: {ex}", file=sys.stderr)
            continue

        for e in feed.entries:
            if not matches(e, terms, title_only=title_only):
                continue

            dt = pick_date(e) or datetime(1970, 1, 1, tzinfo=timezone.utc)
            if dt < cutoff:
                continue

            g = guid_for(e)
            if g in seen:
                continue
            seen.add(g)

            html = first_html(e) or ""
            link = pick_link(e) or ""
            img = pick_image_enclosure(e) if take_img else None

            collected.append({
                "guid": g,
                "title": e.get("title") or "",
                "link": link,
                "html": html,
                "pubDate": dt,
                "category": label,
                "image": img,
            })

    collected.sort(key=lambda x: x["pubDate"], reverse=True)
    if max_items:
        collected = collected[:max_items]

    # Budowa RSS 2.0 z bezpiecznymi namespaces (atom, content)
    rss = etree.Element(
        "rss",
        nsmap={
            "atom": "http://www.w3.org/2005/Atom",
            "content": "http://purl.org/rss/1.0/modules/content/",
        },
        version="2.0",
    )
    channel = etree.SubElement(rss, "channel")
    etree.SubElement(channel, "title").text = ch.get("title", "Agregat podcastów (7 dni)")
    etree.SubElement(channel, "link").text = ch.get("link", "https://example.com")
    etree.SubElement(channel, "description").text = ch.get("description", "Zbiorczy RSS z filtracją po nazwach podcastów.")
    etree.SubElement(channel, "language").text = ch.get("language", "pl")
    etree.SubElement(channel, "lastBuildDate").text = rfc822(datetime.now(timezone.utc))

    # Opcjonalny atom:link rel=self — podaj w feeds.yaml: channel.self_url
    self_url = ch.get("self_url")
    if self_url:
        etree.SubElement(
            channel,
            "{http://www.w3.org/2005/Atom}link",
            rel="self",
            type="application/rss+xml",
            href=self_url
        )

    for it in collected:
        node = etree.SubElement(channel, "item")
        guid_el = etree.SubElement(node, "guid")
        guid_el.text = it["guid"]
        guid_el.set("isPermaLink", "false")  # po normalizacji GUID nie traktujemy go jako URL
        etree.SubElement(node, "title").text = it["title"]
        if it["link"]:
            etree.SubElement(node, "link").text = it["link"]
        d = etree.SubElement(node, "description")
        d.text = etree.CDATA(it["html"])
        etree.SubElement(node, "pubDate").text = rfc822(it["pubDate"])
        etree.SubElement(node, "category").text = it["category"]
        if it["image"]:
            etree.SubElement(node, "enclosure", url=it["image"], type="image/jpeg")

    # --- ZAPIS: prolog z podwójnymi cudzysłowami (dla SM) ---
    xml_bytes = etree.tostring(
        rss,
        encoding="UTF-8",
        xml_declaration=True,
        pretty_print=False
    )
    xml_text = xml_bytes.decode("utf-8")
    if xml_text.startswith("<?xml"):
        xml_text = xml_text.replace("version='1.0'", 'version="1.0"', 1)
        xml_text = xml_text.replace("encoding='UTF-8'", 'encoding="UTF-8"', 1)

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="\n") as f:
        f.write(xml_text)

    # self-check (opcjonalne ostrzeżenie do logów Actions)
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        first_line = f.readline().strip()
    expected = '<?xml version="1.0" encoding="UTF-8"?>'
    if not first_line.startswith(expected):
        print("[WARN] Prolog XML nie wygląda idealnie:", first_line, file=sys.stderr)

    print(f"OK: zapisano {OUTPUT_FILE} ({len(collected)} pozycji).")

if __name__ == "__main__":
    main()
