import hashlib, re, sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import feedparser, requests, yaml
from dateutil import parser as dateparser
from lxml import etree

OUTPUT_FILE = "feed.xml"
UA = {"User-Agent": "Onet-Podcast-Aggregator/1.0 (+github actions)"}

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
    for k in ("published", "updated", "created"):
        if e.get(k):
            try: return dateparser.parse(e[k])
            except: pass
    for k in ("published_parsed", "updated_parsed"):
        if e.get(k):
            return datetime(*e[k][:6], tzinfo=timezone.utc)
    return None

def first_html(e):
    # Preferuj content[0].value, potem summary, potem description
    if e.get("content") and len(e["content"]) and e["content"][0].get("value"):
        return e["content"][0]["value"]
    if e.get("summary"):
        return e["summary"]
    if e.get("description"):
        return e["description"]
    return ""

def text_haystack(e):
    parts = []
    if e.get("title"): parts.append(str(e["title"]))
    if e.get("summary"): parts.append(str(e["summary"]))
    if e.get("description"): parts.append(str(e["description"]))
    # content as text (strip tags crudely)
    if e.get("content") and len(e["content"]) and e["content"][0].get("value"):
        parts.append(re.sub("<[^>]+>", " ", e["content"][0]["value"]))
    return " ".join(parts)

def matches(e, terms):
    hay = text_haystack(e).lower()
    return any(t.lower() in hay for t in terms)

def pick_link(e):
    # RSS: e.link w zasadzie OK
    if e.get("link"):
        return e["link"]
    # Atom: wybierz rel=alternate, type text/html
    for L in e.get("links", []):
        if L.get("rel") == "alternate" and ("text/html" in (L.get("type") or "") or not L.get("type")):
            return L.get("href")
    # fallback: pierwszy link
    if e.get("links"):
        return e["links"][0].get("href")
    return ""

def normalize_protocol(url):
    # //ocdn.eu/... -> https://ocdn.eu/...
    if url and url.startswith("//"):
        return "https:" + url
    return url

def pick_image_enclosure(e):
    # Szukamy image enclosure w RSS lub Atom (rel=enclosure, type=image/*)
    # 1) feedparser: e.enclosures
    for en in e.get("enclosures", []):
        typ = (en.get("type") or "").lower()
        if typ.startswith("image/"):
            return normalize_protocol(en.get("href") or en.get("url"))
    # 2) Atom links
    for L in e.get("links", []):
        if L.get("rel") == "enclosure":
            typ = (L.get("type") or "").lower()
            if typ.startswith("image/"):
                return normalize_protocol(L.get("href"))
    return None

def guid_for(e):
    # Preferuj id/guid; fallback hash(link+title)
    for k in ("id", "guid"):
        if e.get(k):
            return str(e[k])
    base = (e.get("link") or "") + "||" + (e.get("title") or "")
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def main():
    cfg = load_config()
    window_days = int(cfg.get("window_days", 7))
    max_items = int(cfg.get("max_items", 400))
    ch = cfg.get("channel", {})
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)

    collected, seen = [], set()

    for src in cfg["sources"]:
        url = src["url"]
        label = src["label"]
        terms = src.get("match", [])
        take_img = bool(src.get("take_image_enclosure", False))

        try:
            feed = parse_feed(url)
        except Exception as e:
            print(f"[WARN] Nie pobrano {url}: {e}", file=sys.stderr)
            continue

        for e in feed.entries:
            if not matches(e, terms):
                continue

            dt = pick_date(e) or datetime(1970,1,1,tzinfo=timezone.utc)
            if dt < cutoff:
                continue

            g = guid_for(e)
            if g in seen: 
                continue
            seen.add(g)

            html = first_html(e)
            link = pick_link(e)
            img = pick_image_enclosure(e) if take_img else None

            collected.append({
                "guid": g,
                "title": e.get("title") or "",
                "link": link or "",
                "html": html or "",
                "pubDate": dt,
                "category": label,
                "image": img,
            })

    collected.sort(key=lambda x: x["pubDate"], reverse=True)
    if max_items:
        collected = collected[:max_items]

    # Budowa RSS 2.0
    rss = etree.Element("rss", version="2.0")
    channel = etree.SubElement(rss, "channel")
    etree.SubElement(channel, "title").text = ch.get("title", "Agregat podcastów (7 dni)")
    etree.SubElement(channel, "link").text = ch.get("link", "https://example.com")
    etree.SubElement(channel, "description").text = ch.get("description", "Zbiorczy RSS z filtracją po nazwach podcastów.")
    etree.SubElement(channel, "language").text = ch.get("language", "pl")
    etree.SubElement(channel, "lastBuildDate").text = rfc822(datetime.now(timezone.utc))

    for it in collected:
        node = etree.SubElement(channel, "item")
        etree.SubElement(node, "guid").text = it["guid"]
        etree.SubElement(node, "title").text = it["title"]
        if it["link"]:
            etree.SubElement(node, "link").text = it["link"]
        d = etree.SubElement(node, "description")
        d.text = etree.CDATA(it["html"])
        etree.SubElement(node, "pubDate").text = rfc822(it["pubDate"])
        etree.SubElement(node, "category").text = it["category"]

        # tylko jeśli mamy obrazek z kultury (image enclosure)
        if it["image"]:
            etree.SubElement(node, "enclosure", url=it["image"], type="image/jpeg")

    tree = etree.ElementTree(rss)
    tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)
    print(f"OK: zapisano {OUTPUT_FILE} ({len(collected)} pozycji).")

if __name__ == "__main__":
    main()
