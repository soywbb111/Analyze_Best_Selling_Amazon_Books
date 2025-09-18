import argparse
import csv
import datetime
import os
import random
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =================== CẤU HÌNH CƠ BẢN ===================
BASE = "https://www.amazon.com"
# Trang đầu hiện hành (Amazon có 2 kiểu route, bắt đầu từ route mới)
LIST_URL = "https://www.amazon.com/gp/bestsellers/books"

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
]

# =================== HTTP SESSION + RETRY ===================
def make_session():
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

SESSION = make_session()

def http_get(url, max_retries=3, backoff_base=2):
    """GET với headers + retry và nhận diện robot/captcha."""
    last_err = None
    for i in range(max_retries):
        headers = {
            "User-Agent": random.choice(UA_POOL),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": BASE,
            "Cache-Control": "no-cache",
        }
        # thêm nhiễu nhẹ để tránh cache
        noise = f"&_={int(time.time()*1000)}{random.randint(100,999)}"
        sep = "&" if "?" in url else "?"
        url_noised = url + sep + noise

        try:
            resp = SESSION.get(url_noised, headers=headers, timeout=30)
            tl = resp.text.lower()
            blocked = ("captcha" in tl
                       or "robot check" in tl
                       or "make sure you're not a robot" in tl)
            if resp.status_code == 200 and not blocked:
                return resp
            last_err = f"status={resp.status_code}, blocked={blocked}"
        except Exception as e:
            last_err = str(e)

        time.sleep(backoff_base + i * 2 + random.random() * 2.5)
    raise RuntimeError(f"Failed to fetch {url} ({last_err})")

# =================== TIỆN ÍCH PARSER ===================
def clean_int(text):
    if not text: return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None

def parse_price(text):
    if not text: return None
    text = text.replace(",", "")
    m = re.search(r"\$([\d\.]+)", text)
    return float(m.group(1)) if m else None

def to_year(date_text):
    if not date_text: return None
    s = date_text.strip()
    m = re.search(r"\((.*?)\)", s)
    if m: s = m.group(1).strip()
    for fmt in ("%B %d, %Y", "%B %Y", "%Y"):
        try:
            return datetime.datetime.strptime(s, fmt).year
        except ValueError:
            pass
    m2 = re.search(r"(19|20)\d{2}", s)
    return int(m2.group(0)) if m2 else None

# =================== PARSE TRANG DANH SÁCH ===================
def parse_list_page(html):
    """Lấy Name, Author, Rating, Reviews, Price, url từ trang list."""
    soup = BeautifulSoup(html, "lxml")

    # Amazon đổi layout → thử nhiều selector
    selectors = [
        "div.zg-grid-general-faceout",
        "div.p13n-sc-uncoverable-faceout",
        "div._cDEzb_grid-row_3Cywl > div",
        "div._cDEzb_p13n-sc-uncoverable-card_3Xk2N",
        "div.p13n-sc-uncoverable-faceout-legacy"
    ]
    cards = []
    for sel in selectors:
        cards = soup.select(sel)
        if cards: break

    items = []
    for c in cards:
        # Title / Name
        t = (c.select_one("a.a-link-normal > span")
             or c.select_one("div.p13n-sc-truncate")
             or c.select_one("img[alt]"))
        if not t: 
            continue
        name = t.get_text(strip=True) if t.name != "img" else t.get("alt", "").strip()
        if not name:
            continue

        # URL
        a = c.select_one("a.a-link-normal")
        href = a.get("href") if a else None
        url = urljoin(BASE, href) if href else None

        # Author
        author_el = c.select_one(".a-color-secondary .a-size-small, .a-row.a-size-small")
        author = author_el.get_text(" ", strip=True) if author_el else None

        # Rating
        rating = None
        rating_el = c.select_one("span.a-icon-alt")
        if rating_el:
            m = re.search(r"([0-9.]+)\s+out of 5", rating_el.get_text(strip=True))
            rating = float(m.group(1)) if m else None

        # Reviews
        reviews = None
        reviews_el = c.select_one("span.a-size-small.a-link-normal")
        if reviews_el:
            reviews = clean_int(reviews_el.get_text())

        # Price
        price_el = (c.select_one("span.a-price > span.a-offscreen")
                    or c.select_one(".p13n-sc-price")
                    or c.select_one("span.a-color-price"))
        price = parse_price(price_el.get_text(strip=True) if price_el else None)

        items.append({
            "Name": name,
            "Author": author,
            "User Rating": rating,
            "Reviews": reviews,
            "Price": price,
            "url": url,
        })
    return items

# =================== TÌM LINK TRANG TIẾP THEO ===================
def find_next_url(html: str):
    """Tìm URL 'Next' (hỗ trợ cả route gp/bestsellers và zgbs)."""
    soup = BeautifulSoup(html, "lxml")
    # pagination chuẩn
    a = soup.select_one("ul.a-pagination li.a-last a")
    if a and a.get("href"):
        return urljoin(BASE, a["href"])
    # fallback: có ?pg= hoặc mẫu zg_bs_pg_
    a = soup.select_one('a[href*="?pg="]') or soup.select_one('a[href*="zg_bs_pg_"]')
    if a and a.get("href"):
        return urljoin(BASE, a["href"])
    # <link rel="next">
    link_next = soup.find("link", rel="next")
    if link_next and link_next.get("href"):
        return urljoin(BASE, link_next["href"])
    return None

# =================== PARSE TRANG CHI TIẾT ===================
def get_book_details(detail_url: str):
    """Trả về (language, publisher_year, genre) từ trang chi tiết."""
    if not detail_url:
        return None, None, None

    resp = http_get(detail_url)
    soup = BeautifulSoup(resp.text, "lxml")

    # Genre từ breadcrumb
    genre = None
    crumbs = [a.get_text(" ", strip=True) for a in soup.select("#wayfinding-breadcrumbs_feature_div a")]
    joined = " > ".join(crumbs).lower()
    if joined:
        genre = "Fiction" if "fiction" in joined else "Non-fiction"

    language = None
    publication_text = None

    # detail bullets mới
    for li in soup.select("#detailBullets_feature_div li"):
        label_node = li.select_one("span.a-text-bold")
        if label_node:
            label = label_node.get_text(strip=True).rstrip(":").lower()
            value_full = li.get_text(" ", strip=True)
            value = value_full.split(":", 1)[1].strip() if ":" in value_full else value_full.strip()
        else:
            spans = li.select("span")
            if len(spans) >= 2:
                label = spans[0].get_text(strip=True).rstrip(":").lower()
                value = spans[-1].get_text(" ", strip=True).strip()
            else:
                continue

        if "language" in label and not language:
            language = value
        if ("publication date" in label or "publisher" in label) and not publication_text:
            publication_text = value

    # bảng cũ
    if not language or not publication_text:
        for row in soup.select("#productDetailsTable tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True).lower()
            val = td.get_text(" ", strip=True)
            if "language" in key and not language:
                language = val
            if ("publication date" in key or "publisher" in key) and not publication_text:
                publication_text = val

    publisher_year = to_year(publication_text)
    return language, publisher_year, genre

# =================== ĐIỀU PHỐI: LẤY ĐỦ 52 ===================
def scrape_exact_n(n=52,
                   pause_between_pages=(5, 9),
                   pause_detail=(2.5, 4.5),
                   max_pages=10):
    """Đi theo 'Next' từ LIST_URL, gom theo thứ tự, dừng khi đủ n."""
    results, seen = [], set()
    page_count = 0
    url = LIST_URL

    while url and len(results) < n and page_count < max_pages:
        print(f"[INFO] Fetching list page: {url}")
        resp = http_get(url)
        html = resp.text

        items = parse_list_page(html)
        for it in items:
            key = (it.get("Name"), it.get("Author"))
            if key in seen:
                continue
            seen.add(key)

            # lấy chi tiết
            lang, pub_year, genre = None, None, None
            try:
                lang, pub_year, genre = get_book_details(it.get("url"))
                time.sleep(random.uniform(*pause_detail))
            except Exception:
                pass

            results.append({
                "Name": it.get("Name"),
                "Author": it.get("Author"),
                "User Rating": it.get("User Rating"),
                "Reviews": it.get("Reviews"),
                "Language": lang,
                "Price": it.get("Price"),
                "Publisher_year": pub_year,
                "Genre": genre,
            })
            if len(results) >= n:
                break

        if len(results) >= n:
            break

        url = find_next_url(html)  # theo link Next bất kể dạng nào
        page_count += 1
        time.sleep(random.uniform(*pause_between_pages))

    return results

# =================== CLI =================
def main():
    ap = argparse.ArgumentParser(description="Scrape ~52 Amazon Best Sellers (Books).")
    ap.add_argument("--limit", type=int, default=52, help="Số sách cần lấy (mặc định 52)")
    ap.add_argument("--out", type=str, default="data/samples/books_52.csv", help="Đường dẫn file CSV xuất")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    rows = scrape_exact_n(n=args.limit)

    fieldnames = ["Name", "Author", "User Rating", "Reviews", "Language", "Price", "Publisher_year", "Genre"]
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"[DONE] Saved {len(rows)} rows to {args.out}")

if __name__ == "__main__":
    main(