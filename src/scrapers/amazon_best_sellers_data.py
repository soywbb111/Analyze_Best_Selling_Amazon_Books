import os
import re
import csv
import time
import random
import datetime
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =================== CẤU HÌNH CƠ BẢN ===================
BASE_URL = "https://www.amazon.com"
START_URL = "https://www.amazon.com/gp/bestsellers/books"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

# =================== HTTP SESSION VÀ RETRY LOGIC ===================
def make_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

SESSION = make_session()

def http_get(url):
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": BASE_URL,
    }
    try:
        response = SESSION.get(url, headers=headers, timeout=30)
        response.raise_for_status() # Báo lỗi nếu status code là 4xx hoặc 5xx
        if "captcha" in response.text.lower() or "robot check" in response.text.lower():
            raise Exception("Bị chặn bởi CAPTCHA/Robot check.")
        return response
    except Exception as e:
        print(f"[ERROR] Không thể tải {url}: {e}")
        return None

# =================== CÁC HÀM PARSING (BÓC TÁCH DỮ LIỆU) ===================

def parse_list_page(html):
    # ... hàm này giữ nguyên như cũ ...
    soup = BeautifulSoup(html, "lxml")
    selectors = [
        "div.zg-grid-general-faceout", "div.p13n-sc-uncoverable-faceout",
        "div._cDEzb_grid-row_3Cywl > div"
    ]
    cards = []
    for sel in selectors:
        cards = soup.select(sel)
        if cards: break
    items = []
    for c in cards:
        t = (c.select_one("a.a-link-normal > span") or c.select_one("div.p13n-sc-truncate") or c.select_one("img[alt]"))
        if not t: continue
        name = t.get_text(strip=True) if t.name != "img" else t.get("alt", "").strip()
        if not name: continue
        a = c.select_one("a.a-link-normal")
        url = urljoin(BASE_URL, a.get("href")) if a and a.get('href') else None
        author_el = c.select_one(".a-color-secondary .a-size-small, .a-row.a-size-small")
        author = author_el.get_text(" ", strip=True) if author_el else None
        rating_el = c.select_one("span.a-icon-alt")
        rating = None
        if rating_el:
            m = re.search(r"([0-9.]+)\s+out of 5", rating_el.get_text(strip=True))
            rating = float(m.group(1)) if m else None
        price_el = (c.select_one("span.a-price > span.a-offscreen") or c.select_one(".p13n-sc-price") or c.select_one("span.a-color-price"))
        price = float(re.search(r"([\d\.]+)", price_el.get_text().replace(",", ""))[1]) if price_el else None
        items.append({"Name": name, "Author": author, "User Rating": rating, "Price": price, "url": url})
    return items

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

def parse_book_details_from_html(html):
    """Hàm này chỉ nhận HTML và bóc tách, không tự tải."""
    soup = BeautifulSoup(html, "lxml")
    details = {"Language": None, "Publisher_year": None, "Genre": "Non-fiction"}
    crumbs = [a.get_text(" ", strip=True).lower() for a in soup.select("#wayfinding-breadcrumbs_feature_div a")]
    if any("fiction" in c for c in crumbs):
        details["Genre"] = "Fiction"
    publication_text = None
    for li in soup.select("#detailBullets_feature_div li"):
        text = li.get_text(" ", strip=True)
        text_lower = text.lower()
        if "language" in text_lower:
            details["Language"] = text.split(":")[-1].strip()
        if "publication date" in text_lower or "publisher" in text_lower:
            publication_text = text.split(":")[-1].strip()
    details["Publisher_year"] = to_year(publication_text)
    return details

def find_next_url(html):
    soup = BeautifulSoup(html, "lxml")
    next_link = soup.select_one("ul.a-pagination li.a-last a")
    if next_link and next_link.get("href"):
        return urljoin(BASE_URL, next_link["href"])
    return None

# =================== HÀM ĐIỀU PHỐI CHÍNH ===================
def scrape_online_with_details(limit=52):
    results = []
    seen_books = set()
    current_url = START_URL
    page_count = 1

    while current_url and len(results) < limit:
        print(f"--- Đang tải trang danh sách {page_count}: {current_url} ---")
        list_response = http_get(current_url)
        if not list_response:
            print("Không thể tải trang danh sách, dừng chương trình.")
            break
        
        books_on_page = parse_list_page(list_response.text)
        print(f"Tìm thấy {len(books_on_page)} sách trên trang này.")

        for book in books_on_page:
            if len(results) >= limit:
                break

            book_key = (book.get("Name"), book.get("Author"))
            if book_key in seen_books:
                continue
            
            print(f"-> Đang xử lý sách: {book.get('Name')}")
            
            # Tải trang chi tiết
            detail_url = book.get("url")
            if detail_url:
                print(f"   Đang tải chi tiết từ: {detail_url[:50]}...")
                # TẠM DỪNG GIỮA CÁC LẦN GỌI ĐẾN TRANG CHI TIẾT ĐỂ TRÁNH BỊ CHẶN
                time.sleep(random.uniform(2, 5)) 
                
                detail_response = http_get(detail_url)
                if detail_response:
                    details = parse_book_details_from_html(detail_response.text)
                    book.update(details) # Gộp thông tin chi tiết vào
            
            results.append(book)
            seen_books.add(book_key)
            print(f"   => Đã lấy xong. Tổng số sách: {len(results)}/{limit}")

        if len(results) >= limit:
            print(f"\n[INFO] Đã thu thập đủ {len(results)} sách. Dừng lại.")
            break

        # Tạm dừng giữa các trang danh sách
        print("Tạm dừng trước khi sang trang mới...")
        time.sleep(random.uniform(3, 6))
        
        current_url = find_next_url(list_response.text)
        page_count += 1
    
    return results[:limit]

# =================== HÀM MAIN ĐỂ CHẠY ===================
def main():
    LIMIT = 52
    output_csv_path = "../../data/raw/52books_online_results.csv"

    final_books = scrape_online_with_details(limit=LIMIT)

    if not final_books:
        print("\nKhông có dữ liệu nào được thu thập. Kết thúc.")
        return
        
    print(f"\n--- Ghi {len(final_books)} cuốn sách ra file {output_csv_path} ---")
    fieldnames = ["Name", "Author", "User Rating", "Price", "Language", "Publisher_year", "Genre", "url"]
    
    try:
        with open(output_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(final_books)
        print(f"✅ Ghi file thành công!")
    except Exception as e:
        print(f"[ERROR] Gặp lỗi khi ghi file CSV: {e}")

if __name__ == "__main__":
    main()