import requests
from bs4 import BeautifulSoup
import time
import random
import re
from urllib.parse import urljoin
import pandas as pd

BASE_URL = 'https://homedy.com/ban-nha-dat-ha-noi'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
}


def clean_number(text):
    if not text:
        return None
    s = re.sub(r'[\,\.\s]+', '', str(text))  # remove separators and spaces
    digits = re.findall(r'\d+', s)
    if not digits:
        return None
    return int(''.join(digits))


def extract_from_item(item, page_url):
    # Trả về dict với các trường phổ biến; dùng nhiều selector để tăng độ bền
    def t(sel_list):
        for sel in sel_list:
            e = item.select_one(sel)
            if e:
                return e.get_text(strip=True)
        return None

    title = t(['h3.product-title', 'h3 a', 'a.product-title', '.product-title'])
    link_tag = item.select_one('a')
    url = urljoin(page_url, link_tag['href']) if link_tag and link_tag.get('href') else None
    price = t(['.product-price', '.price', '.product-item__price', '.price-value'])
    location = t(['.product-address', '.product-location', '.address', '.location'])
    # try to get area from item text or title (m2)
    area = None
    area_text = t(['.area', '.product-area']) or (title or '')
    m = re.search(r'(\d+[\.,]?\d*)\s*(m2|m²|m)', (area_text or '').lower())
    if m:
        area = clean_number(m.group(1))
    # short description/snippet if exists
    snippet = t(['.product-desc', '.desc', '.product-description', 'p'])

    return {
        'title': title,
        'url': url,
        'price_raw': price,
        'price_vnd': clean_number(price),
        'location': location,
        'area_m2': area,
        'snippet': snippet
    }


def find_next_page(soup, current_url):
    # tìm link 'next' thông dụng
    sel_candidates = ['a[rel=next]', '.pagination a.next', 'li.next a', "a[aria-label=\"Next\"]", '.page-item.next a']
    for sel in sel_candidates:
        a = soup.select_one(sel)
        if a and a.get('href'):
            return urljoin(current_url, a['href'])
    # fallback: tìm link có text '›' hoặc '»' hoặc 'Next'
    for a in soup.find_all('a'):
        if a.get('href') and (a.get_text(strip=True) in ['›','»','Next','next','Trang sau'] or 'next' in (a.get('class') or [])):
            return urljoin(current_url, a['href'])
    return None


def crawl(base_url, max_pages=50, delay=(1,2)):
    results = []
    url = base_url
    page = 0
    while url and page < max_pages:
        page += 1
        print(f'Fetching page {page}: {url}')
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            print('Request failed:', e)
            break

        soup = BeautifulSoup(resp.content, 'html.parser')
        items = soup.find_all('div', class_=lambda c: c and 'product-item' in c)
        if not items:
            # fallback: try generic item selectors
            items = soup.select('.product, .listing-item, .item')
        print(f'  Found {len(items)} items on page {page}')
        for it in items:
            try:
                rec = extract_from_item(it, url)
                results.append(rec)
            except Exception as e:
                print('  Error extracting item:', e)

        next_url = find_next_page(soup, url)
        if not next_url:
            print('No next page found, stopping.')
            break
        url = next_url
        time.sleep(random.uniform(*delay))
    return results


if __name__ == '__main__':
    data = crawl(BASE_URL, max_pages=30, delay=(1.0, 2.5))
    df = pd.DataFrame(data)
    for c in ['title','location','snippet','url','price_raw']:
        if c in df.columns:
            df[c] = df[c].fillna('').astype(str)
    out_path = 'homedy_listings_hanoi.csv'
    df.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f'Saved {len(df)} rows to {out_path}')
