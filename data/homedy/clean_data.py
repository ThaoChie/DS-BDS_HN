#!/usr/bin/env python3
"""Clean homedy listings CSV for ML-ready dataset.

Produces two files:
- homedy_listings_hanoi_clean.csv (parsed numeric columns, deduped)
- homedy_listings_hanoi_clean_imputed.csv (same but area imputed with median)

Run: python3 clean_data.py
"""
import re
import math
import sys
from statistics import mean
import pandas as pd


INPUT = 'homedy_listings_hanoi.csv'
OUTPUT_CLEAN = 'homedy_listings_hanoi_clean.csv'
OUTPUT_IMPUTED = 'homedy_listings_hanoi_clean_imputed.csv'


def parse_price_to_million(s: str):
    """Parse price string (Vietnamese) to a numeric value in million VND.

    Returns float (million VND) or None.
    Handles: '12,5 Tỷ', '58 Triệu', '5 - 5,2 Tỷ', 'Thỏa thuận', ranges -> mean.
    """
    if not isinstance(s, str):
        return None
    raw = s.strip()
    if not raw:
        return None
    low = raw.lower()
    if 'thỏa' in low or 'thoa' in low or 'thỏa thuận' in low:
        raw.pd.dropna(axis=0, how='all', inplace=True)
        return raw

    # normalize hyphens
    raw = raw.replace('\u2013', '-').replace('\u2014', '-').replace('\xa0', ' ')
    # remove emoji and stray non-ASCII except digits, comma, dot, dash and letters
    raw = re.sub(r"[^0-9,\.\-\styỷtyrtriệutrieuMHzKmBbKk]", ' ', raw)
    raw = raw.strip()

    # unit detection
    unit = None
    if re.search(r'ty|t\u1ef5|tỷ', low):
        unit = 'ty'
    elif 'triệu' in low or 'trieu' in low or 'triệu' in low:
        unit = 'trieu'

    # split ranges
    parts = re.split(r'\s*-\s*|\sto\s|\s–\s', raw)
    nums = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        # extract number-like substring
        m = re.search(r'[0-9]+[\.,]?[0-9]*', p)
        if not m:
            continue
        num_s = m.group(0)
        # replace comma as decimal separator when appropriate
        if num_s.count(',') == 1 and num_s.count('.') == 0:
            num_s = num_s.replace(',', '.')
        else:
            num_s = num_s.replace(',', '')
        try:
            num = float(num_s)
        except Exception:
            continue
        nums.append(num)

    if not nums:
        return None

    # If unit not explicitly found, try to infer: very large numbers >1000 likely in million or billion
    if unit is None:
        # if original text contains 'triệu' in any latin form
        if re.search(r'tri[eê]u|trieu', low):
            unit = 'trieu'
        elif re.search(r'ty|t\u1ef5|tỷ', low):
            unit = 'ty'
        else:
            # heuristics: if any number > 1000 -> value probably in million already
            if any(x > 1000 for x in nums):
                unit = 'trieu'
            else:
                # if numbers are small (<100) and raw contains ' T' or 'ty' treat as ty
                if any(x < 100 for x in nums) and ('ty' in low or ' t ' in (' ' + low + ' ')):
                    unit = 'ty'
                else:
                    # default: if numbers < 1000 assume 'ty' if <100 and contains comma decimal
                    unit = 'ty' if any(x <= 1000 for x in nums) else 'trieu'

    # compute mean of numbers
    val = mean(nums)
    if unit == 'ty':
        return float(val * 1000.0)
    elif unit == 'trieu':
        return float(val)
    else:
        return float(val)


def clean_location(loc: str):
    if not isinstance(loc, str):
        return None
    s = loc.replace('\n', ' ').strip()
    s = re.sub(r'\s+', ' ', s)
    # Normalize commas and spacing
    s = s.strip(' ,')
    return s


def extract_district(loc: str):
    if not isinstance(loc, str):
        return None
    s = loc
    # patterns: Quận X, Huyện Y, Thị xã Z, TP., tỉnh etc.
    m = re.search(r'(Quận|Quận|Qu?n|Huyện|Huyen|Thị xã|Thi xa|Thị xã|Thị Xã)\s+([^,\n]+)', s, re.I)
    if m:
        return m.group(0).strip()
    # fallback: take part before comma
    if ',' in s:
        return s.split(',')[0].strip()
    return s.strip()


def remove_emojis(text):
    if not isinstance(text, str):
        return text
    return re.sub(r'[\U00010000-\U0010ffff]', '', text)


def main():
    df = pd.read_csv(INPUT, dtype=str)
    # normalize column names
    df.columns = [c.strip() for c in df.columns]

    # keep original
    df['price_raw_orig'] = df.get('price_raw')

    # drop exact duplicates
    before = len(df)
    df = df.drop_duplicates(subset=['url'])
    after = len(df)
    print(f'Dropped {before-after} duplicate rows by url')

    # clean title
    df['title'] = df['title'].astype(str).map(lambda x: remove_emojis(x).strip())

    # parse price into million VND
    df['price_million'] = df['price_raw'].map(parse_price_to_million)
    # also compute price_billion for convenience
    df['price_billion'] = df['price_million'].map(lambda x: None if x is None or math.isnan(x) else x/1000.0)

    # replace previous numeric price_vnd if inconsistent
    # convert existing price_vnd to float safely
    def safe_float(x):
        try:
            return float(x)
        except Exception:
            return None

    df['price_vnd_original'] = df.get('price_vnd').map(safe_float)

    # if price_million exists use it; else fallback to price_vnd_original (assume it was in millions?)
    df['price_million_final'] = df['price_million']
    df.loc[df['price_million_final'].isna() & df['price_vnd_original'].notna(), 'price_million_final'] = df['price_vnd_original']

    # clean area
    df['area_m2'] = pd.to_numeric(df['area_m2'], errors='coerce')
    # invalid area ranges -> NaN
    df.loc[(df['area_m2'] <= 0) | (df['area_m2'] > 2000), 'area_m2'] = pd.NA

    # clean location and extract district
    df['location'] = df['location'].map(clean_location)
    df['district'] = df['location'].map(extract_district)

    # clean snippet
    if 'snippet' in df.columns:
        df['snippet'] = df['snippet'].fillna('').astype(str).map(lambda x: x.strip())
    else:
        df['snippet'] = ''

    # final important columns order
    out_cols = [
        'title', 'url', 'price_raw_orig', 'price_million_final', 'price_billion',
        'price_vnd_original', 'location', 'district', 'area_m2', 'snippet'
    ]

    out = df[out_cols].copy()

    # write cleaned file
    out.to_csv(OUTPUT_CLEAN, index=False)
    print(f'Wrote cleaned file: {OUTPUT_CLEAN} ({len(out)} rows)')

    # create imputed copy: fill area with median
    area_med = pd.to_numeric(out['area_m2'], errors='coerce').median()
    out_imputed = out.copy()
    out_imputed['area_m2'] = out_imputed['area_m2'].fillna(area_med)
    out_imputed.to_csv(OUTPUT_IMPUTED, index=False)
    print(f'Wrote imputed file: {OUTPUT_IMPUTED} (area median={area_med})')

    # print summary
    print('\nSummary:')
    print(out[['price_million_final', 'area_m2', 'district']].describe(include='all'))
    print('\nSample rows:')
    print(out.head(10).to_string(index=False))


if __name__ == '__main__':
    main()
