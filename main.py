import requests
from bs4 import BeautifulSoup
import datetime
import time
import random
import csv
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import asyncio
import html as html_module

from crawl4ai.async_webcrawler import AsyncWebCrawler

BASE_URL = "https://www.pib.gov.in/allRel.aspx?reg=3&lang=1"
ROOT = "https://www.pib.gov.in"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}

#
def random_sleep():
    time.sleep(random.uniform(0.2, 0.5))


# -------------------------
# ASP.NET hidden fields
# -------------------------
def get_viewstate_payload(soup: BeautifulSoup):
    payload = {}
    for inp in soup.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        payload[name] = inp.get("value", "")
    for sel in soup.find_all("select"):
        name = sel.get("name")
        if not name:
            continue
        opt = sel.find("option", selected=True)
        if opt:
            payload[name] = opt.get("value", "")
        else:
            first = sel.find("option")
            payload[name] = first.get("value", "") if first else ""
    return payload


def post_for_month(session: requests.Session, month: int, year: int, day: int | None):
    """Return HTML for listing page with (month, year, day-or-All)."""
    r = session.get(BASE_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    payload = get_viewstate_payload(soup)

    payload["ctl00$ContentPlaceHolder1$ddlday"] = "0" if day is None else str(day)
    payload["ctl00$ContentPlaceHolder1$ddlMonth"] = str(month)
    payload["ctl00$ContentPlaceHolder1$ddlYear"] = str(year)

    # event target depends on whether day was changed
    payload["__EVENTTARGET"] = "ctl00$ContentPlaceHolder1$ddlMonth" if day is None else "ctl00$ContentPlaceHolder1$ddlday"
    payload["__EVENTARGUMENT"] = ""

    random_sleep()
    r2 = session.post(BASE_URL, data=payload, headers=HEADERS, timeout=30)
    r2.raise_for_status()
    return r2.text


def extract_release_links(listing_html: str):
    soup = BeautifulSoup(listing_html, "html.parser")
    links = []
    for a in soup.select("a[href*='PressReleasePage.aspx']"):
        href = a.get("href")
        if not href:
            continue
        if href.startswith("/"):
            full = ROOT + href
        elif href.startswith("http"):
            full = href
        else:
            full = ROOT + "/" + href
        if full not in links:
            links.append(full)
    return links


def find_hindi_link_from_release_html(html: str):
    soup = BeautifulSoup(html, "html.parser")
    block = soup.find("div", class_="ReleaseLang")
    if block:
        for a in block.find_all("a"):
            txt = (a.get_text() or "").strip()
            if "हिन्दी" in txt or "हिंदी" in txt or txt.lower() == "hindi":
                href = a.get("href")
                if not href:
                    continue
                if href.startswith("/"):
                    return ROOT + href
                if href.startswith("http"):
                    return href
                return ROOT + "/" + href
    # fallback search anywhere
    a = soup.find("a", string=lambda t: t and ("हिन्दी" in t or "हिंदी" in t or t.strip().lower() == "hindi"))
    if a:
        href = a.get("href")
        if href:
            if href.startswith("/"): return ROOT + href
            if href.startswith("http"): return href
            return ROOT + "/" + href
    return None


# -------------------------
# BeautifulSoup fallback cleaner
# -------------------------
def bs_extract_text_from_release_html(html: str) -> str:
    """
    Fallback extractor: check hidden field ltrDescriptionn, PdfDiv, main content p tags, or aggregate <p>.
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1) Check hidden input ltrDescriptionn (contains HTML-escaped content)
    hidden = soup.find("input", {"id": "ltrDescriptionn"})
    if hidden and hidden.get("value"):
        raw = hidden.get("value")
        # it's HTML-escaped, so unescape and strip tags
        unescaped = html_module.unescape(raw)
        txt_soup = BeautifulSoup(unescaped, "html.parser")
        texts = [t.get_text(separator=" ", strip=True) for t in txt_soup.find_all(["p", "div", "span"])]
        joined = "\n\n".join([t for t in texts if t])
        if joined.strip():
            return joined.strip()

    # 2) PdfDiv (often contains the printable content)
    pdfdiv = soup.find(id="PdfDiv")
    if pdfdiv:
        txt = pdfdiv.get_text(separator="\n", strip=True)
        if txt.strip():
            return txt.strip()

    # 3) main content area - many pages have .innner-page-main-about-us-content-right-part
    main = soup.find(class_="innner-page-main-about-us-content-right-part")
    if main:
        # collect p tags
        ps = main.find_all("p")
        if ps:
            texts = [p.get_text(separator=" ", strip=True) for p in ps]
            joined = "\n\n".join([t for t in texts if t])
            if joined.strip():
                return joined.strip()
        # else fallback to all text
        txt = main.get_text(separator="\n", strip=True)
        if txt.strip():
            return txt.strip()

    # 4) Generic fallback - join all <p> on page
    ps = soup.find_all("p")
    if ps:
        texts = [p.get_text(separator=" ", strip=True) for p in ps]
        joined = "\n\n".join([t for t in texts if t])
        if joined.strip():
            return joined.strip()

    # 5) final fallback - body text
    body = soup.body
    if body:
        txt = body.get_text(separator="\n", strip=True)
        return txt.strip()

    return ""


# -------------------------
# crawl4ai on raw HTML (AsyncWebCrawler)
# -------------------------
def clean_html_with_asynccrawler(html: str) -> str:
    """
    Try to run AsyncWebCrawler on the provided raw HTML.
    Use arun(html=...) if available; otherwise try arun with a data: URL fallback.
    """
    async def fetch_using_html():
        crawler = AsyncWebCrawler()
        # try likely parameter name 'html' or 'source' — prefer html
        try:
            # common API: arun(html=...)
            res = await crawler.arun(html=html)
            return getattr(res, "markdown", None) or getattr(res, "text", None) or ""
        except TypeError:
            # try alternative call signature if arun doesn't accept 'html'
            try:
                res = await crawler.arun(document=html)  # some libs use 'document'
                return getattr(res, "markdown", None) or getattr(res, "text", None) or ""
            except Exception:
                # fallback to making a tiny in-memory data URL (less ideal but sometimes works)
                try:
                    # create data URL (may or may not be accepted by crawler)
                    data_url = "data:text/html;charset=utf-8," + requests.utils.requote_uri(html[:100000])
                    res = await crawler.arun(data_url)
                    return getattr(res, "markdown", None) or getattr(res, "text", None) or ""
                except Exception:
                    raise

    try:
        return asyncio.run(fetch_using_html())
    except Exception:
        # any failure reported to caller; return empty to trigger BS fallback
        return ""


# -------------------------
# worker: cleans HTML pair (eng_html, hin_html)
# -------------------------
def worker_clean_html(pair):
    """
    pair: (eng_html, hin_html)
    returns (eng_text, hin_text) or None
    """
    eng_html, hin_html = pair

    # politeness per worker
    random_sleep()

    # Try crawl4ai on English HTML
    eng_text = ""
    try:
        eng_text = clean_html_with_asynccrawler(eng_html)
    except Exception:
        eng_text = ""

    # Fallback if empty
    if not eng_text:
        eng_text = bs_extract_text_from_release_html(eng_html)

    if not eng_text:
        return None

    random_sleep()

    hin_text = ""
    try:
        hin_text = clean_html_with_asynccrawler(hin_html)
    except Exception:
        hin_text = ""

    if not hin_text:
        hin_text = bs_extract_text_from_release_html(hin_html)

    if not hin_text:
        return None

    return (eng_text, hin_text)


# -------------------------
# Main orchestration
# -------------------------
def iterate_by_month_parallel(start_year: int, start_month: int, start_day: int | None,
                              output_csv="pib_bilingual.csv", workers=None):
    if workers is None:
        workers = max(2, cpu_count() // 2)

    today = datetime.date.today()
    cur_year = start_year
    cur_month = start_month

    sess = requests.Session()
    sess.headers.update(HEADERS)

    total_months = (today.year - start_year) * 12 + (today.month - start_month) + 1
    month_bar = tqdm(total=total_months, desc="Months", ncols=80)

    saved_pairs = 0

    with open(output_csv, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, quoting=csv.QUOTE_ALL)
        writer.writerow(["English", "Hindi"])

        while (cur_year < today.year) or (cur_year == today.year and cur_month <= today.month):
            print(f"\n=== Month {cur_month:02d}-{cur_year}, Day={start_day if start_day else 'ALL'} ===")
            try:
                listing_html = post_for_month(sess, cur_month, cur_year, start_day)
                links = extract_release_links(listing_html)
                print("Found releases:", len(links))

                # Build raw-HTML candidate pairs (main process fetches both pages)
                candidate_html_pairs = []
                for link in tqdm(links, desc="Checking releases", leave=False, ncols=80):
                    try:
                        random_sleep()
                        r_en = sess.get(link, headers=HEADERS, timeout=30)
                        r_en.raise_for_status()
                        hindi_url = find_hindi_link_from_release_html(r_en.text)
                        if not hindi_url:
                            continue
                        # fetch hindi HTML using same session (preserves cookies)
                        random_sleep()
                        r_hi = sess.get(hindi_url, headers=HEADERS, timeout=30)
                        r_hi.raise_for_status()
                        candidate_html_pairs.append((r_en.text, r_hi.text))
                    except Exception:
                        continue

                print("Pairs queued for cleaning:", len(candidate_html_pairs))

                # Parallel cleaning (workers only process raw HTML; no requests to PIB)
                if candidate_html_pairs:
                    with Pool(workers) as p:
                        for res in tqdm(p.imap_unordered(worker_clean_html, candidate_html_pairs),
                                        total=len(candidate_html_pairs),
                                        desc="Cleaning",
                                        ncols=80):
                            if res:
                                writer.writerow(list(res))
                                saved_pairs += 1

                print("Saved so far:", saved_pairs)

            except Exception as e:
                print("Month-level error:", e)

            # advance month
            if cur_month == 12:
                cur_month = 1
                cur_year += 1
            else:
                cur_month += 1

            month_bar.update(1)

    month_bar.close()
    print("\n=== DONE ===")
    print("Total bilingual entries:", saved_pairs)
    print("Output CSV:", output_csv)


# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    # tuning
    START_YEAR = 2025
    START_MONTH = 12
    START_DAY = 4   # None for "All"
    WORKERS = 24

    iterate_by_month_parallel(
        START_YEAR,
        START_MONTH,
        START_DAY,
        output_csv="pib_bilingual.csv",
        workers=WORKERS
    )
