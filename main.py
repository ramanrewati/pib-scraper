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
from concurrent.futures import ThreadPoolExecutor, as_completed

from crawl4ai.async_webcrawler import AsyncWebCrawler


BASE_URL = "https://www.pib.gov.in/allRel.aspx?reg=3&lang=1"
ROOT = "https://www.pib.gov.in"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}


# -----------------------------------
# Politeness sleep
# -----------------------------------
def random_sleep():
    time.sleep(random.uniform(0.1, 0.3))


# -----------------------------------
# Deduplication of paragraphs
# -----------------------------------
def dedupe_paragraphs(text: str) -> str:
    """
    Remove duplicate paragraphs while preserving order.
    Paragraphs split on double newline.
    """
    seen = set()
    cleaned = []
    for para in text.split("\n\n"):
        p = para.strip()
        if not p:
            continue
        if p not in seen:
            seen.add(p)
            cleaned.append(p)
    return "\n\n".join(cleaned)


# -----------------------------------
# ASP.NET hidden field extraction
# -----------------------------------
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
        payload[name] = opt.get("value", "") if opt else sel.find("option").get("value", "")

    return payload


# -----------------------------------
# ASP.NET postback: month/year/day
# -----------------------------------
def post_for_month(session: requests.Session, month: int, year: int, day: int | None):
    r = session.get(BASE_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    payload = get_viewstate_payload(soup)

    payload["ctl00$ContentPlaceHolder1$ddlday"] = "0" if day is None else str(day)
    payload["ctl00$ContentPlaceHolder1$ddlMonth"] = str(month)
    payload["ctl00$ContentPlaceHolder1$ddlYear"] = str(year)

    payload["__EVENTTARGET"] = (
        "ctl00$ContentPlaceHolder1$ddlMonth" if day is None else "ctl00$ContentPlaceHolder1$ddlday"
    )
    payload["__EVENTARGUMENT"] = ""

    random_sleep()
    r2 = session.post(BASE_URL, data=payload, headers=HEADERS, timeout=30)
    r2.raise_for_status()
    return r2.text


# -----------------------------------
# Extract all release PRID links
# -----------------------------------
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


# -----------------------------------
# Extract Hindi PRID link
# -----------------------------------
def find_hindi_link_from_release_html(html: str):
    soup = BeautifulSoup(html, "html.parser")

    block = soup.find("div", class_="ReleaseLang")
    if block:
        for a in block.find_all("a"):
            txt = (a.get_text() or "").strip()
            if "हिन्दी" in txt or "हिंदी" in txt or txt.lower() == "hindi":
                href = a.get("href")
                if not href:
                    return None
                if href.startswith("/"):
                    return ROOT + href
                if href.startswith("http"):
                    return href
                return ROOT + "/" + href

    a = soup.find("a", string=lambda t: t and ("हिन्दी" in t or "हिंदी" in t or t.strip().lower() == "hindi"))
    if a:
        href = a.get("href")
        if href.startswith("/"):
            return ROOT + href
        if href.startswith("http"):
            return href
        return ROOT + "/" + href

    return None


# -----------------------------------
# BeautifulSoup fallback cleaner
# -----------------------------------
def bs_extract_text_from_release_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    hidden = soup.find("input", {"id": "ltrDescriptionn"})
    if hidden and hidden.get("value"):
        raw = hidden.get("value")
        unescaped = html_module.unescape(raw)
        txt_soup = BeautifulSoup(unescaped, "html.parser")
        texts = [
            t.get_text(separator=" ", strip=True)
            for t in txt_soup.find_all(["p", "div", "span"])
        ]
        joined = "\n\n".join([t for t in texts if t])
        if joined.strip():
            return joined.strip()

    pdfdiv = soup.find(id="PdfDiv")
    if pdfdiv:
        txt = pdfdiv.get_text(separator="\n", strip=True)
        if txt.strip():
            return txt.strip()

    main = soup.find(class_="innner-page-main-about-us-content-right-part")
    if main:
        ps = main.find_all("p")
        if ps:
            texts = [p.get_text(separator=" ", strip=True) for p in ps]
            joined = "\n\n".join([t for t in texts if t])
            if joined.strip():
                return joined.strip()
        txt = main.get_text(separator="\n", strip=True)
        if txt.strip():
            return txt.strip()

    ps = soup.find_all("p")
    if ps:
        texts = [p.get_text(separator=" ", strip=True) for p in ps]
        joined = "\n\n".join([t for t in texts if t])
        if joined.strip():
            return joined.strip()

    body = soup.body
    if body:
        return body.get_text(separator="\n", strip=True)

    return ""


# -----------------------------------
# crawl4ai cleaning for raw HTML
# -----------------------------------
def clean_html_with_asynccrawler(html: str) -> str:
    async def fetch_html():
        crawler = AsyncWebCrawler()
        try:
            res = await crawler.arun(html=html)
            return getattr(res, "markdown", None) or getattr(res, "text", None) or ""
        except TypeError:
            try:
                res = await crawler.arun(document=html)
                return getattr(res, "markdown", None) or getattr(res, "text", None) or ""
            except:
                return ""

    try:
        return asyncio.run(fetch_html())
    except:
        return ""


# -----------------------------------
# Worker: clean HTML -> text
# -----------------------------------
def worker_clean_html(pair):
    eng_html, hin_html = pair

    random_sleep()

    eng_text = clean_html_with_asynccrawler(eng_html) or bs_extract_text_from_release_html(eng_html)
    if not eng_text:
        return None
    eng_text = dedupe_paragraphs(eng_text)

    random_sleep()

    hin_text = clean_html_with_asynccrawler(hin_html) or bs_extract_text_from_release_html(hin_html)
    if not hin_text:
        return None
    hin_text = dedupe_paragraphs(hin_text)

    return (eng_text, hin_text)


# -----------------------------------
# MAIN LOOP
# -----------------------------------
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

        while cur_year < today.year or (cur_year == today.year and cur_month <= today.month):
            print(f"\n=== Month {cur_month:02d}-{cur_year}, Day={start_day or 'ALL'} ===")

            try:
                listing_html = post_for_month(sess, cur_month, cur_year, start_day)
                links = extract_release_links(listing_html)
                print("Found releases:", len(links))

                # ------------------------ PARALLEL FETCH ------------------------
                candidate_html_pairs = []

                def fetch_pair(link):
                    try:
                        r_en = sess.get(link, headers=HEADERS, timeout=30)
                        r_en.raise_for_status()
                        hindi_url = find_hindi_link_from_release_html(r_en.text)
                        if not hindi_url:
                            return None
                        r_hi = sess.get(hindi_url, headers=HEADERS, timeout=30)
                        r_hi.raise_for_status()
                        return (r_en.text, r_hi.text)
                    except:
                        return None

                with ThreadPoolExecutor(max_workers=32) as executor:
                    futures = {executor.submit(fetch_pair, link): link for link in links}
                    for future in tqdm(
                        as_completed(futures),
                        total=len(links),
                        desc="Fetching EN+HI in parallel",
                        ncols=80,
                        leave=False,
                    ):
                        res = future.result()
                        if res:
                            candidate_html_pairs.append(res)
                # -----------------------------------------------------------------

                print("Pairs queued for cleaning:", len(candidate_html_pairs))

                if candidate_html_pairs:
                    with Pool(workers) as p:
                        for res in tqdm(
                            p.imap_unordered(worker_clean_html, candidate_html_pairs),
                            total=len(candidate_html_pairs),
                            desc="Cleaning",
                            ncols=80,
                        ):
                            if res:
                                writer.writerow(list(res))
                                saved_pairs += 1

                print("Saved so far:", saved_pairs)

            except Exception as e:
                print("Month-level error:", e)

            # move to next month
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


# -----------------------------------
# Run
# -----------------------------------
if __name__ == "__main__":
    START_YEAR = 2025
    START_MONTH = 12
    START_DAY = 4    # None for ALL
    WORKERS = 24

    iterate_by_month_parallel(
        START_YEAR,
        START_MONTH,
        START_DAY,
        output_csv="pib_bilingual.csv",
        workers=WORKERS
    )
