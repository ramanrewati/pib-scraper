
import requests
from bs4 import BeautifulSoup
import datetime
import time
import random
import csv
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

import asyncio
from crawl4ai.async_webcrawler import AsyncWebCrawler

BASE_URL = "https://www.pib.gov.in/allRel.aspx?reg=3&lang=1"
ROOT = "https://www.pib.gov.in"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
}


# -------------------------------------------------
# Politeness sleep
# -------------------------------------------------
def random_sleep():
    time.sleep(random.uniform(2, 5))


# -------------------------------------------------
# Extract ASP.NET hidden fields
# -------------------------------------------------
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


# -------------------------------------------------
# Correct ASP.NET postback for (month, year, day)
# -------------------------------------------------
def post_for_month(session: requests.Session, month: int, year: int, day: int | None):
    """Return HTML for selected month/year/day."""
    r = session.get(BASE_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    payload = get_viewstate_payload(soup)

    # DAY handling
    if day is None:
        payload["ctl00$ContentPlaceHolder1$ddlday"] = "0"       # All
    else:
        payload["ctl00$ContentPlaceHolder1$ddlday"] = str(day)  # Specific date

    payload["ctl00$ContentPlaceHolder1$ddlMonth"] = str(month)
    payload["ctl00$ContentPlaceHolder1$ddlYear"]  = str(year)

    # Event target logic
    if day is None:
        payload["__EVENTTARGET"] = "ctl00$ContentPlaceHolder1$ddlMonth"
    else:
        payload["__EVENTTARGET"] = "ctl00$ContentPlaceHolder1$ddlday"

    payload["__EVENTARGUMENT"] = ""

    random_sleep()
    r2 = session.post(BASE_URL, data=payload, headers=HEADERS, timeout=30)
    r2.raise_for_status()
    return r2.text


# -------------------------------------------------
# Extract all release PRID links from listing page
# -------------------------------------------------
def extract_release_links(html: str):
    soup = BeautifulSoup(html, "html.parser")
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


# -------------------------------------------------
# Detect Hindi PRID link inside ReleaseLang block
# -------------------------------------------------
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

    # fallback search
    a = soup.find("a", string=lambda t: t and ("हिन्दी" in t or "हिंदी" in t or t.strip().lower() == "hindi"))
    if a:
        href = a.get("href")
        if href:
            if href.startswith("/"): return ROOT + href
            if href.startswith("http"): return href
            return ROOT + "/" + href

    return None


# -------------------------------------------------
# crawl4ai async wrapper
# -------------------------------------------------
def clean_text_with_asynccrawler(url: str):
    async def fetch():
        crawler = AsyncWebCrawler()
        result = await crawler.arun(url)
        txt = getattr(result, "markdown", None) or getattr(result, "text", None) or ""
        return txt.strip()

    try:
        return asyncio.run(fetch())
    except RuntimeError:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(fetch())
    except Exception:
        return ""


# -------------------------------------------------
# Worker function for pool
# -------------------------------------------------
def worker_process(pair):
    """ pair = (eng_url, hindi_url) """
    eng_url, hin_url = pair

    random_sleep()
    eng = clean_text_with_asynccrawler(eng_url)
    if not eng:
        return None

    random_sleep()
    hin = clean_text_with_asynccrawler(hin_url)
    if not hin:
        return None

    return (eng, hin)


# -------------------------------------------------
# MAIN LOOP
# -------------------------------------------------
def iterate_by_month_parallel(start_year: int, start_month: int, start_day: int | None,
                              output_csv="pib_bilingual.csv", workers=None):

    if workers is None:
        workers = max(2, cpu_count() // 2)

    today = datetime.date.today()

    cur_year = start_year
    cur_month = start_month

    sess = requests.Session()
    sess.headers.update(HEADERS)

    # Month range
    total_months = (today.year - start_year) * 12 + (today.month - start_month) + 1

    month_bar = tqdm(total=total_months, desc="Months", ncols=80)

    saved_pairs = 0

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["English", "Hindi"])

        while (cur_year < today.year) or (cur_year == today.year and cur_month <= today.month):

            print(f"\n=== Month {cur_month:02d}-{cur_year}, Day={start_day if start_day else 'ALL'} ===")

            try:
                html = post_for_month(sess, cur_month, cur_year, start_day)
                links = extract_release_links(html)
                print("Found releases:", len(links))

                # Check English page to see if Hindi exists
                candidate_pairs = []
                for link in tqdm(links, desc="Checking Releases", leave=False, ncols=80):
                    try:
                        random_sleep()
                        r = sess.get(link, headers=HEADERS, timeout=30)
                        r.raise_for_status()
                        hindi_url = find_hindi_link_from_release_html(r.text)
                        if hindi_url:
                            candidate_pairs.append((link, hindi_url))
                    except:
                        continue

                print("Pairs eligible for extraction:", len(candidate_pairs))

                # Parallel crawl4ai extraction
                if candidate_pairs:
                    with Pool(workers) as p:
                        for res in tqdm(p.imap_unordered(worker_process, candidate_pairs),
                                        total=len(candidate_pairs),
                                        desc="Extracting",
                                        ncols=80):
                            if res:
                                writer.writerow(list(res))
                                saved_pairs += 1

                print("Saved so far:", saved_pairs)

            except Exception as e:
                print("Error:", e)

            # Next month
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


# -------------------------------------------------
# RUN
# -------------------------------------------------
if __name__ == "__main__":
    START_YEAR = 2025
    START_MONTH = 1

    # Set START_DAY to an integer (1–31) to test a single day.
    # Set START_DAY = None for all days.
    START_DAY = 4   # Example: scrape only 4th of the month
    # START_DAY = None

    WORKERS = 6

    iterate_by_month_parallel(
        START_YEAR,
        START_MONTH,
        START_DAY,
        output_csv="pib_bilingual.csv",
        workers=WORKERS
    )
