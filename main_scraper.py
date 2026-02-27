import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus, urljoin
import os
import time
import re
import json
import boto3 
import selenium_scraper
import subprocess
import sys
from ta_scraper import run_ta_scraper
from pathlib import Path
from difflib import SequenceMatcher
from supabase import create_client
from datetime import datetime, timezone, timedelta

SEEN_PARAGRAPHS = set()

def get_supabase_client():
    supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY")
        or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    )
    if not supabase_url or not supabase_key:
        return None
    return create_client(supabase_url, supabase_key)

def normalize_place_name(name):
    if not name:
        return ""
    cleaned = re.sub(r"[^a-z0-9]+", " ", name.lower()).strip()
    cleaned = re.sub(r"^(the|a|an)\s+", "", cleaned)
    cleaned = re.sub(r"^(st|saint)\s+", "", cleaned)
    return cleaned

def find_existing_place(destination):
    supabase = get_supabase_client()
    if not supabase:
        return None

    dest_clean = normalize_place_name(destination)
    if not dest_clean:
        return None

    dest_city = destination.split(",")[0].strip()
    city_query = normalize_place_name(dest_city)
    if not city_query:
        city_query = dest_clean

    try:
        res = supabase.table("place").select(
            "place_id, place_city, place_countryregion, place_stateprovince"
        ).ilike("place_city", f"%{city_query}%").limit(50).execute()
    except Exception as e:
        return None

    direction_tokens = {"north", "south", "east", "west", "n", "s", "e", "w"}
    dest_tokens = set(dest_clean.split())

    best = None
    best_score = 0.0
    for row in res.data or []:
        row_city = row.get("place_city") or ""
        row_norm = normalize_place_name(row_city)
        if not row_norm:
            continue

        row_tokens = set(row_norm.split())
        dest_dirs = dest_tokens.intersection(direction_tokens)
        row_dirs = row_tokens.intersection(direction_tokens)

        if dest_dirs != row_dirs:
            continue

        score = SequenceMatcher(None, dest_clean, row_norm).ratio()

        if dest_clean == row_norm:
            score = 1.0

        if score > best_score:
            best = row
            best_score = score

    if best and best_score >= 0.88:
        best["_match_score"] = round(best_score, 2)
        return best

    return None

def parse_timestamp(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        cleaned = str(value).strip()
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        return datetime.fromisoformat(cleaned)
    except Exception:
        return None

def should_refresh_destination(destination):
    existing = find_existing_place(destination)
    if not existing:
        return True

    supabase = get_supabase_client()
    if not supabase:
        return True

    place_id = existing.get("place_id")
    if not place_id:
        return True

    try:
        res = supabase.table("attraction").select(
            "attraction_lastrefreshed"
        ).eq("place_id", place_id).order(
            "attraction_lastrefreshed", desc=True
        ).limit(1).execute()
    except Exception as e:
        return True

    latest = None
    if res.data:
        latest = parse_timestamp(res.data[0].get("attraction_lastrefreshed"))

    if not latest:
        return True

    now = datetime.now(timezone.utc)
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)

    return (now - latest) > timedelta(days=90)

def get_s3_client():
    try:
        key = os.getenv('SCRAPER_AWS_ACCESS_KEY_ID')
        secret = os.getenv('SCRAPER_AWS_SECRET_ACCESS_KEY')
        print(f"[s3] key_prefix={key[:8] if key else 'MISSING'} | secret_present={secret is not None}")
        return boto3.client(
            's3',
            aws_access_key_id=key,
            aws_secret_access_key=secret
        )
    except Exception as e:
        print(f"[s3] client creation failed | error={e}")
        return None

def upload_to_s3(local_filepath, destination_name):
    s3 = get_s3_client()
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    if not s3 or not bucket_name:
        return

    timestamp = time.strftime('%Y-%m-%d')
    file_name = os.path.basename(local_filepath)
    s3_key = f"raw_scrapes/{file_name}" 

    try:
        s3.upload_file(local_filepath, bucket_name, s3_key)
    except Exception as e:
        pass

def process_html_content(raw_html):
    soup = BeautifulSoup(raw_html, 'html.parser')

    comment_selectors = [
        '#comments', '.comments-area', '.comment-list', 
        '#reviews', '.reviews-section', '#respond', 
        '.user-reviews', '.feedback-list',
        '[id*="comment"]', '[class*="comment-body"]'
    ]

    extracted_reviews = []
    for selector in comment_selectors:
        elements = soup.select(selector)
        for el in elements:
            text = el.get_text(separator="\n", strip=True)
            if len(text) > 50:
                extracted_reviews.append(text)
            el.decompose()

    reviews_text = "\n---\n".join(extracted_reviews)

    for element in soup(["script", "style", "header", "footer", "nav", "iframe", "noscript", "form", "aside", "button", "input"]):
        element.decompose()

    for h in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
        level = int(h.name[1])
        h.string = f"\n\n{'#' * level} {h.get_text().strip()}\n"

    for li in soup.find_all('li'):
        li.string = f"\n- {li.get_text().strip()}"

    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    
    final_chunks = []
    for chunk in chunks:
        if not chunk: continue
        if len(chunk) > 50 and chunk in SEEN_PARAGRAPHS:
            continue
        SEEN_PARAGRAPHS.add(chunk)
        final_chunks.append(chunk)

    body_text = '\n'.join(final_chunks)
    
    return body_text, reviews_text

def refine_text_content(text, title, destination):
    dest_lower = destination.lower()
    title_lower = title.lower()
    
    junk_patterns = [
        r"^home\s*/", r"^menu", r"^search", r"^skip to content",
        r"browse by destination", r"recent posts", r"table of contents",
        r"share\s+tweet", r"click to share", r"pin it",
        r"connect with", r"follow us", r"share this",
        r"copyright", r"all rights reserved", 
        r"affiliate links", r"commission", "sponsored content",
        r"advertisement", r"transparency note",
        r"read more", r"related posts", r"you may also like",
        r"check out this", r"read next",
        r"leave a comment", r"cancel reply", r"post comment",
        r"add a comment", r"reply to", r"posted by",
        r"star this", r"upvote", r"downvote", r"likes?",
        r"react to this", r"login or join",
        r"comments are closed", r"click on a star", 
        r"submit rating", r"submit feedback", 
        r"\d+\s*comments?", r"reply\s*$"
    ]
    junk_regex = re.compile('|'.join(junk_patterns), re.IGNORECASE)

    cleaned_lines = []
    for line in text.splitlines():
        line = line.strip()
        if not line: continue
        if junk_regex.search(line): continue
        if len(line) > 300 and line.count('.') < 2: continue 
        cleaned_lines.append(line)

    clean_text = '\n'.join(cleaned_lines)
    
    if dest_lower in title_lower:
        return clean_text

    term_count = clean_text.lower().count(dest_lower)
    if term_count < 3:
        return None

    return clean_text

def extract_image_candidates(raw_html, base_url, max_images=8):
    soup = BeautifulSoup(raw_html, 'html.parser')
    candidates = []
    seen = set()

    def add_candidate(url, alt_text):
        if not url:
            return
        url = str(url).strip()
        if not url or url.startswith("data:"):
            return
        if url.startswith("//"):
            url = "https:" + url
        if not url.startswith("http"):
            url = urljoin(base_url, url)
        url_key = url.split("?")[0].strip()
        if not url_key or url_key in seen:
            return
        lower = url_key.lower()
        if any(token in lower for token in ("sprite", "icon", "logo", "avatar", "placeholder", "blank")):
            return
        if lower.endswith(".svg"):
            return
        seen.add(url_key)
        candidates.append({
            "url": url,
            "alt": (alt_text or "").strip()[:200]
        })

    for meta_key in ("og:image", "twitter:image"):
        tag = soup.find("meta", attrs={"property": meta_key}) or soup.find("meta", attrs={"name": meta_key})
        if tag and tag.get("content"):
            add_candidate(tag.get("content"), "")

    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or img.get("data-original")
        alt = img.get("alt") or img.get("title") or ""
        add_candidate(src, alt)
        if len(candidates) >= max_images:
            break

    return candidates

def scrape_and_crawl(destination):
    print(f"[main] scrape_and_crawl start | destination={destination}")
    search_term = quote_plus(destination)

    sites = {
        "Rick Steves": f"https://search.ricksteves.com/?query={search_term}",
        "My Family Travels": f"https://myfamilytravels.com/?s={search_term}",
        "This Rare Earth": f"https://thisrareearth.com/?s={search_term}",
        "My Global Viewpoint": f"https://www.myglobalviewpoint.com/?s={search_term}",
        "Nomadic Matt": f"https://www.nomadicmatt.com/?s={search_term}",
        "The Blonde Abroad": f"https://www.theblondeabroad.com/?s={search_term}"
    }
    
    selenium_only_sites = {"Reddit"}
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    all_results = []

    for site_name, url in sites.items():
        site_data = []
        try:
            time.sleep(1)
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                if site_name == "Rick Steves":
                    cards = soup.select("a.search-result")[:5]
                    for card in cards:
                        site_data.append({'Site': site_name, 'Title': card.select_one("h2").get_text(strip=True), 'Link': card.get('href')})
                else:
                    cards = soup.select("article")[:5] or soup.select("div.post")[:5] or soup.select("div.repeater-item")[:5]
                    for card in cards:
                        title = card.select_one("h2 a") or card.select_one("h3 a") or card.select_one("a.page-article__link")
                        if title: site_data.append({'Site': site_name, 'Title': title.get_text(strip=True), 'Link': title['href']})
        except: pass
        
        if not site_data:
            print(f"[main] selenium fallback | site={site_name} | destination={destination}")
            site_data = selenium_scraper.scrape_links_selenium(site_name, destination)
        print(f"[main] site results | site={site_name} | count={len(site_data)}")
        
        all_results.extend(site_data)
    
    print(f"[main] selenium reddit start | destination={destination}")
    reddit_data = selenium_scraper.scrape_links_selenium("Reddit", destination)
    print(f"[main] selenium reddit done | destination={destination} | count={len(reddit_data)}")
    if reddit_data:
        for item in reddit_data:
            pass
    all_results.extend(reddit_data)

    print(f"[main] aggregated links | destination={destination} | total_links={len(all_results)}")
    if not all_results:
        print(f"[main] no links found | destination={destination}")
        return

    driver = selenium_scraper.get_driver()
    if not driver:
        print(f"[main] article crawl skipped (no selenium driver) | destination={destination}")
        return

    entries = []
    for i, item in enumerate(all_results, 1):
        url = item['Link']
        
        try:
            driver.get(url)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            page_source = driver.page_source
            body_text, reviews_text = process_html_content(page_source)
            image_candidates = extract_image_candidates(page_source, url)
            final_body = refine_text_content(body_text, item['Title'], destination)
            
            if final_body:
                entry = {
                    "source": item['Site'],
                    "title": item['Title'],
                    "url": url,
                    "type": "web_article",
                    "scraped_at": time.strftime('%Y-%m-%d'),
                    "content_body": final_body,
                    "user_reviews": reviews_text,
                    "has_reviews": bool(reviews_text.strip()),
                    "image_candidates": image_candidates
                }
                entries.append(entry)
                
        except Exception as e:
            pass

    driver.quit()
    print(f"[main] article crawl done | destination={destination} | entries={len(entries)}")
    
    if entries:
        s3 = get_s3_client()
        bucket_name = os.getenv('S3_BUCKET_NAME')
        if s3 and bucket_name:
            stamp = time.strftime('%Y%m%d_%H%M%S')
            file_slug = destination.replace(' ', '_').lower()
            s3_key = f"raw_scrapes/{file_slug}_{stamp}.jsonl"
            payload = "\n".join(json.dumps(entry, ensure_ascii=False) for entry in entries) + "\n"
            try:
                s3.put_object(Bucket=bucket_name, Key=s3_key, Body=payload.encode("utf-8"))
                print(f"[main] s3 write success | key={s3_key} | entries={len(entries)}")
            except Exception as e:
                print(f"[main] s3 write failed | key={s3_key} | error={e}")

def run_tripadvisor_scraper(destination):
    run_ta_scraper(destination)

def run_ai_processor():
    ai_path = Path(__file__).resolve().parent / "ai_processor.py"
    if not ai_path.exists():
        return None
    return subprocess.Popen([sys.executable, str(ai_path)])

def run_google_reviews_enrichment():
    google_path = Path(__file__).resolve().parent / "google_reviews_api.py"
    if not google_path.exists():
        return None
    return subprocess.Popen([sys.executable, str(google_path)])

def run_post_tripadvisor_processors():
    procs = []

    ai_proc = run_ai_processor()
    if ai_proc is not None:
        procs.append(("AI processor", ai_proc))

    google_proc = run_google_reviews_enrichment()
    if google_proc is not None:
        procs.append(("Google reviews", google_proc))

    if not procs:
        return

    for label, proc in procs:
        proc.wait()

def run_main_scraper(dest):
    refresh_needed = should_refresh_destination(dest)
    force_blog_scrape = os.getenv("FORCE_BLOG_SCRAPE", "1").lower() in ("1", "true", "yes")
    print(f"[main] refresh decision | destination={dest} | should_refresh={refresh_needed}")

    if refresh_needed or force_blog_scrape:
        print(f"[main] blog scrape enabled | destination={dest} | force_blog_scrape={force_blog_scrape}")
        scrape_and_crawl(dest)
    else:
        print(f"[main] skipped blog scrape due to freshness window | destination={dest}")

    if refresh_needed:
        run_tripadvisor_scraper(dest)
        run_post_tripadvisor_processors()
    else:
        print(f"[main] skipped tripadvisor/ai/google due to freshness window | destination={dest}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_main_scraper(sys.argv[1])
    else:
        pass