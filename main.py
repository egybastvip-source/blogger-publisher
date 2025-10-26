#!/usr/bin/env python3
# auto_publisher_with_file_queue_static.py
# Modified to publish static HTML files into a local git repo (for GitHub Pages).
# Replace previous Blogger-API based create_post_and_patch with static writer + git push.

import os
import re
import time
import json
import sqlite3
import logging
import random
import tempfile
import subprocess
import shutil
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Iterable
from urllib.parse import urlparse
from pathlib import Path
from html import escape

import requests
from jinja2 import Template
from unidecode import unidecode

# ---------------------------
# CONFIG
# ---------------------------
TMDB_API_KEY = os.environ.get('TMDB_API_KEY', '61f4e46d715d5298af1bd36249818283')
# previous Blogger variables removed - using static site instead
# Local site repo directory where HTML files will be written and git push will be executed
SITE_DIR = os.environ.get('SITE_DIR', 'site')  # <-- set this to your local repo path
GITHUB_PAGES_URL = os.environ.get('GITHUB_PAGES_URL', '')  # e.g. https://username.github.io/repo (optional)
AUTHOR_NAME = os.environ.get('AUTHOR_NAME', 'AutoPublisher')

# DB paths
DB_PATH = os.environ.get('DB_PATH', 'published_items.db')        # table 'published' (this script)
IMDB_DB_PATH = os.environ.get('IMDB_DB_PATH', 'imdb_queue.db')   # bootstrap/updater DB with table 'imdb_queue' (status field)

IMDB_FILE = os.environ.get('IMDB_FILE', 'imdb_ids.txt')  # file produced by bootstrap_imdb_list.py

# Rate bounds (in seconds) for random sleep after successful publish
RATE_MIN = int(os.environ.get('RATE_MIN', '130'))
RATE_MAX = int(os.environ.get('RATE_MAX', '260'))

# processing
CHUNK_SIZE = int(os.environ.get('CHUNK_SIZE', '200'))

# Template (kept as provided)
HTML_TEMPLATE = r"""
<div class="egy-single-post">
    <div class="egy-post-top">
        <div class="egy-post-right">
            <img src="{{ image_url }}" width="100%" height="100%" >
            <a href="https://www.effectivegatecpm.com/zrvavtngbz?key=30616f742797c861a0b30871c8d155c4" class="egy-post-watch"><i class="fa fa-cloud-download"></i> تحميل مباشر</a>
        </div>
        <div class="egy-post-left">
            <h1 class="main-title">مشاهده {{ content_type }} {{ name }} {{ year }} مترجم</h1>
            <ul>
                <li>
                    <label>التصنيف</label>
                    <div>
                        {{ content_type }}
                    </div>
                </li>
                <li>
                    <label>سنة الاصدار</label>
                    <div>
                        {{ year }}
                    </div>
                </li>
                <li>
                    <label>الجودة</label>
                    <div>
                        FHD
                    </div>
                </li>
                <li>
                    <label>البلد</label>
                    <div>
                        {{ country }}
                    </div>
                </li>
                <li>
                    <label>اللغة</label>
                    <div>
                        {{ lang }}
                    </div>
                </li>
                <li>
                    <label>النوع</label>
                    <div>
                        {{ category }}
                    </div>
                </li>
                <li>
                    <label>الدقة</label>
                    <div>
                        1080p , 720p , 360p
                    </div>
                </li>
                <li>
                    <label>التقييم العالمي</label>
                    <div>
                        <span><i class="fa fa-star"></i>{{ imdb_rating }}</span>
                    </div>
                </li>
                <li>
                    <label>تاريخ الاصدار</label>
                    <div>
                        <span>{{ release_date }}</span>
                    </div>
                </li>
                <li>
                    <label>مدة العرض</label>
                    <div>
                        <span>{{ show_time }}</span>
                    </div>
                </li>
            </ul>
        </div>
    </div>
    <div class="egy-story">
        <h2 class="egy-story-title">القصة</h2>
        <p>{{ story }}</p>
    </div>
    <div class="egy-server">
        <h2 class="egy-server-title">سيرفرات المشاهدة - <span style="color:red;">لا تنسى تفعيل الترجمة و تخصيصها كما تريد</span></h2>
        <div class="egy-server-content">
            <div class="egy-server-list">
                <button class="tablinks active" onclick="openCity(event, 'server01')">VidSrc v1</button>
            </div>
            <div class="egy-server-container">
                <div class="tabcontent" id="server01" style="display: block;">
                    <iframe src="{{ embed_server1 }}" scrolling="no" frameborder="0" width="100%" height="100%" allowfullscreen="true" webkitallowfullscreen="true" mozallowfullscreen="true" referrerpolicy="no-referrer"></iframe>
                </div>
            </div>
        </div>
    </div>

{% if episodes_html %}
<div class="egy-episodes-box-full">
    <h3>اختر الموسم والحلقة</h3>

    {{ episodes_html | safe }}
</div>
{% endif %}

<div class="egy-related-ss-box">
    <h3>إذا كنت تبحث عن</h3>
    <div class="ss">
        {% for s in search_spans %}
        <span>{{ s }}</span>
        {% endfor %}
    </div>
</div>
"""

# ---------------------------
# DB
# ---------------------------
CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS published (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    imdb_id TEXT NOT NULL,
    content_type TEXT NOT NULL, -- movie or tv
    name TEXT,
    year TEXT,
    season INTEGER,
    episode INTEGER,
    blog_post_id TEXT,
    url TEXT,
    date_added TEXT
);
'''

# ---------------------------
# Helpers & logging
# ---------------------------
logging.basicConfig(level=logging.INFO)

# ---------- NEW: counter for actual successful posts in current cycle ----------
PUBLISHED_THIS_CYCLE = 0

# ---------------------------
# File-based queue helpers
# ---------------------------
IMDB_REGEX = re.compile(r'^(tt\d{6,})$', re.IGNORECASE)

def ensure_file_exists(path: str):
    if not os.path.exists(path):
        open(path, 'a', encoding='utf-8').close()

def load_imdb_ids_from_txt(path: str = IMDB_FILE) -> List[str]:
    ensure_file_exists(path)
    ids = []
    seen = set()
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            m = IMDB_REGEX.match(s)
            if not m:
                continue
            iid = m.group(1)
            if iid not in seen:
                seen.add(iid)
                ids.append(iid)
    return ids

def append_imdb_ids_to_txt(new_ids: Iterable[str], path: str = IMDB_FILE) -> int:
    ensure_file_exists(path)
    existing = set(load_imdb_ids_from_txt(path))
    to_add = []
    for i in new_ids:
        if not i:
            continue
        m = IMDB_REGEX.match(i.strip())
        if not m:
            continue
        iid = m.group(1)
        if iid not in existing:
            existing.add(iid)
            to_add.append(iid)
    if not to_add:
        return 0
    with open(path, 'a', encoding='utf-8') as f:
        for iid in to_add:
            f.write(iid + '\n')
    return len(to_add)

def remove_imdb_ids_from_txt(remove_ids: Iterable[str], path: str = IMDB_FILE) -> int:
    remove_set = {i for i in (remove_ids or []) if i}
    if not remove_set:
        return 0
    ensure_file_exists(path)
    removed = 0
    target_dir = os.path.dirname(os.path.abspath(path)) or '.'
    fd, tmp_path = tempfile.mkstemp(prefix='imdb_tmp_', dir=target_dir, text=True)
    os.close(fd)
    try:
        with open(path, 'r', encoding='utf-8') as inf, open(tmp_path, 'w', encoding='utf-8') as outf:
            for line in inf:
                s = line.strip()
                if not s:
                    continue
                m = IMDB_REGEX.match(s)
                if not m:
                    outf.write(line)
                    continue
                iid = m.group(1)
                if iid in remove_set:
                    removed += 1
                else:
                    outf.write(iid + '\n')
        try:
            os.replace(tmp_path, path)
        except OSError:
            shutil.move(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
    return removed

def iter_imdb_queue(path: str = IMDB_FILE, chunk_size: int = CHUNK_SIZE):
    ids = load_imdb_ids_from_txt(path)
    for i in range(0, len(ids), chunk_size):
        yield ids[i:i+chunk_size]

# ---------------------------
# DB helpers
# ---------------------------
def init_db(path: str = DB_PATH):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.executescript(CREATE_TABLE_SQL)
    conn.commit()
    conn.close()

def db_has(imdb_id: str, season: Optional[int] = None, episode: Optional[int] = None) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        if season is None and episode is None:
            cur.execute('SELECT 1 FROM published WHERE imdb_id=? AND season IS NULL AND episode IS NULL', (imdb_id,))
        elif season is not None and episode is not None:
            cur.execute('SELECT 1 FROM published WHERE imdb_id=? AND season=? AND episode=?', (imdb_id, season, episode))
        elif season is not None and episode is None:
            cur.execute('SELECT 1 FROM published WHERE imdb_id=? AND season=?', (imdb_id, season))
        else:
            cur.execute('SELECT 1 FROM published WHERE imdb_id=?', (imdb_id,))
        r = cur.fetchone()
        return r is not None
    finally:
        conn.close()

def db_insert(record: Dict[str, Any]):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO published (imdb_id, content_type, name, year, season, episode, blog_post_id, url, date_added)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        record.get('imdb_id'), record.get('content_type'), record.get('name'), record.get('year'),
        record.get('season'), record.get('episode'), record.get('blog_post_id'), record.get('url'),
        datetime.now(timezone.utc).isoformat()
    ))
    conn.commit()
    conn.close()

# ---------------------------
# mark imdb_queue status as published (if imdb_queue DB exists)
# ---------------------------
def mark_imdb_published(imdb_id: str, imdb_db_path: str = IMDB_DB_PATH) -> bool:
    try:
        if not os.path.exists(imdb_db_path):
            logging.debug('IMDB DB path %s does not exist; skip mark_imdb_published for %s', imdb_db_path, imdb_id)
            return False
        conn = sqlite3.connect(imdb_db_path)
        cur = conn.cursor()
        try:
            cur.execute("PRAGMA table_info(imdb_queue)")
            cols = [r[1] for r in cur.fetchall()]
            if 'status' not in cols:
                try:
                    cur.execute("ALTER TABLE imdb_queue ADD COLUMN status TEXT")
                except Exception:
                    pass
            if 'published_at' not in cols:
                try:
                    cur.execute("ALTER TABLE imdb_queue ADD COLUMN published_at TEXT")
                except Exception:
                    pass
        except Exception:
            pass
        now = datetime.now(timezone.utc).isoformat()
        cur.execute("UPDATE imdb_queue SET status=?, published_at=? WHERE imdb_id=?", ('published', now, imdb_id))
        conn.commit()
        updated = cur.rowcount if hasattr(cur, 'rowcount') else 0
        conn.close()
        if updated:
            logging.info('Marked imdb_queue %s as published in %s', imdb_id, imdb_db_path)
            return True
        else:
            return False
    except Exception:
        logging.exception('Failed to mark imdb_queue %s as published', imdb_id)
        return False

def imdb_queue_is_published(imdb_id: str, imdb_db_path: str = IMDB_DB_PATH) -> bool:
    try:
        if not os.path.exists(imdb_db_path):
            return False
        conn = sqlite3.connect(imdb_db_path)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='imdb_queue'")
        if not cur.fetchone():
            conn.close()
            return False
        cur.execute("PRAGMA table_info(imdb_queue)")
        cols = [r[1] for r in cur.fetchall()]
        if 'status' in cols:
            cur.execute("SELECT status FROM imdb_queue WHERE imdb_id=? LIMIT 1", (imdb_id,))
            row = cur.fetchone()
            conn.close()
            if row and (row[0] or '').lower() == 'published':
                return True
            return False
        else:
            conn.close()
            return False
    except Exception:
        return False

# ---------------------------
# Utility helpers
# ---------------------------
def slugify(text: str) -> str:
    text = (text or '').strip()
    text = unidecode(text)
    text = re.sub(r'\s+', '-', text)
    text = re.sub(r'[^a-zA-Z0-9\-]', '', text)
    text = re.sub(r'-{2,}', '-', text)
    return text.strip('-').lower()

# ---------------------------
# TMDB helpers
# ---------------------------
TMDB_BASE = 'https://api.themoviedb.org/3'

def tmdb_find_by_imdb(imdb_id: str) -> Optional[Dict[str, Any]]:
    url = f"{TMDB_BASE}/find/{imdb_id}"
    params = {'api_key': TMDB_API_KEY, 'external_source': 'imdb_id'}
    r = requests.get(url, params=params, timeout=20)
    if r.status_code != 200:
        logging.error('TMDB find failed: %s %s', r.status_code, r.text)
        return None
    return r.json()

def tmdb_get_detail(kind: str, tmdb_id: int, lang: str = 'en') -> Optional[Dict[str, Any]]:
    url = f"{TMDB_BASE}/{kind}/{tmdb_id}"
    params = {'api_key': TMDB_API_KEY, 'language': lang, 'append_to_response': 'credits,seasons'}
    r = requests.get(url, params=params, timeout=20)
    if r.status_code == 200:
        return r.json()
    logging.warning('TMDB detail %s %s lang=%s failed: %s', kind, tmdb_id, lang, r.status_code)
    return None

# ---------------------------
# Render helpers (unchanged)
# ---------------------------
def build_episodes_html(name_en: str, year: str, seasons: List[Dict[str, Any]], tmdb_id: Optional[int] = None, date_prefix: str = '') -> str:
    base_slug_episode_name = slugify(name_en or '')
    parts = []
    if date_prefix:
        date_prefix = date_prefix.rstrip('/')
    for s in (seasons or []):
        sn = s.get('season_number')
        if sn is None or sn == 0:
            continue
        episode_count = s.get('episode_count')
        if (not episode_count or episode_count == 0) and tmdb_id:
            try:
                resp = requests.get(f"{TMDB_BASE}/tv/{tmdb_id}/season/{sn}", params={'api_key': TMDB_API_KEY}, timeout=15)
                if resp.status_code == 200:
                    sec = resp.json()
                    episode_count = len(sec.get('episodes', []))
            except Exception:
                episode_count = 0
        episode_count = episode_count or 0
        details = ['<details class="season">', f'<summary>الموسم {sn}</summary>', '<div class="episodes">']
        for ep in range(1, episode_count + 1):
            ep_slug = f"{base_slug_episode_name}-{sn}-{ep}"
            link = f"{date_prefix}/{ep_slug}.html" if date_prefix else f"/{ep_slug}.html"
            details.append(f'<a href="{link}">الحلقة {ep}</a>')
        details.append('</div></details>')
        parts.append('\n'.join(details))
    return '\n'.join(parts)

def build_search_spans(name: str, year: str, is_tv: bool) -> List[str]:
    arr = []
    typ = 'مسلسل' if is_tv else 'فيلم'
    name_with_year = f"{name} {year}" if year else name
    arr.extend([
        f'مشاهدة {typ} {name_with_year}',
        f'تحميل {typ} {name_with_year}',
        f'مشاهدة {typ} {name_with_year} مترجم',
        f'مشاهدة {typ} {name_with_year} أون لاين',
        f'تحميل {typ} {name_with_year} HD',
        f'أفضل {typ} {name_with_year}',
        f'قصة {typ} {name_with_year}',
        f'تقييم {typ} {name_with_year}',
        f'مشاهدة {typ} {name_with_year} كامل',
        f'تحميل {typ} {name_with_year} تورنت',
        f'مشاهدة {typ} {name_with_year} بجودة عالية',
        f'{name_with_year} مشاهده و تنزيل'
    ])
    arr += ['egybest, egybast, egybst', 'ايجي بيست', 'ايجي بست']
    return arr

def generate_labels(kind: str, data: Dict[str, Any], season: Optional[int] = None, episode: Optional[int] = None) -> List[str]:
    labels: List[str] = []
    base_label = 'movies' if kind == 'movie' else 'series'
    labels.append(base_label)
    lang = (data.get('original_language') or '').lower()
    if lang == 'en':
        labels.append('en')

    genre_map = {
        'Action': 'action', 'Horror': 'horror', 'Animation': 'cartoon', 'Science Fiction': 'sci-fi',
        'Adventure': 'adventure', 'Drama': 'drama', 'Fantasy': 'fantasy', 'Comedy': 'comedy',
        'Romance': 'romance', 'Thriller': 'thriller', 'Mystery': 'mystery', 'Documentary': 'documentary',
        'Family': 'family', 'Crime': 'crime', 'Western': 'western', 'Music': 'music', 'TV Movie': 'tv-movie'
    }
    for g in data.get('genres') or []:
        gname = (g.get('name') or '').strip()
        if not gname:
            continue
        mapped = genre_map.get(gname)
        if mapped:
            if mapped not in labels:
                labels.append(mapped)
        else:
            slugged = slugify(gname)
            if slugged and slugged not in labels:
                labels.append(slugged)

    if lang in ['ja', 'ko', 'zh', 'hi', 'th', 'vi', 'bn', 'ta'] and 'asian' not in labels:
        labels.append('asian')
    genres_names = [g.get('name', '').lower() for g in (data.get('genres') or [])]
    if any('anime' in gn for gn in genres_names) or 'anime' in (data.get('original_name') or '').lower() or 'anime' in (data.get('name') or '').lower():
        if 'anime' not in labels:
            labels.append('anime')

    try:
        vote_avg = float(data.get('vote_average') or 0)
    except Exception:
        vote_avg = 0.0
    try:
        vote_count = int(data.get('vote_count') or 0)
    except Exception:
        vote_count = 0
    try:
        popularity = float(data.get('popularity') or 0)
    except Exception:
        popularity = 0.0

    if vote_count > 2000 or popularity > 80:
        if 'famous' not in labels:
            labels.append('famous')

    if vote_avg >= 8.0:
        best_label = 'best-movies' if kind == 'movie' else 'best-series'
        if best_label not in labels:
            labels.append(best_label)

    rel = (data.get('release_date') or data.get('first_air_date') or '')
    if rel:
        try:
            rel_date = datetime.fromisoformat(rel)
            if (datetime.now(timezone.utc) - rel_date).days <= 365:
                new_label = 'new-movies' if kind == 'movie' else 'new-series'
                if new_label not in labels:
                    labels.append(new_label)
        except Exception:
            pass

    final: List[str] = []
    for l in labels:
        if not l:
            continue
        low = l.lower()
        if low not in final:
            final.append(low)
    return final

# ---------------------------
# Static site writer (replacement for Blogger API)
# ---------------------------
def _ensure_site_dirs(post_path: Path):
    post_path.parent.mkdir(parents=True, exist_ok=True)

def _git_commit_and_push(repo_path: str, message: str) -> bool:
    """Run git add/commit/push. Return True on success."""
    try:
        subprocess.check_call(['git', 'add', '-A'], cwd=repo_path)
        try:
            subprocess.check_call(['git', 'commit', '-m', message], cwd=repo_path)
        except subprocess.CalledProcessError as e:
            # commit returns non-zero if no changes -> ignore
            logging.debug('git commit returned non-zero (maybe no changes): %s', e)
        subprocess.check_call(['git', 'push'], cwd=repo_path)
        return True
    except Exception:
        logging.exception('Git commit/push failed for repo %s', repo_path)
        return False

def _render_full_html(title: str, description: str, content_html: str, labels: Optional[List[str]] = None, schema_json: Optional[str] = None):
    labels_meta = ','.join(labels or [])
    head = f"""<!doctype html>
<html lang="ar">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{escape(title)}</title>
<meta name="description" content="{escape(description or '')}">
<meta name="keywords" content="{escape(labels_meta)}">
"""
    if schema_json:
        head += f"<script type='application/ld+json'>{schema_json}</script>\n"
    head += "</head>\n<body>\n"
    tail = "\n</body>\n</html>"
    return head + content_html + tail

def create_post_and_patch(service_unused, blog_id: str, temp_title: str, final_title: str, content_html: str, labels: Optional[List[str]] = None, description: Optional[str] = None):
    """
    Replacement for Blogger API:
    - Writes static file to SITE_DIR/YYYY/MM/slug.html
    - Commits and pushes to git repo at SITE_DIR
    - Returns (post_id, post_url) where post_id is slug and post_url constructed from GITHUB_PAGES_URL (if set)
    """
    try:
        now = datetime.now(timezone.utc)
        year = f"{now.year:04d}"
        month = f"{now.month:02d}"

        slug = (temp_title or '').strip()
        if not slug:
            slug = slugify(final_title) or f"post-{int(now.timestamp())}"

        filename = f"{slug}.html"
        post_dir = Path(SITE_DIR) / year / month
        post_path = post_dir / filename

        _ensure_site_dirs(post_path)

        # if JSON-LD already prefixed inside content_html, we don't need to pass schema_json
        full_html = _render_full_html(final_title, description or '', content_html, labels=labels, schema_json=None)

        tmp_path = post_path.with_suffix('.tmp')
        with open(tmp_path, 'w', encoding='utf-8') as f:
            f.write(full_html)
        tmp_path.replace(post_path)

        commit_msg = f"Add post {slug} ({year}/{month}) by {AUTHOR_NAME}"
        ok = _git_commit_and_push(SITE_DIR, commit_msg)

        post_id = slug
        if GITHUB_PAGES_URL:
            post_url = f"{GITHUB_PAGES_URL.rstrip('/')}/{year}/{month}/{filename}"
        else:
            post_url = str(post_path.resolve())

        try:
            global PUBLISHED_THIS_CYCLE
            PUBLISHED_THIS_CYCLE += 1
            logging.info('Incremented published counter -> %s', PUBLISHED_THIS_CYCLE)
        except Exception:
            logging.exception('Failed to increment published counter.')

        logging.info('Created static post: %s -> %s (git_push=%s)', slug, post_url, ok)
        return post_id, post_url
    except Exception:
        logging.exception('Failed to create static post for %s', final_title)
        return None, None

def random_sleep_after_publish():
    sleep_for = random.randint(RATE_MIN, RATE_MAX)
    logging.info('Sleeping %s seconds before next publish...', sleep_for)
    time.sleep(sleep_for)

# ---------------------------
# JSON-LD builder (unchanged)
# ---------------------------
def build_jsonld_schema(kind: str, data: Dict[str, Any], imdb_id: str, season: Optional[int] = None, episode: Optional[int] = None):
    type_name = "Movie" if kind == "movie" else "TVSeries"
    name = (data.get('title') or data.get('name') or data.get('original_title') or data.get('original_name') or '')
    description = data.get('overview') or ''
    poster_path = data.get('poster_path') or ''
    image_full = f"https://image.tmdb.org/t/p/w780{poster_path}" if poster_path else ''
    date_published = data.get('release_date') or data.get('first_air_date') or ''
    try:
        rating_value = float(data.get('vote_average')) if data.get('vote_average') not in (None, '') else None
    except Exception:
        rating_value = None
    try:
        raw_count = data.get('vote_count')
        if raw_count is None or raw_count == '':
            rating_count = 0
        else:
            if isinstance(raw_count, str):
                digits = ''.join(ch for ch in raw_count if ch.isdigit())
                rating_count = int(digits) if digits else 0
            else:
                rating_count = int(raw_count)
    except Exception:
        rating_count = 0
    best_rating = 10
    worst_rating = 0
    director_name = None
    credits = data.get('credits') or {}
    crew = credits.get('crew') or []
    for member in crew:
        job = (member.get('job') or '').lower()
        if job == 'director':
            director_name = member.get('name')
            break
    if not director_name and kind == 'tv':
        creators = data.get('created_by') or []
        if creators:
            director_name = creators[0].get('name')
    schema = {
        "@context": "https://schema.org",
        "@type": type_name,
        "name": name,
        "description": description,
        "image": image_full,
        "datePublished": date_published,
        "aggregateRating": {
            "@type": "AggregateRating",
            "ratingValue": rating_value if rating_value is not None else 0,
            "ratingCount": rating_count,
            "bestRating": best_rating,
            "worstRating": worst_rating
        },
        "identifier": {
            "@type": "PropertyValue",
            "propertyID": "IMDb",
            "value": imdb_id
        }
    }
    if director_name:
        schema["director"] = {
            "@type": "Person",
            "name": director_name
        }
    return schema

# ---------------------------
# Publishing helpers (unchanged)
# ---------------------------
def publish_missing_episodes(imdb_id: str, tmdb_id: int, name_use: str, year: str, seasons_list: List[Dict[str, Any]], data_ar: Dict[str, Any], data_en: Dict[str, Any], root_date_prefix: str, service):
    logging.info('Publishing missing episodes for %s', imdb_id)
    for s in seasons_list:
        sn = s.get('season_number')
        if sn is None or sn == 0:
            continue
        ep_count = s.get('episode_count') or 0
        if ep_count == 0 and tmdb_id:
            try:
                resp = requests.get(f"{TMDB_BASE}/tv/{tmdb_id}/season/{sn}", params={'api_key': TMDB_API_KEY}, timeout=15)
                if resp.status_code == 200:
                    sec = resp.json()
                    ep_count = len(sec.get('episodes', []))
            except Exception:
                logging.exception('Failed to fetch season detail for %s season %s', imdb_id, sn)
                ep_count = 0
        logging.info('Season %s has %s episodes (imdb=%s)', sn, ep_count, imdb_id)
        for ep in range(1, ep_count + 1):
            if db_has(imdb_id, sn, ep):
                continue
            image_url = 'https://image.tmdb.org/t/p/w780' + ((data_en.get('poster_path') or '') or '')
            embed_server1 = f'https://vidsrc.xyz/embed/tv/{imdb_id}/{sn}/{ep}'
            embed_server2 = f'https://vidsrc.to/embed/tv/{imdb_id}/{sn}/{ep}'
            episodes_html = build_episodes_html(name_use, year, seasons_list, tmdb_id, date_prefix=root_date_prefix)
            context = {
                'image_url': image_url,
                'name': name_use,
                'year': year,
                'content_type': 'مسلسل',
                'country': (data_ar.get('production_countries') or [{}])[0].get('name', ''),
                'lang': data_en.get('original_language', ''),
                'category': ', '.join([g.get('name') for g in (data_ar.get('genres') or [])]),
                'imdb_rating': data_en.get('vote_average') or data_ar.get('vote_average') or '',
                'release_date': data_ar.get('release_date') or data_ar.get('first_air_date') or '',
                'show_time': data_ar.get('runtime') or (data_ar.get('episode_run_time')[0] if data_ar.get('episode_run_time') else ''),
                'story': data_ar.get('overview') or data_en.get('overview') or '',
                'embed_server1': embed_server1,
                'embed_server2': embed_server2,
                'episodes_html': episodes_html,
                'search_spans': build_search_spans(name_use, year, True)
            }
            html_content = Template(HTML_TEMPLATE).render(**context)
            try:
                schema = build_jsonld_schema('tv', data_en or data_ar, imdb_id, sn, ep)
            except Exception:
                schema = None
            content_with_schema = (f"<script type='application/ld+json'>{json.dumps(schema, ensure_ascii=False)}</script>\n" if schema else "") + html_content
            ep_slug = f"{slugify(name_use)}-{sn}-{ep}"
            temp_title = ep_slug
            final_title = f"مشاهده مسلسل {name_use} الموسم {sn} الحلقه {ep} مترجم - ايجی بست"
            description = f"مشاهده و تنزيل مسلسل {name_use} الموسم {sn} الحلقه {ep}"
            labels = generate_labels('tv', data_ar or data_en, sn, ep)
            labels = [l for l in labels if isinstance(l, str) and l.strip()]
            ordered = []
            if 'en' in labels:
                ordered.append('en'); labels.remove('en')
            base = 'series'
            if base in labels:
                ordered.append(base); labels.remove(base)
            ordered.extend(labels)
            ordered.append(random.choice(['hd', 'hdtv']))
            labels = ordered
            try:
                post_id, post_url = create_post_and_patch(None, '', temp_title, final_title, content_with_schema, labels, description)
                db_insert({
                    'imdb_id': imdb_id,
                    'content_type': 'tv',
                    'name': name_use,
                    'year': year,
                    'season': sn,
                    'episode': ep,
                    'blog_post_id': post_id,
                    'url': post_url
                })
                mark_imdb_published(imdb_id)
                try:
                    removed = remove_imdb_ids_from_txt([imdb_id], IMDB_FILE)
                    if removed:
                        logging.info('Removed %s from file after episode publish (removed=%s)', imdb_id, removed)
                except Exception:
                    logging.exception('Failed to remove imdb id from file after episode publish %s', imdb_id)
                logging.info('Published episode S%sE%s -> %s', sn, ep, post_url)
                random_sleep_after_publish()
            except Exception:
                logging.exception('Failed to create episode post for %s S%sE%s', imdb_id, sn, ep)

def publish_imdb_item(imdb_id: str, season: Optional[int] = None, episode: Optional[int] = None, is_dry_run: bool = False):
    logging.info('Processing %s (s=%s e=%s)', imdb_id, season, episode)
    if season is None and episode is None and db_has(imdb_id, None, None):
        logging.info('Already published (movie or tv root). Fast skip.')
        return None
    found = tmdb_find_by_imdb(imdb_id)
    if not found:
        logging.error('Not found on TMDB for %s', imdb_id)
        return None
    if found.get('movie_results'):
        kind = 'movie'
        tmdb_item = found['movie_results'][0]
        tmdb_id = tmdb_item.get('id')
    elif found.get('tv_results'):
        kind = 'tv'
        tmdb_item = found['tv_results'][0]
        tmdb_id = tmdb_item.get('id')
    else:
        logging.error('TMDB returned no movie or tv for %s', imdb_id)
        return None
    data_ar = tmdb_get_detail(kind, tmdb_id, lang='ar') or {}
    data_en = tmdb_get_detail(kind, tmdb_id, lang='en') or {}
    name_en = (data_en.get('title') or data_en.get('name') or data_en.get('original_title') or data_en.get('original_name') or '').strip()
    name_use = name_en or imdb_id
    year = (data_ar.get('release_date') or data_ar.get('first_air_date') or '')[:4]
    is_tv = (kind == 'tv')
    poster_path = data_en.get('poster_path') or ''
    root_slug = slugify(f"{name_use}{year}") if name_use else slugify(imdb_id)
    seasons_list = data_ar.get('seasons') or data_en.get('seasons') or []
    logging.info('Seasons from TMDB for %s: %s', imdb_id, seasons_list)
    embed_server_movie_1 = f'https://vidsrc.xyz/embed/movie/{imdb_id}'
    embed_server_movie_2 = f'https://vidsrc.to/embed/movie/{imdb_id}'
    context_base = {
        'image_url': 'https://image.tmdb.org/t/p/w780' + (poster_path or ''),
        'name': name_use,
        'year': year,
        'content_type': 'مسلسل' if is_tv else 'فيلم',
        'country': (data_ar.get('production_countries') or [{}])[0].get('name', ''),
        'lang': data_en.get('original_language', ''),
        'category': ', '.join([g.get('name') for g in (data_ar.get('genres') or [])]),
        'imdb_rating': data_en.get('vote_average') or data_ar.get('vote_average') or '',
        'release_date': data_ar.get('release_date') or data_ar.get('first_air_date') or '',
        'show_time': data_ar.get('runtime') or (data_ar.get('episode_run_time')[0] if data_ar.get('episode_run_time') else ''),
        'story': data_ar.get('overview') or data_en.get('overview') or '',
        'embed_server1': embed_server_movie_1,
        'embed_server2': embed_server_movie_2,
        'episodes_html': build_episodes_html(name_use, year, seasons_list, tmdb_id, date_prefix=''),
        'search_spans': build_search_spans(name_use, year, is_tv)
    }
    try:
        schema_root = build_jsonld_schema(kind, data_en or data_ar, imdb_id, season, episode)
    except Exception:
        schema_root = None

    # TV root
    if is_tv and season is None and episode is None:
        if db_has(imdb_id, None, None):
            logging.info('Series root exists (second check). Publishing missing episodes only.')
            conn = sqlite3.connect(DB_PATH)
            try:
                cur = conn.cursor()
                cur.execute('SELECT url FROM published WHERE imdb_id=? AND season IS NULL AND episode IS NULL', (imdb_id,))
                row = cur.fetchone()
                root_date_prefix = ''
                if row and row[0]:
                    parsed = urlparse(row[0])
                    parts = parsed.path.split('/')
                    if len(parts) >= 3 and parts[1].isdigit() and parts[2].isdigit():
                        root_date_prefix = f"/{parts[1]}/{parts[2]}"
            finally:
                conn.close()
            publish_missing_episodes(imdb_id, tmdb_id, name_use, year, seasons_list, data_ar, data_en, root_date_prefix, None)
            return True
        temp_title = root_slug
        final_title = f"مشاهده مسلسل {name_use} {year} مترجم - ايجی بست"
        description = f"مشاهده و تنزيل مسلسل {name_use} {year} مترجم اونلاين - ايجی بست"
        html_content = Template(HTML_TEMPLATE).render(**context_base)
        content_with_schema = (f"<script type='application/ld+json'>{json.dumps(schema_root, ensure_ascii=False)}</script>\n" if schema_root else "") + html_content
        labels = generate_labels('tv', data_ar or data_en, None, None)
        labels = [l for l in labels if isinstance(l, str) and l.strip()]
        ordered = []
        if 'en' in labels:
            ordered.append('en'); labels.remove('en')
        base = 'series'
        if base in labels:
            ordered.append(base); labels.remove(base)
        ordered.extend(labels)
        ordered.append(random.choice(['hd', 'hdtv']))
        labels = ordered
        try:
            post_id, post_url = create_post_and_patch(None, '', temp_title, final_title, content_with_schema, labels, description)
            db_insert({
                'imdb_id': imdb_id,
                'content_type': 'tv',
                'name': name_use,
                'year': year,
                'season': None,
                'episode': None,
                'blog_post_id': post_id,
                'url': post_url
            })
            mark_imdb_published(imdb_id)
            try:
                removed = remove_imdb_ids_from_txt([imdb_id], IMDB_FILE)
                if removed:
                    logging.info('Removed %s from file after root publish (removed=%s)', imdb_id, removed)
            except Exception:
                logging.exception('Failed to remove imdb id from file after root publish %s', imdb_id)
            logging.info('Published series root: %s -> %s', final_title, post_url)
            parsed = urlparse(post_url)
            date_prefix = ''
            try:
                parts = parsed.path.split('/')
                if len(parts) >= 3 and parts[1].isdigit() and parts[2].isdigit():
                    date_prefix = f"/{parts[1]}/{parts[2]}"
            except Exception:
                date_prefix = ''
            context_base['episodes_html'] = build_episodes_html(name_use, year, seasons_list, tmdb_id, date_prefix=date_prefix)
            updated_content = Template(HTML_TEMPLATE).render(**context_base)
            updated_with_schema = (f"<script type='application/ld+json'>{json.dumps(schema_root, ensure_ascii=False)}</script>\n" if schema_root else "") + updated_content
            # For static site, write updated root post file again (replace)
            try:
                # use temp_title (slug) to write again
                create_post_and_patch(None, '', temp_title, final_title, updated_with_schema, labels, description)
                logging.info('Updated series root with dated links: %s', post_url)
            except Exception as e:
                logging.exception('Failed to update root post with dated links: %s', e)
            publish_missing_episodes(imdb_id, tmdb_id, name_use, year, seasons_list, data_ar, data_en, date_prefix, None)
            return True
        except Exception:
            logging.exception('Failed to create series root for %s', imdb_id)
            return None

    # Movie root
    if kind == 'movie' and season is None and episode is None:
        temp_title = root_slug
        final_title = f"مشاهده فیلم {name_use} {year} مترجم - ايجی بست"
        description = f"مشاهده وتنزيل فیلم {name_use} {year} مترجم اونلاين - ايجی بست"
        html_content = Template(HTML_TEMPLATE).render(**context_base)
        content_with_schema = (f"<script type='application/ld+json'>{json.dumps(schema_root, ensure_ascii=False)}</script>\n" if schema_root else "") + html_content
        labels = generate_labels('movie', data_ar or data_en, None, None)
        labels = [l for l in labels if isinstance(l, str) and l.strip()]
        ordered = []
        if 'en' in labels:
            ordered.append('en'); labels.remove('en')
        base = 'movies'
        if base in labels:
            ordered.append(base); labels.remove(base)
        ordered.extend(labels)
        ordered.append(random.choice(['hd', 'hdtv']))
        labels = ordered
        try:
            post_id, post_url = create_post_and_patch(None, '', temp_title, final_title, content_with_schema, labels, description)
            db_insert({
                'imdb_id': imdb_id,
                'content_type': 'movie',
                'name': name_use,
                'year': year,
                'season': None,
                'episode': None,
                'blog_post_id': post_id,
                'url': post_url
            })
            mark_imdb_published(imdb_id)
            try:
                removed = remove_imdb_ids_from_txt([imdb_id], IMDB_FILE)
                if removed:
                    logging.info('Removed %s from file after movie publish (removed=%s)', imdb_id, removed)
            except Exception:
                logging.exception('Failed to remove imdb id from file after movie publish %s', imdb_id)
            logging.info('Published movie: %s -> %s', final_title, post_url)
            random_sleep_after_publish()
            return True
        except Exception:
            logging.exception('Failed to publish movie %s', imdb_id)
            return None

    # single episode
    if is_tv and season is not None and episode is not None:
        if db_has(imdb_id, season, episode):
            logging.info('Episode already published. Skipping S%sE%s', season, episode)
            try:
                removed = remove_imdb_ids_from_txt([imdb_id], IMDB_FILE)
                if removed:
                    logging.info('Removed %s from file because episode already published (removed=%s)', imdb_id, removed)
            except Exception:
                logging.exception('Failed to remove imdb id from file after skip %s', imdb_id)
            return None
        embed_server1 = f'https://vidsrc.xyz/embed/tv/{imdb_id}/{season}/{episode}'
        embed_server2 = f'https://vidsrc.to/embed/tv/{imdb_id}/{season}/{episode}'
        episodes_html = build_episodes_html(name_use, year, seasons_list, tmdb_id, date_prefix='')
        context = {
            'image_url': 'https://image.tmdb.org/t/p/w780' + (poster_path or ''),
            'name': name_use,
            'year': year,
            'content_type': 'مسلسل',
            'country': (data_ar.get('production_countries') or [{}])[0].get('name', ''),
            'lang': data_en.get('original_language', ''),
            'category': ', '.join([g.get('name') for g in (data_ar.get('genres') or [])]),
            'imdb_rating': data_en.get('vote_average') or data_ar.get('vote_average') or '',
            'release_date': data_ar.get('release_date') or data_ar.get('first_air_date') or '',
            'show_time': data_ar.get('runtime') or (data_ar.get('episode_run_time')[0] if data_ar.get('episode_run_time') else ''),
            'story': data_ar.get('overview') or data_en.get('overview') or '',
            'embed_server1': embed_server1,
            'embed_server2': embed_server2,
            'episodes_html': episodes_html,
            'search_spans': build_search_spans(name_use, year, True)
        }
        html_content = Template(HTML_TEMPLATE).render(**context)
        try:
            schema = build_jsonld_schema('tv', data_en or data_ar, imdb_id, season, episode)
        except Exception:
            schema = None
        content_with_schema = (f"<script type='application/ld+json'>{json.dumps(schema, ensure_ascii=False)}</script>\n" if schema else "") + html_content
        ep_slug = f"{slugify(name_use)}-{season}-{episode}"
        temp_title = ep_slug
        final_title = f"مشاهده مسلسل {name_use} الموسم {season} الحلقه {episode} {year} مترجم - ایجی بست"
        description = f"مشاهده و تنزيل مسلسل {name_use} الموسم {season} الحلقه {episode}"
        labels = generate_labels('tv', data_ar or data_en, season, episode)
        labels = [l for l in labels if isinstance(l, str) and l.strip()]
        ordered = []
        if 'en' in labels:
            ordered.append('en'); labels.remove('en')
        base = 'series'
        if base in labels:
            ordered.append(base); labels.remove(base)
        ordered.extend(labels)
        ordered.append(random.choice(['hd', 'hdtv']))
        labels = ordered
        try:
            post_id, post_url = create_post_and_patch(None, '', temp_title, final_title, content_with_schema, labels, description)
            db_insert({
                'imdb_id': imdb_id,
                'content_type': 'tv',
                'name': name_use,
                'year': year,
                'season': season,
                'episode': episode,
                'blog_post_id': post_id,
                'url': post_url
            })
            mark_imdb_published(imdb_id)
            try:
                removed = remove_imdb_ids_from_txt([imdb_id], IMDB_FILE)
                if removed:
                    logging.info('Removed %s from file after episode publish (removed=%s)', imdb_id, removed)
            except Exception:
                logging.exception('Failed to remove imdb id from file after episode publish %s', imdb_id)
            logging.info('Published episode: %s -> %s', final_title, post_url)
            random_sleep_after_publish()
            return True
        except Exception:
            logging.exception('Failed to publish episode %s S%sE%s', imdb_id, season, episode)
            return None

    return None

# ---------------------------
# Main loop (reads file-based queue)
# ---------------------------
def main():
    init_db()
    RUN_FOREVER = os.environ.get('RUN_FOREVER', '1') == '1'
    CYCLE_SLEEP = int(os.environ.get('CYCLE_SLEEP', '600'))
    MAX_PUBLISH_PER_CYCLE = int(os.environ.get('MAX_PUBLISH_PER_CYCLE', '20'))

    while True:
        published_count = 0
        try:
            global PUBLISHED_THIS_CYCLE
            PUBLISHED_THIS_CYCLE = 0

            any_ids_found = False
            stop_processing = False

            for chunk in iter_imdb_queue(IMDB_FILE, chunk_size=CHUNK_SIZE):
                if stop_processing:
                    break

                any_ids_found = True
                queue = [{'imdb_id': iid, 'type': 'movie', 'season': None, 'episode': None} for iid in chunk]

                for item in queue:
                    if PUBLISHED_THIS_CYCLE >= MAX_PUBLISH_PER_CYCLE:
                        logging.info('Reached MAX_PUBLISH_PER_CYCLE (%s) during this cycle. Stopping processing queue.', MAX_PUBLISH_PER_CYCLE)
                        stop_processing = True
                        break

                    imdb_id = item.get('imdb_id')

                    try:
                        if db_has(imdb_id, None, None):
                            logging.info('Found %s already in published table -> removing from file and skipping.', imdb_id)
                            try:
                                removed = remove_imdb_ids_from_txt([imdb_id], IMDB_FILE)
                                logging.info('Removed %s entries from file for %s', removed, imdb_id)
                            except Exception:
                                logging.exception('Failed to remove imdb id from file for %s after found in published table', imdb_id)
                            continue
                    except Exception:
                        logging.exception('Error while checking published table for %s', imdb_id)

                    try:
                        if imdb_queue_is_published(imdb_id):
                            logging.info('Found %s marked published in imdb_queue DB -> removing from file and skipping.', imdb_id)
                            try:
                                removed = remove_imdb_ids_from_txt([imdb_id], IMDB_FILE)
                                logging.info('Removed %s entries from file for %s (imdb_queue published)', removed, imdb_id)
                            except Exception:
                                logging.exception('Failed to remove imdb id from file for %s after imdb_queue reported published', imdb_id)
                            continue
                    except Exception:
                        logging.exception('Failed checking imdb_queue publish status for %s', imdb_id)

                    try:
                        ok = publish_imdb_item(imdb_id, season=None, episode=None, is_dry_run=False)
                        if ok:
                            try:
                                removed = remove_imdb_ids_from_txt([imdb_id], IMDB_FILE)
                                logging.info('Published and removed %s from file (removed=%s)', imdb_id, removed)
                            except Exception:
                                logging.exception('Failed to remove imdb id from file after publish %s', imdb_id)
                        else:
                            logging.info('Skipped or failed to publish %s (ok=%s)', imdb_id, ok)
                    except Exception:
                        logging.exception('Failed publish from file queue %s', imdb_id)

            if not any_ids_found:
                logging.info('No imdb ids found in %s this cycle.', IMDB_FILE)

            published_count = PUBLISHED_THIS_CYCLE
            logging.info('Cycle completed. Published %s items this cycle.', published_count)

        except Exception:
            logging.exception('Unexpected error in file-based publish cycle.')
            try:
                published_count = PUBLISHED_THIS_CYCLE
            except Exception:
                published_count = 0

        if not RUN_FOREVER:
            break

        if published_count >= MAX_PUBLISH_PER_CYCLE and CYCLE_SLEEP > 0:
            logging.info('Published %s items (>= %s). Sleeping %s seconds before next discover cycle...', published_count, MAX_PUBLISH_PER_CYCLE, CYCLE_SLEEP)
            time.sleep(CYCLE_SLEEP)
        else:
            logging.info('Published %s items (< %s). Continuing next cycle immediately.', published_count, MAX_PUBLISH_PER_CYCLE)
            time.sleep(0.5)

if __name__ == '__main__':
    main()
