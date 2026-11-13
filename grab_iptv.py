#!/usr/bin/env python3
"""Full IPTV live-source grab + verify, standalone (for InstaBox).

Fetches the latest iptv-org data, filters to HK/MO/TW + foreign news +
foreign movie channels (English or Chinese feeds only), validates every
candidate stream, and writes labelled M3U playlists with clear program
names (tvg-name = channel name, display title = stream title or channel
name, group-title = category) plus a summary JSON.

Validation rule (same as the 2026-11-09 run):
- only http/https URLs; GET timeout=6s, stream, read up to 64KB
- status must be 200 or 206
- for .m3u8 the body must contain '#EXTM3U' or '#EXT-X-'
- max 2 concurrent requests per host; ThreadPoolExecutor workers=20

Outputs (current dir): gang-aotai.m3u, guowai-xinwen.m3u,
guowai-dianying.m3u, heji.m3u, summary.json, dropped.txt
"""
import json
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import requests

API = 'https://iptv-org.github.io/api'
SOURCES = {
    'channels': f'{API}/channels.json',
    'feeds': f'{API}/feeds.json',
    'streams': f'{API}/streams.json',
    'categories': f'{API}/categories.json',
    'languages': f'{API}/languages.json',
    'blocklist': f'{API}/blocklist.json',
}
LANGS = {'eng', 'zho'}
MAX_WORKERS = 20
TIMEOUT = 6
BJ = timezone(timedelta(hours=8))

print('fetching iptv-org data...', flush=True)
data = {}
for name, url in SOURCES.items():
    r = requests.get(url, timeout=120, headers={'User-Agent': 'Mozilla/5.0'})
    r.raise_for_status()
    data[name] = r.json()
    print(f'  {name}: {len(data[name])}', flush=True)

channels = {c['id']: c for c in data['channels']}
cat_ids = {c['id'] for c in data['categories']}
assert 'news' in cat_ids and 'movies' in cat_ids, 'news/movies categories missing!'

block = {b['channel'] for b in data['blocklist']}
feed_lang = defaultdict(set)
feed_map = {}
for f in data['feeds']:
    feed_map[f['id']] = f
    feed_lang[f['channel']] |= set(f.get('languages') or [])


def chan_ok(cid):
    c = channels.get(cid)
    return c is not None and not c.get('is_nsfw') and cid not in block


def stream_lang_ok(s):
    f = feed_map.get(s.get('feed'))
    if f is not None:
        return bool(set(f.get('languages') or []) & LANGS)
    return bool(feed_lang.get(s['channel'], set()) & LANGS)


def stream_ok(s):
    return chan_ok(s['channel']) and stream_lang_ok(s)


def bucket(pred):
    return [s for s in data['streams'] if stream_ok(s) and pred(channels[s['channel']])]


HK = {'HK', 'MO', 'TW'}
EXCL = {'CN'} | HK
hk_all = bucket(lambda c: c.get('country') in HK)
news_all = bucket(lambda c: 'news' in (c.get('categories') or []) and c.get('country') not in EXCL)
mov_all = bucket(lambda c: 'movies' in (c.get('categories') or []) and c.get('country') not in EXCL)
ALL = ([('hk', s) for s in hk_all]
       + [('news', s) for s in news_all]
       + [('mov', s) for s in mov_all])
print(f'total candidates: {len(ALL)} '
      f'(hk {len(hk_all)}, news {len(news_all)}, mov {len(mov_all)})', flush=True)

HOST_SEMS = {}
HOST_LOCK = threading.Lock()


def host_sem(netloc):
    with HOST_LOCK:
        sem = HOST_SEMS.get(netloc)
        if sem is None:
            sem = threading.Semaphore(2)
            HOST_SEMS[netloc] = sem
        return sem


def check(stream):
    url = stream['url']
    scheme = urlparse(url).scheme
    if scheme not in ('http', 'https'):
        return ('skip_non_http', stream)
    headers = {'User-Agent': 'Mozilla/5.0'}
    if stream.get('user_agent'):
        headers['User-Agent'] = stream['user_agent']
    if stream.get('referrer'):
        headers['Referer'] = stream['referrer']
    netloc = urlparse(url).netloc
    try:
        with host_sem(netloc):
            r = requests.get(url, headers=headers, timeout=TIMEOUT, stream=True)
            status = r.status_code
            body = b''
            try:
                for chunk in r.iter_content(chunk_size=8192):
                    body += chunk
                    if len(body) >= 65536:
                        break
            except Exception:
                pass
            r.close()
        if status not in (200, 206):
            return ('drop_http', stream)
        if '.m3u8' in url.split('?')[0].lower():
            text = body.decode('utf-8', errors='ignore')
            if '#EXTM3U' not in text and '#EXT-X-' not in text:
                return ('drop_not_playlist', stream)
        return ('ok', stream)
    except requests.exceptions.Timeout:
        return ('drop_timeout', stream)
    except Exception:
        return ('drop_error', stream)


def validate(lst, label):
    t0 = time.time()
    results = {'ok': [], 'drop_http': [], 'drop_not_playlist': [],
               'drop_timeout': [], 'drop_error': [], 'skip_non_http': []}
    n = len(lst)
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(check, s) for s in lst]
        for fut in as_completed(futs):
            k, s = fut.result()
            results[k].append(s)
            done += 1
            if done % 300 == 0:
                print(f'{label}: {done}/{n} ({time.time() - t0:.0f}s, ok {len(results["ok"])})',
                      flush=True)
    dt = time.time() - t0
    dropped = sum(len(results[k]) for k in
                  ('drop_http', 'drop_not_playlist', 'drop_timeout', 'drop_error'))
    print(f'{label}: checked {n} in {dt:.0f}s -> ok {len(results["ok"])}, dropped {dropped}',
          flush=True)
    return results


def split_results(all_results, groups):
    """all_results: list of (bucket, result_key, stream) in input order."""
    out = {}
    for g in groups:
        out[g] = {'ok': [], 'drop_http': [], 'drop_not_playlist': [],
                  'drop_timeout': [], 'drop_error': [], 'skip_non_http': []}
    for b, k, s in all_results:
        out[b][k].append(s)
    return out


# validate each group (same code, keeps per-group stats clean)
res_hk = validate(hk_all, 'hk')
res_news = validate(news_all, 'news')
res_mov = validate(mov_all, 'mov')


def esc(v):
    return str(v).replace('"', "'").replace('\n', ' ').strip()


def write_m3u(path, items):
    seen = set()
    lines = ['#EXTM3U']
    for s, group in sorted(items, key=lambda p: ((channels[p[0]['channel']].get('name') or ''), p[0]['url'])):
        url = s['url']
        if url in seen:
            continue
        seen.add(url)
        c = channels[s['channel']]
        title = s.get('title') or c.get('name') or s['channel']
        lines.append(f'#EXTINF:-1 tvg-id="{esc(s["channel"])}" tvg-name="{esc(c.get("name") or "")}" group-title="{group}",{esc(title)}')
        lines.append(url)
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write('\n'.join(lines) + '\n')
    return len(seen)


now = datetime.now(BJ)
kept_hk = write_m3u('gang-aotai.m3u', [(s, '港澳台') for s in res_hk['ok']])
kept_news = write_m3u('guowai-xinwen.m3u', [(s, '国外新闻') for s in res_news['ok']])
kept_mov = write_m3u('guowai-dianying.m3u', [(s, '国外电影') for s in res_mov['ok']])
combined = ([(s, '港澳台') for s in res_hk['ok']]
            + [(s, '国外新闻') for s in res_news['ok']]
            + [(s, '国外电影') for s in res_mov['ok']])
kept_all = write_m3u('heji.m3u', combined)


def geo_dropped(res):
    n = 0
    for k in ('drop_http', 'drop_not_playlist', 'drop_timeout', 'drop_error'):
        n += sum(1 for s in res[k] if 'Geo-blocked' in (s.get('labels') or []))
    return n


def dropped(res):
    return sum(len(res[k]) for k in ('drop_http', 'drop_not_playlist', 'drop_timeout', 'drop_error'))


def fstats(group, res, checked, kept):
    return {'group': group,
            'channels_kept': len({s['channel'] for s in res['ok']}),
            'streams_checked': checked,
            'streams_working': len(res['ok']),
            'streams_dropped': dropped(res),
            'geo_blocked_dropped': geo_dropped(res),
            'skipped_non_http': len(res['skip_non_http']),
            'entries_in_file': kept}


summary = {
    'run_timestamp_beijing': now.strftime('%Y-%m-%d %H:%M:%S') + ' (Beijing, UTC+8)',
    'run_timestamp_beijing_iso': now.isoformat(),
    'sources': SOURCES,
    'files': {
        'gang-aotai': fstats('港澳台', res_hk, len(hk_all), kept_hk),
        'guowai-xinwen': fstats('国外新闻', res_news, len(news_all), kept_news),
        'guowai-dianying': fstats('国外电影', res_mov, len(mov_all), kept_mov),
    },
    'heji_entries': kept_all,
    'totals': {
        'streams_checked': len(hk_all) + len(news_all) + len(mov_all),
        'streams_working': len(res_hk['ok']) + len(res_news['ok']) + len(res_mov['ok']),
        'streams_dropped': dropped(res_hk) + dropped(res_news) + dropped(res_mov),
        'geo_blocked_dropped': geo_dropped(res_hk) + geo_dropped(res_news) + geo_dropped(res_mov),
        'skipped_non_http': len(res_hk['skip_non_http']) + len(res_news['skip_non_http']) + len(res_mov['skip_non_http']),
        'channels_kept_total': len({s['channel'] for s in res_hk['ok'] + res_news['ok'] + res_mov['ok']}),
    },
}
with open('summary.json', 'w', encoding='utf-8') as fh:
    json.dump(summary, fh, ensure_ascii=False, indent=2)
with open('dropped.txt', 'w', encoding='utf-8') as fh:
    for g, res in (('hk', res_hk), ('news', res_news), ('mov', res_mov)):
        for k in ('drop_http', 'drop_not_playlist', 'drop_timeout', 'drop_error', 'skip_non_http'):
            for s in res[k]:
                fh.write(f'[{g}/{k}] {s["channel"]} {s["url"]}\n')
print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
print('DONE', flush=True)
