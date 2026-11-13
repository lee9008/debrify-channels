#!/usr/bin/env python3
"""Validate EXTRA HK/MO/TW candidates (not covered by the iptv-org API pass):
- iptv-org country .m3u files (tw/hk/mo) — URLs not in gang-aotai.m3u
  and not in dropped.txt
- bobyang3 liveTVList.txt — direct .m3u8/.ts entries with HK/MO/TW names
  (YouTube watch URLs and youtube-proxy URLs excluded)
- imDazui 台湾香港澳门202506.m3u — all entries not already known
Same validation rule as the 2026-11-12 grab: http/https, GET timeout 6s,
read <=64KB, status 200/206, .m3u8 body must contain '#EXTM3U' or '#EXT-X-'.
Per-host semaphore 2, 20 workers. No retries.
Results are saved incrementally (every 20 completions) so a restart
does not lose partial progress.
"""
import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests

UA = {'User-Agent': 'Mozilla/5.0'}
WS = '/home/hatch/workspace/iptv-live-sources/'
OUT = WS + 'hk_extra_results.json'
TIMEOUT = 6

existing = set()
for line in open(WS + 'gang-aotai.m3u', encoding='utf-8'):
    line = line.strip()
    if line.startswith('http'):
        existing.add(line)
dropped = set()
for line in open(WS + 'dropped.txt', encoding='utf-8'):
    m = re.search(r'(https?://\S+)', line)
    if m:
        dropped.add(m.group(1))

HK_KW = ['香港', '港台', 'RTHK', 'TVB', '翡翠', 'J2', 'HOY', 'Viu',
         '鳳凰', '凤凰', '創世', '创世', '奇妙', '有線', '有线', 'Now',
         '無綫', '无线', '亞視', '亚视', '佛陀', '澳門', '澳门']
MO_KW = ['澳門', '澳门', 'TDM', '蓮花', '莲花', '澳亞', '澳亚']
TW_KW = ['台灣', '台湾', '台視', '台视', '華視', '华视', '民視', '民视',
         '公視', '公视', '中視', '中视', '東森', '东森', '三立', 'TVBS',
         '中天', '大愛', '大爱', '客家', '原民', '原住', '唯心', '好消息',
         '生命', '希望', 'NTD', '新唐人', '八大', '衛視', '卫视', 'ELTA',
         '愛爾達', '爱尔达', '緯來', '纬来', '非凡', '年代', '壹電視',
         '壹电视', '鏡新聞', '镜新闻', '寰宇', 'MOMO', 'Momo', '靖天',
         '國會', '国会', '人間', '人间', 'AXN', 'HBO', 'Cinemax', 'Animax',
         'Discovery', 'CTS', 'FTV', 'PTS', 'CTV', 'EBC', 'SET']


def region_of(name):
    for kw in MO_KW:
        if kw in name:
            return 'MO'
    for kw in HK_KW:
        if kw in name:
            return 'HK'
    for kw in TW_KW:
        if kw in name:
            return 'TW'
    return ''


def parse_m3u(text):
    out = []
    name = None
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith('#EXTINF'):
            m = re.search(r',([^,]*)$', line)
            name = m.group(1).strip() if m else line
        elif line.startswith('#'):
            continue
        elif line.startswith('http'):
            out.append((name or line, line))
            name = None
    return out


cands = {}  # url -> {name, url, sources, region}


def add(name, url, src):
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        return
    if url in existing or url in dropped:
        return
    if url in cands:
        cands[url]['sources'].append(src)
        return
    cands[url] = {'name': name, 'url': url, 'sources': [src],
                  'region': region_of(name)}


# 1. iptv-org country files
for r in ('tw', 'hk', 'mo'):
    text = open(f'/tmp/iptv-org-{r}.m3u', encoding='utf-8').read()
    for n, u in parse_m3u(text):
        add(n, u, f'iptv-org-{r}')
print(f'after iptv-org country files: {len(cands)}', flush=True)

# 2. imDazui
text = open('/tmp/imdazui-gangaotai.m3u', encoding='utf-8', errors='ignore').read()
n0 = len(cands)
for n, u in parse_m3u(text):
    add(n, u, 'imDazui-202506')
print(f'after imDazui: {len(cands)} (+{len(cands) - n0})', flush=True)

# 3. liveTVList.txt (TVBox txt: "name,url" lines; strip &data= / $decoder suffixes)
BAD_HOSTS = ('youtube.com', 'youtu.be', 'clonglii.serv00.net')
n0 = len(cands)
for line in open('/tmp/livetvlist.txt', encoding='utf-8', errors='ignore'):
    line = line.strip()
    if not line or line.startswith('#') or ',http' not in line:
        continue
    name, url = line.split(',', 1)
    name = name.strip()
    url = url.strip()
    url = re.split(r'&data=|\$decoder|\s', url)[0].strip()
    if not url.startswith(('http://', 'https://')):
        continue
    host = urlparse(url).netloc.lower()
    if any(b in host for b in BAD_HOSTS):
        continue
    if not region_of(name):
        continue
    low = url.lower().split('?')[0]
    if not (low.endswith(('.m3u8', '.ts', '.mpd', '.php', '.m3u')) or '/live/' in low
            or re.search(r':\d+/', low)):
        continue
    add(name, url, 'liveTVList')
print(f'after liveTVList: {len(cands)} (+{len(cands) - n0})', flush=True)

items = list(cands.values())
print(f'total extra candidates to validate: {len(items)}', flush=True)

HOST_SEMS = {}
HOST_LOCK = threading.Lock()


def host_sem(netloc):
    with HOST_LOCK:
        sem = HOST_SEMS.get(netloc)
        if sem is None:
            sem = threading.Semaphore(2)
            HOST_SEMS[netloc] = sem
        return sem


def check(item):
    url = item['url']
    try:
        with host_sem(urlparse(url).netloc):
            r = requests.get(url, headers=UA, timeout=TIMEOUT, stream=True)
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
            return ('drop_http', item)
        if '.m3u8' in url.split('?')[0].lower():
            text = body.decode('utf-8', errors='ignore')
            if '#EXTM3U' not in text and '#EXT-X-' not in text:
                return ('drop_not_playlist', item)
        return ('ok', item)
    except requests.exceptions.Timeout:
        return ('drop_timeout', item)
    except Exception:
        return ('drop_error', item)


results = {'ok': [], 'drop_http': [], 'drop_not_playlist': [],
           'drop_timeout': [], 'drop_error': []}


def save(partial=False):
    with open(OUT, 'w', encoding='utf-8') as fh:
        json.dump({'ok': results['ok'],
                   'dropped': {k: results[k] for k in
                               ('drop_http', 'drop_not_playlist',
                                'drop_timeout', 'drop_error')},
                   'counts': {'candidates': len(items),
                              'checked_so_far': sum(len(v) for v in results.values()),
                              'ok': len(results['ok']),
                              'partial': partial}},
                  fh, ensure_ascii=False, indent=2)


with ThreadPoolExecutor(max_workers=20) as ex:
    futs = [ex.submit(check, it) for it in items]
    for i, fut in enumerate(as_completed(futs), 1):
        k, it = fut.result()
        results[k].append(it)
        if i % 20 == 0:
            save(partial=True)
            print(f'validated {i}/{len(items)} ok={len(results["ok"])}', flush=True)

save(partial=False)
print(f'checked {len(items)} -> ok {len(results["ok"])}, '
      f'drop_http {len(results["drop_http"])}, '
      f'drop_not_playlist {len(results["drop_not_playlist"])}, '
      f'drop_timeout {len(results["drop_timeout"])}, '
      f'drop_error {len(results["drop_error"])}', flush=True)
for it in sorted(results['ok'], key=lambda x: (x['region'], x['name'])):
    print(f"OK [{it['region']}] {it['name']} :: {it['url']}  ({';'.join(it['sources'])})",
          flush=True)
print('DONE ->', OUT, flush=True)
