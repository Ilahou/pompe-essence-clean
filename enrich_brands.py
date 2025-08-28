#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

API_BASE = os.getenv("FUEL_API_STATION_BASE", "https://api.prix-carburants.2aaz.fr/station/")

def _clean_str(s):
    if not isinstance(s, str):
        return None
    s = s.strip()
    return s or None

def _extract_text(v):
    if v is None:
        return None
    if isinstance(v, str):
        return v.strip() or None
    if isinstance(v, dict):
        for k in ("value", "text", "short", "label"):
            val = v.get(k)
            if isinstance(val, str) and val.strip():
                return val.strip()
    return None

def _brand_fields(brand: dict):
    if not isinstance(brand, dict):
        return (None, None)

    name_candidates = [
        brand.get("name"), brand.get("Name"), brand.get("label"),
        brand.get("display_name"), brand.get("brand")
    ]
    short_candidates = [
        brand.get("short_name"), brand.get("shortName"), brand.get("shortname"),
        brand.get("short"), brand.get("abbr"), brand.get("code"), brand.get("alias")
    ]

    name = None
    for c in name_candidates:
        name = _extract_text(c)
        if name:
            break

    short = None
    for c in short_candidates:
        short = _extract_text(c)
        if short:
            break

    return (_clean_str(name), _clean_str(short))

def get_db_conn():
    load_dotenv()
    db_url = os.getenv("DATABASE_PUBLIC_URL") or os.getenv("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url, connect_timeout=10)

    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT", "5432")
    db   = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    pwd  = os.getenv("PGPASSWORD")
    sslmode = os.getenv("PGSSLMODE")

    kwargs = dict(host=host, port=port, dbname=db, user=user, password=pwd, connect_timeout=10)
    if sslmode:
        kwargs["sslmode"] = sslmode
    return psycopg2.connect(**kwargs)

def ensure_brand_columns(conn):
    cur = conn.cursor()
    # ✅ corrige la syntaxe : pas de "IF NOT EXISTS" après ALTER TABLE
    cur.execute("ALTER TABLE stations ADD COLUMN IF NOT EXISTS brand_name TEXT;")
    cur.execute("ALTER TABLE stations ADD COLUMN IF NOT EXISTS brand_short_name TEXT;")
    conn.commit()
    cur.close()

def get_candidate_ids(conn, only_missing=True, limit=None):
    cur = conn.cursor()
    if only_missing:
        sql = "SELECT id FROM stations WHERE brand_name IS NULL OR brand_short_name IS NULL ORDER BY id"
    else:
        sql = "SELECT id FROM stations ORDER BY id"
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    rows = [r[0] for r in cur.fetchall()]
    cur.close()
    return rows

def apply_updates(conn, rows):
    if not rows:
        return 0
    sql = """
        UPDATE stations
        SET
            brand_name = COALESCE(%s, brand_name),
            brand_short_name = COALESCE(%s, brand_short_name)
        WHERE id = %s
    """
    cur = conn.cursor()
    execute_batch(cur, sql, [(bn, bs, sid) for (sid, bn, bs) in rows], page_size=300)
    conn.commit()
    cur.close()
    return len(rows)

def fetch_brand_for_id(station_id, session: requests.Session, retries=3, timeout=15, debug=False):
    url = f"{API_BASE}{station_id}"
    for attempt in range(retries):
        try:
            r = session.get(url, headers={"accept": "application/json"}, timeout=timeout)
            if r.status_code == 404:
                return (station_id, None, None, 404)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                time.sleep(0.6 * (attempt + 1))
                continue
            r.raise_for_status()
            js = r.json()
            brand = js.get("Brand") or {}
            name, short = _brand_fields(brand)

            if debug and not short:
                print(f"[debug] station {station_id} Brand brut:", json.dumps(brand, ensure_ascii=False))

            return (station_id, name, short, 200)
        except requests.RequestException:
            time.sleep(0.6 * (attempt + 1))
    return (station_id, None, None, -1)

def main(limit=None, max_workers=12, only_missing=True, debug=False):
    conn = get_db_conn()
    ensure_brand_columns(conn)

    ids = get_candidate_ids(conn, only_missing=only_missing, limit=limit)
    if not ids:
        print("ℹ️  Aucun ID à enrichir.")
        conn.close()
        return

    print(f"🔧 Enrichissement des marques pour {len(ids)} station(s)…")

    ok, missing, not_found = 0, 0, 0
    t0 = time.time()
    updates = []
    with requests.Session() as s:
        s.headers.update({"accept": "application/json"})
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(fetch_brand_for_id, sid, s, debug=debug): sid for sid in ids}
            done = 0
            for fut in as_completed(futures):
                sid = futures[fut]
                try:
                    sid, name, short, code = fut.result()
                    if code == 200:
                        updates.append((sid, name, short))
                        ok += 1
                    elif code == 404:
                        not_found += 1
                    else:
                        missing += 1
                except Exception:
                    missing += 1

                done += 1
                if done % 25 == 0 or done == len(ids):
                    pct = int(done * 100 / len(ids))
                    print(f"… {done}/{len(ids)} ({pct}%) — ok:{ok} / sans-marque:{missing} / 404:{not_found}")

    count = apply_updates(conn, updates)
    dt = time.time() - t0
    print(f"✅ Terminé en {dt:.1f}s — {count} mis à jour, {missing} sans marque / {not_found} 404.")
    conn.close()

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Enrichit stations.brand_name / brand_short_name via l’API prix-carburants.")
    p.add_argument("--limit", type=int, default=None, help="Limiter le nombre d’IDs traités")
    p.add_argument("--max-workers", type=int, default=12, help="Threads en parallèle")
    p.add_argument("--all", action="store_true", help="Traiter toutes les stations (pas seulement celles sans marque)")
    p.add_argument("--debug", action="store_true", help="Afficher le Brand brut si short introuvable")
    args = p.parse_args()

    main(limit=args.limit, max_workers=args.max_workers, only_missing=not args.all, debug=args.debug)
