# main.py
import os
from pathlib import Path
from datetime import date
from dotenv import load_dotenv

load_dotenv()

import getxml       # télécharge le XML officiel
import parse        # parse + upsert en base
import enrich_brands

def assert_recent_import():
    """Échoue le job si on n'a pas d'import courant aujourd'hui (détecte les faux positifs)."""
    import psycopg2
    conn = enrich_brands.get_db_conn()
    history_enabled = str(os.getenv("ENABLE_CARBURANTS_HISTORY") or "").strip().lower() in {"1", "true", "yes", "on"}
    with conn.cursor() as cur:
        if history_enabled:
            cur.execute("SELECT MAX(date_import) FROM carburants")
        else:
            cur.execute("SELECT MAX(ts) FROM carburant_current")
        mx = cur.fetchone()[0]
    conn.close()
    if not mx or mx.date() < date.today():
        raise SystemExit(
            f"[main] ÉCHEC: pas de lignes carburants courantes datées aujourd'hui (max={mx}). "
            "Vérifie getxml/parse/logs."
        )
    source = "carburants.date_import" if history_enabled else "carburant_current.ts"
    print(f"[main] OK: max({source}) = {mx}")

def print_env_debug():
    print(f"[main] CWD: {Path.cwd()}")
    print(f"[main] Repo dir: {Path(__file__).resolve().parent}")
    for k in ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER"):
        print(f"[main] {k}={os.getenv(k)}")

def print_sample_with_brands(n=5):
    import psycopg2
    conn = enrich_brands.get_db_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, ville, brand_name, brand_short_name
            FROM stations
            ORDER BY (brand_name IS NULL), id
            LIMIT %s
        """, (n,))
        rows = cur.fetchall()
    conn.close()

    print("\n🧪 Échantillon stations (avec marques si dispo):")
    print("-" * 60)
    for (sid, ville, bname, bshort) in rows:
        print(f"ID: {sid:<8} | Ville: {ville:<20} | Brand: {bname or '—'} | Short: {bshort or '—'}")
    print("-" * 60)

def main():
    print_env_debug()

    print("[main] 1) Téléchargement XML…")
    getxml.main()  # Assure-toi que getxml écrit bien dans data/actuel/… (identique à parse)

    print("[main] 2) Parse + upsert…")
    parse.main()

    print("[main] 3) Garde-fou d'import (doit être aujourd'hui)…")
    assert_recent_import()

    print("[main] 4) Enrichissement marques…")
    # limite si tu veux: limit=None pour tout; only_missing=True par défaut
    enrich_brands.main(limit=None, max_workers=12, only_missing=True)

    print("[main] 5) Contrôle visuel:")
    print_sample_with_brands(n=8)

if __name__ == "__main__":
    main()
