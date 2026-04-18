#!/usr/bin/env python3
"""Purge complète de la table carburants historique.

Usage:
  python truncate_carburants_history.py

Variables optionnelles:
  DROP_CARBURANTS_HISTORY=1  -> supprime la table au lieu de la vider
"""

import os
from datetime import datetime

from dotenv import load_dotenv

import enrich_brands


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _size_pretty(cur):
    cur.execute(
        """
        SELECT
          COALESCE(to_regclass('public.carburants')::text, '') AS relname,
          CASE
            WHEN to_regclass('public.carburants') IS NULL THEN NULL
            ELSE pg_size_pretty(pg_total_relation_size('public.carburants'))
          END AS total_size
        """
    )
    return cur.fetchone()


def main():
    load_dotenv()
    drop_history = _env_flag("DROP_CARBURANTS_HISTORY", default=False)

    print(f"[truncate-history] start {datetime.utcnow().isoformat()}Z")
    print(f"[truncate-history] mode={'drop' if drop_history else 'truncate'}")

    conn = enrich_brands.get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.carburants')")
            exists = cur.fetchone()[0] is not None
            if not exists:
                print("[truncate-history] table carburants absente, rien à faire.")
                conn.commit()
                return

            before_relname, before_size = _size_pretty(cur)
            print(f"[truncate-history] before: table={before_relname} size={before_size}")

            if drop_history:
                cur.execute("DROP TABLE carburants")
            else:
                cur.execute("TRUNCATE TABLE carburants")

            conn.commit()

            after_relname, after_size = _size_pretty(cur)
            if drop_history:
                print("[truncate-history] table carburants supprimée.")
            else:
                print(f"[truncate-history] after: table={after_relname} size={after_size}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
