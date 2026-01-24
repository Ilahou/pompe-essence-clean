#!/usr/bin/env python3
# dedup_services_batch.py
import os
import time
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

import enrich_brands


def dedup_batch(pair_limit=500, delete_limit=5000, sleep_s=0.3, max_batches=None):
    conn = enrich_brands.get_db_conn()
    conn.autocommit = False
    total_deleted = 0
    batch = 0

    try:
        with conn.cursor() as cur:
            while True:
                cur.execute(
                    """
                    WITH dup_pairs AS (
                      SELECT station_id, service
                      FROM services
                      GROUP BY station_id, service
                      HAVING COUNT(*) > 1
                      LIMIT %s
                    ), kept AS (
                      SELECT DISTINCT ON (s.station_id, s.service)
                        s.ctid, s.station_id, s.service
                      FROM services s
                      JOIN dup_pairs d USING (station_id, service)
                      ORDER BY s.station_id, s.service, s.date_import DESC NULLS LAST, s.ctid DESC
                    ), to_delete AS (
                      SELECT s.ctid
                      FROM services s
                      JOIN dup_pairs d USING (station_id, service)
                      LEFT JOIN kept k ON k.ctid = s.ctid
                      WHERE k.ctid IS NULL
                      LIMIT %s
                    )
                    DELETE FROM services
                    WHERE ctid IN (SELECT ctid FROM to_delete)
                    RETURNING 1
                    """,
                    (pair_limit, delete_limit),
                )
                deleted = cur.rowcount or 0
                conn.commit()
                total_deleted += deleted
                batch += 1

                print(f"[dedup] batch {batch} deleted={deleted} total_deleted={total_deleted}")

                if deleted == 0:
                    break
                if max_batches is not None and batch >= max_batches:
                    break
                time.sleep(sleep_s)
    finally:
        conn.close()

    print(f"[dedup] DONE total_deleted={total_deleted}")


def create_unique_index():
    conn = enrich_brands.get_db_conn()
    conn.autocommit = True  # required for CONCURRENTLY
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_services_station_service
                ON services(station_id, service)
                """
            )
    finally:
        conn.close()


def main():
    load_dotenv()
    pair_limit = int(os.getenv("DEDUP_PAIR_LIMIT", "500"))
    delete_limit = int(os.getenv("DEDUP_DELETE_LIMIT", "5000"))
    sleep_s = float(os.getenv("DEDUP_SLEEP_S", "0.3"))
    max_batches_env = os.getenv("DEDUP_MAX_BATCHES")
    max_batches = int(max_batches_env) if max_batches_env else None

    print(f"[dedup] start {datetime.utcnow().isoformat()}Z")
    print(f"[dedup] pair_limit={pair_limit} delete_limit={delete_limit} sleep_s={sleep_s} max_batches={max_batches}")
    dedup_batch(pair_limit=pair_limit, delete_limit=delete_limit, sleep_s=sleep_s, max_batches=max_batches)
    print("[dedup] creating unique index concurrently...")
    create_unique_index()
    print("[dedup] done")


if __name__ == "__main__":
    main()
