#!/usr/bin/env python3
# purge_carburants_batch.py
import os
import time
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

import enrich_brands


def purge_batch(batch_size=5000, sleep_s=0.3, max_batches=None):
    conn = enrich_brands.get_db_conn()
    conn.autocommit = False
    total_deleted = 0
    batch = 0

    try:
        with conn.cursor() as cur:
            while True:
                cur.execute(
                    """
                    WITH doomed AS (
                        SELECT ctid
                        FROM carburants
                        WHERE COALESCE(date_maj, date_import) < NOW() - INTERVAL '30 days'
                        LIMIT %s
                    )
                    DELETE FROM carburants
                    WHERE ctid IN (SELECT ctid FROM doomed)
                    RETURNING 1
                    """,
                    (batch_size,),
                )
                deleted = cur.rowcount or 0
                conn.commit()
                total_deleted += deleted
                batch += 1

                print(f"[purge] batch {batch} deleted={deleted} total_deleted={total_deleted}")

                if deleted < batch_size:
                    break
                if max_batches is not None and batch >= max_batches:
                    break
                time.sleep(sleep_s)
    finally:
        conn.close()

    print(f"[purge] DONE total_deleted={total_deleted}")


def main():
    load_dotenv()
    batch_size = int(os.getenv("PURGE_BATCH_SIZE", "5000"))
    sleep_s = float(os.getenv("PURGE_SLEEP_S", "0.3"))
    max_batches_env = os.getenv("PURGE_MAX_BATCHES")
    max_batches = int(max_batches_env) if max_batches_env else None

    print(f"[purge] start {datetime.utcnow().isoformat()}Z")
    print(f"[purge] batch_size={batch_size} sleep_s={sleep_s} max_batches={max_batches}")
    purge_batch(batch_size=batch_size, sleep_s=sleep_s, max_batches=max_batches)


if __name__ == "__main__":
    main()
