# parse.py
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_batch, execute_values
import os
import re
from datetime import datetime, timezone
from pathlib import Path

def _clean(txt: str) -> str:
    # Nettoie basiquement les textes XML pour éliminer les espaces multiples.
    return re.sub(r'\s+', ' ', (txt or '').strip())

def main():
    print("Début parsing...")
    now_utc = datetime.now(timezone.utc)
    now_naive = now_utc.replace(tzinfo=None)

    # --- Résolution de chemin robuste (cron-proof)
    BASE_DIR = Path(__file__).resolve().parent
    XML_PATH = Path(os.getenv("XML_PATH", BASE_DIR / "data/actuel/PrixCarburants_instantane.xml"))

    if not XML_PATH.exists():
        raise FileNotFoundError(
            f"XML introuvable: {XML_PATH} (cwd={Path.cwd()}). "
            "Assure-toi que getxml écrit bien à cet emplacement ou passe XML_PATH dans l'env."
        )

    # Log de contrôle
    try:
        mtime = datetime.fromtimestamp(XML_PATH.stat().st_mtime, tz=timezone.utc)
        print(f"[parse] XML: {XML_PATH}")
        print(f"[parse] XML mtime (UTC): {mtime.isoformat()}")
    except Exception:
        pass

    # --- Connexion BDD (Railway: PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD)
    try:
        DB_HOST = os.getenv("PGHOST")
        DB_PORT = os.getenv("PGPORT")
        DB_NAME = os.getenv("PGDATABASE")
        DB_USER = os.getenv("PGUSER")
        DB_PASS = os.getenv("PGPASSWORD")

        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()

        # --- DDL (CREATE d'abord, puis ALTER) — idempotent
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stations (
              id INTEGER PRIMARY KEY,
              code_postal TEXT,
              ville TEXT,
              adresse TEXT,
              latitude DOUBLE PRECISION,
              longitude DOUBLE PRECISION,
              automate INTEGER
            )
        """)
        # Si table existante sans 'adresse'
        cur.execute("ALTER TABLE stations ADD COLUMN IF NOT EXISTS adresse TEXT")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS carburants (
              station_id INTEGER REFERENCES stations(id),
              carburant  TEXT,
              prix       DOUBLE PRECISION,
              date_import TIMESTAMP,
              date_maj   TIMESTAMP
            )
        """)
        cur.execute("ALTER TABLE carburants ADD COLUMN IF NOT EXISTS date_maj TIMESTAMP")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS services (
              station_id INTEGER REFERENCES stations(id),
              service    TEXT,
              date_import TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS carburant_current (
              station_id INTEGER NOT NULL,
              carburant  TEXT NOT NULL,
              prix_milli INTEGER NOT NULL,
              ts TIMESTAMP NOT NULL,
              PRIMARY KEY (station_id, carburant)
            )
        """)

        # Index utiles (idempotents)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_carburants_station ON carburants(station_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_carburants_date ON carburants(date_import)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_carburants_station_carb ON carburants(station_id, carburant)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_services_station ON services(station_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_services_date ON services(date_import)")
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_carburants_station_fuel_day
            ON carburants(station_id, carburant, (COALESCE(date_maj, date_import)::date))
        """)
        print("Index carburants créé")

        # --- Parsing XML
        tree = ET.parse(str(XML_PATH))
        root = tree.getroot()

        stations = []
        station_rows = []
        carburant_rows = []
        carburant_current_rows = []
        service_rows = []
        missing_maj = 0
        for pdv in root.findall("pdv"):
            station = {
                "id": int(pdv.get("id")),
                "code_postal": pdv.get("cp"),
                "latitude": float(pdv.get("latitude")) / 100000,
                "longitude": float(pdv.get("longitude")) / 100000,
                "ville": (pdv.findtext("ville", default="") or "").strip(),
                "adresse": _clean(pdv.findtext("adresse", default="")),
                "automate": False,
                "services": [],
                "carburants": {},
            }

            horaires = pdv.find("horaires")
            if horaires is not None:
                station["automate"] = (horaires.get("automate-24-24") == "1")

            station["services"] = [
                s.text.strip() for s in pdv.findall("services/service") if s.text
            ]

            for prix in pdv.findall("prix"):
                nom = prix.get("nom")
                val = prix.get("valeur")
                if nom and val:
                    maj_dt = None
                    maj_str = prix.get("maj")
                    if maj_str:
                        try:
                            maj_dt = datetime.strptime(maj_str, "%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            maj_dt = None
                    if maj_dt is None:
                        missing_maj += 1
                    station["carburants"][nom] = {"price": float(val.replace(",", ".")), "maj": maj_dt}

            stations.append(station)
            station_rows.append(
                (
                    station["id"],
                    station["ville"],
                    station["code_postal"],
                    station["adresse"],
                    station["latitude"],
                    station["longitude"],
                    int(station["automate"]),
                )
            )
            for carb, info in station["carburants"].items():
                maj_dt = info.get("maj") or now_naive
                carburant_rows.append((station["id"], carb, info["price"], now_naive, maj_dt))
                prix_milli = int(round(info["price"] * 1000))
                carburant_current_rows.append((station["id"], carb, prix_milli, maj_dt))
            for svc in station["services"]:
                service_rows.append((station["id"], svc, now_naive))

        print(f"[parse] Stations parsées: {len(stations)}")
        print(f"[parse] Carburants sans date_maj fiable: {missing_maj}")

        # --- Upsert stations + dédup au jour pour carburants/services
        if station_rows:
            execute_batch(
                cur,
                """
                INSERT INTO stations (id, ville, code_postal, adresse, latitude, longitude, automate)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                  ville = EXCLUDED.ville,
                  code_postal = EXCLUDED.code_postal,
                  adresse = EXCLUDED.adresse,
                  latitude = EXCLUDED.latitude,
                  longitude = EXCLUDED.longitude,
                  automate = EXCLUDED.automate
                """,
                station_rows,
                page_size=500,
            )

        if carburant_rows:
            execute_values(
                cur,
                """
                INSERT INTO carburants (station_id, carburant, prix, date_import, date_maj) VALUES %s
                ON CONFLICT (station_id, carburant, (COALESCE(date_maj, date_import)::date)) DO UPDATE SET
                  prix = EXCLUDED.prix,
                  date_maj = EXCLUDED.date_maj,
                  date_import = EXCLUDED.date_import
                WHERE EXCLUDED.date_maj >= carburants.date_maj OR carburants.date_maj IS NULL
                """,
                carburant_rows,
                page_size=5000,
            )
            print(f"{len(carburant_rows)} carburants insérés")

        if carburant_current_rows:
            execute_values(
                cur,
                """
                INSERT INTO carburant_current (station_id, carburant, prix_milli, ts) VALUES %s
                ON CONFLICT (station_id, carburant) DO UPDATE SET
                  prix_milli = EXCLUDED.prix_milli,
                  ts = EXCLUDED.ts
                WHERE EXCLUDED.ts >= carburant_current.ts
                """,
                carburant_current_rows,
                page_size=5000,
            )

        if service_rows:
            execute_values(
                cur,
                "INSERT INTO services (station_id, service, date_import) VALUES %s",
                service_rows,
                page_size=5000,
            )
            print(f"{len(service_rows)} services insérés")

        # --- Métriques de fin d'import
        cur.execute("""
            SELECT
              CURRENT_DATE                                  AS today,
              MAX(date_import)                               AS last_import_ts,
              COUNT(*) FILTER (WHERE date_import::date=CURRENT_DATE) AS rows_today,
              COUNT(DISTINCT station_id) FILTER (WHERE date_import::date=CURRENT_DATE) AS stations_today
            FROM carburants
        """)
        today_row = cur.fetchone()
        print(f"[parse] Contrôle carburants: {today_row}")

        # Purge courte durée: 30 jours max, mais seulement si la table n'est pas trop grosse
        cur.execute("SELECT COUNT(*) FROM carburants")
        total_carburants = cur.fetchone()[0] or 0
        if total_carburants <= 1_000_000:
            print("Purge 30j lancée")
            cur.execute("""
                DELETE FROM carburants
                WHERE COALESCE(date_maj, date_import) < NOW() - INTERVAL '30 days'
            """)
            print(f"[parse] Purge carburants 30j OK (total avant={total_carburants})")
        else:
            print("Purge 30j skippée")
            print(
                f"[parse] Purge carburants SKIP (total={total_carburants} > 1_000_000). "
                "Utilise une purge progressive par batch."
            )

        conn.commit()
        conn.close()
        print("[parse] OK: mise en base terminée, logs ci-dessus.")
    except Exception as e:
        print(f"[parse] ERREUR: {e}")
        raise

if __name__ == "__main__":
    main()
