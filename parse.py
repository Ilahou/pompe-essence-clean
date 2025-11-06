# parse.py
import xml.etree.ElementTree as ET
import psycopg2
import os
import re
from datetime import datetime, timezone, date
from pathlib import Path

def _clean(txt: str) -> str:
    return re.sub(r'\s+', ' ', (txt or '').strip())

def main():
    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()

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

    # --- Parsing XML
    tree = ET.parse(str(XML_PATH))
    root = tree.getroot()

    stations = []
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
                station["carburants"][nom] = float(val.replace(",", "."))

        stations.append(station)

    print(f"[parse] Stations parsées: {len(stations)}")

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
              date_import TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS services (
              station_id INTEGER REFERENCES stations(id),
              service    TEXT,
              date_import TIMESTAMP
            )
        """)

        # Index utiles (idempotents)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_carburants_station ON carburants(station_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_carburants_date ON carburants(date_import)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_carburants_station_carb ON carburants(station_id, carburant)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_services_station ON services(station_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_services_date ON services(date_import)")

        # --- Upsert stations + dédup au jour pour carburants/services
        for st in stations:
            sid = st["id"]
            ville = st["ville"]
            cp = st["code_postal"]
            lat = st["latitude"]
            lon = st["longitude"]
            adr = st["adresse"]
            auto = int(st["automate"])

            # Upsert station
            cur.execute(
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
                (sid, ville, cp, adr, lat, lon, auto)
            )

            # Carburants : 1 ligne max par (jour, station, carburant)
            for carb, price in st["carburants"].items():
                cur.execute(
                    "DELETE FROM carburants WHERE station_id=%s AND carburant=%s AND date_import::date=%s",
                    (sid, carb, today)
                )
                cur.execute(
                    "INSERT INTO carburants (station_id, carburant, prix, date_import) VALUES (%s, %s, %s, %s)",
                    (sid, carb, price, now_utc.replace(tzinfo=None))  # on reste en TIMESTAMP sans TZ
                )

            # Services : idem (si tu veux un historique quotidien)
            for svc in st["services"]:
                cur.execute(
                    "DELETE FROM services WHERE station_id=%s AND service=%s AND date_import::date=%s",
                    (sid, svc, today)
                )
                cur.execute(
                    "INSERT INTO services (station_id, service, date_import) VALUES (%s, %s, %s)",
                    (sid, svc, now_utc.replace(tzinfo=None))
                )

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

        conn.commit()
        conn.close()
        print("[parse] OK: mise en base terminée (dédup jour), logs ci-dessus.")
    except Exception as e:
        print(f"[parse] ERREUR BDD: {e}")
        raise

if __name__ == "__main__":
    main()
