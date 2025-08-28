# main.py
import os
from dotenv import load_dotenv

load_dotenv()

# Étapes existantes
import getxml       # télécharge le XML officiel
import parse        # parse + upsert en base

# Nouvel enrichissement marques
import enrich_brands

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
    for r in rows:
        sid, ville, bname, bshort = r
        print(f"ID: {sid:<8} | Ville: {ville:<20} | Brand: {bname or '—'} | Short: {bshort or '—'}")
    print("-" * 60)

def main():
    # 1) Récupère le XML du jour (télécharge & sauvegarde)
    getxml.main()

    # 2) Parse + upsert des stations/services/prix dans la base
    parse.main()

    # 3) Enrichit les marques (par défaut only_missing=True)
    #    -> mets limit=None pour traiter tout le monde
    enrich_brands.main(limit=None, max_workers=12, only_missing=True)

    # 4) Affichage de contrôle
    print_sample_with_brands(n=8)

if __name__ == "__main__":
    main()
