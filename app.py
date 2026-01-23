from flask import Flask, jsonify, request
from psycopg2.extras import RealDictCursor, register_default_json, register_default_jsonb

import enrich_brands

# On crée notre application web
app = Flask(__name__)
register_default_json(loads=None, globally=True)
register_default_jsonb(loads=None, globally=True)

# On crée une route "/" (racine du site)
@app.route("/")
def home():
    return "Bienvenue sur mon API Flask !"

@app.route("/stations")
def stations():
    limit = request.args.get("limit", type=int)
    if limit is not None and limit <= 0:
        return jsonify({"error": "limit must be a positive integer"}), 400

    sql = """
        SELECT
          s.*,
          COALESCE(
            json_agg(
              json_build_object(
                'carburant', c.carburant,
                'prix_euro', c.prix_milli / 1000.0,
                'ts', c.ts
              )
            ) FILTER (WHERE c.carburant IS NOT NULL),
            '[]'::json
          ) AS carburants
        FROM stations s
        LEFT JOIN carburant_current c
          ON c.station_id = s.id
        GROUP BY s.id
        ORDER BY s.id
    """
    if limit:
        sql += " LIMIT %s"

    conn = enrich_brands.get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if limit:
                cur.execute(sql, (limit,))
            else:
                cur.execute(sql)
            rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()

if __name__ == "__main__":
    app.run(debug=True)
