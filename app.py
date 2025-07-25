import sqlite3
conn = sqlite3.connect('db/PrixCarburants_instantane.db')
cur = conn.cursor()
cur.execute("SELECT * FROM stations")
print(cur.fetchone())
conn.close()

#initialisation d'un API flask
from flask import Flask

# On crée notre application web
app = Flask(__name__)

# On crée une route "/" (racine du site)
@app.route("/")
def home():
    return "Bienvenue sur mon API Flask !"

if __name__ == "__main__":
    app.run(debug=True)