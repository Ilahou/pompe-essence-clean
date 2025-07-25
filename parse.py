import xml.etree.ElementTree as ET
import psycopg2
import os
from datetime import datetime


def main():
    now = datetime.now()
    # Chemin du fichier XML
    xml_path = "data/actuel/PrixCarburants_instantane.xml"

    # Lecture + décodage CP1252
    tree = ET.parse(xml_path)
    root = tree.getroot()

    stations = []


    # Parcours de l'arbre XML

    for pdv in root.findall("pdv"):
        station = {
            "id": int(pdv.get("id")),
            "code_postal": pdv.get("cp"),
            "latitude": float(pdv.get("latitude")) / 100000,
            "longitude": float(pdv.get("longitude")) / 100000,
            "ville": pdv.findtext("ville", default="").strip(),
            "automate": False,
            "services": [],
            "carburants": {}
        }

        # Automate 24/24
        horaires = pdv.find("horaires")
        if horaires is not None:
            station["automate"] = (horaires.get("automate-24-24") == "1")

        # Services
        station["services"] = [
            s.text.strip() for s in pdv.findall("services/service") if s.text
        ]

        # Carburants
        for prix in pdv.findall("prix"):
            nom = prix.get("nom")
            val = prix.get("valeur")
            if nom and val:
                station["carburants"][nom] = float(val.replace(",", "."))

        stations.append(station)

    # Affichage lisible des 5 premières stations
    for station in stations[:5]:
        print("-" * 40)
        print(f"ID          : {station['id']}")
        print(f"Ville       : {station['ville']}")
        print(f"CP          : {station['code_postal']}")
        print(f"Coords      : {station['latitude']}, {station['longitude']}")
        print(f"Automate24  : {station['automate']}")
        print(f"Services    : {station['services']}")
        print(f"Carburants  : {station['carburants']}")
    print(f"\nNombre total de stations : {len(stations)}")

    """
    stations = []

    for elem in root.iter('pdv'):
        stations_dict = {}
        stations_dict["id"] = elem.attrib.get("id")
        stations_dict["code_postal" ] = elem.attrib.get("cp")
        stations_dict["latitude"] = elem.attrib.get("latitude")
        stations_dict["longitude"] = elem.attrib.get("longitude")

        if elem.find("ville") is not None :
            ville = elem.find("ville")
            ville = ville.text
            stations_dict["ville"] = ville

        horaires = elem.find("horaires")
        if horaires is not None:
            automate = horaires.attrib.get("automate-24-24")
            if automate == "1":
                stations_dict["automate"] = True
            elif automate == "":
                stations_dict["automate"] = False
            else:
                stations_dict["automate"] = None

            
        
        service_liste = []
        for service in elem.findall("services/service"):
            service_name = service.text
            service_liste.append(service_name)
            stations_dict["services"] = service_liste

        carburant_liste = {}
        for carburant in elem.findall("prix"):
            carburant_name = carburant.attrib.get("nom")
            carburant_prix = carburant.attrib.get("valeur")
            carburant_liste[carburant_name] = carburant_prix
            stations_dict["carburants"] = carburant_liste


        stations.append(stations_dict)
    print(stations)
    """

        





    # analyse statistique du document
    total_stations = 0
    stations_avec_services = 0
    stations_avec_horaires = 0
    stations_avec_automate = 0
    stations_avec_detail_horaire_jour = 0
    stations_avec_SP98 = 0
    stations_avec_SP95 = 0
    stations_avec_gazole = 0
    stations_avec_E10 = 0
    stations_avec_E85 = 0
    stations_avec_GPLc = 0


    for pdv in root.iter("pdv"):
        total_stations += 1
        if pdv.find("services") is not None:
            stations_avec_services += 1
        if pdv.find("horaires") is not None:
            stations_avec_horaires += 1
        for horaires in pdv.iter("horaires"):
            if horaires.attrib.get("automate-24-24") == "1":
                stations_avec_automate += 1
            for jour in horaires.findall("jour"):
                if jour.find("horaire") is not None:
                    stations_avec_detail_horaire_jour += 1
                    break
        for carburant in pdv.iter("prix"):
            if carburant.attrib.get("nom") == "SP98":
                stations_avec_SP98 += 1
            elif carburant.attrib.get("nom") == "SP95":
                stations_avec_SP95 += 1
            elif carburant.attrib.get("nom") == "Gazole":
                stations_avec_gazole += 1
            elif carburant.attrib.get("nom") == "E10":
                stations_avec_E10 += 1 
            elif carburant.attrib.get("nom") == "E85":
                stations_avec_E85 += 1
            elif carburant.attrib.get("nom") == "GPLc":
                stations_avec_GPLc += 1
        

    type_carburant = []

    for pdv in root.findall("pdv"):
        for carburant in pdv.findall("prix"):
            nom = carburant.attrib.get("nom")
            if nom not in type_carburant:
                type_carburant.append(nom)

    type_services = []

    for pdv in root.findall("pdv"):
        for services in pdv.findall("services"):
            for service in services.findall("service"):
                nom_service = service.text
                if nom_service not in type_services:
                    type_services.append(nom_service)


    print(f"Total de stations : {total_stations}")
    print(f"Total de stations avec services : {stations_avec_services}")
    print(f"Total de stations avec horaires : {stations_avec_horaires}")
    print(f"Total de stations avec automate 24/24 : {stations_avec_automate}")
    print(f"Total de stations avec horaire par jour : {stations_avec_detail_horaire_jour}")
    print(f"Total de stations avec SP98 : {stations_avec_SP98}")
    print(f"Total de stations avec SP95 : {stations_avec_SP95}")
    print(f"Total de stations avec Gazole : {stations_avec_gazole}")
    print(f"Total de stations avec E10 : {stations_avec_E10}")
    print(f"Total de stations avec E85 : {stations_avec_E85}")
    print(f"Total de stations avec GPLc : {stations_avec_GPLc}")
    print()
    print("type de carburant disponibles : " + str(type_carburant))
    print()
    print("type de services disponibles : " + str(type_services))


    # Mise en BDD
    try:
        # Récupération automatique des credentials Railway
        DB_HOST = os.getenv("PGHOST")
        DB_PORT = os.getenv("PGPORT")
        DB_NAME = os.getenv("PGDATABASE")
        DB_USER = os.getenv("PGUSER")
        DB_PASS = os.getenv("PGPASSWORD")

        # Connexion à la base PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )  

        curseur = conn.cursor() 

        curseur.execute("""CREATE TABLE IF NOT EXISTS stations (
            id INTEGER PRIMARY KEY,
            code_postal TEXT,
            ville TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            automate INTEGER,
            date_import TIMESTAMP
                        )
        """)

        curseur.execute("""CREATE TABLE IF NOT EXISTS carburants (
                            station_id INTEGER REFERENCES stations(id), 
                            carburant TEXT, 
                            prix DOUBLE PRECISION,
                            date_import TIMESTAMP )
        """)

        curseur.execute("""CREATE TABLE IF NOT EXISTS services (
                            station_id INTEGER REFERENCES stations(id), 
                            service TEXT,
                            date_import TIMESTAMP )
                        
        """)

        for station in stations:
            id = station["id"]
            ville = station["ville"]
            code_postal = station["code_postal"]
            latitude = station["latitude"]
            longitude = station["longitude"]
            automate = int(station["automate"]) 

            curseur.execute(
                "INSERT INTO stations (id, ville, code_postal, latitude, longitude, automate, date_import) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (id, ville, code_postal, latitude, longitude, automate, now)
            )

            for carburant in station["carburants"]:
                curseur.execute(
                    "INSERT INTO carburants (station_id, carburant, prix, date_import) VALUES (%s, %s, %s, %s)",
                    (id, carburant, station["carburants"][carburant], now)
                )
            for service in station["services"]:
                curseur.execute(
                    "INSERT INTO services (station_id, service, date_import) VALUES (%s, %s,%s)",
                    (id, service)
                )


        




        conn.commit()
        conn.close()
        print("Mise en base terminée, aucun doublon, tout est propre !")
    except Exception as e:
        print(f"Une erreur s'est produite lors de la mise en base de données : {e}")

# ⬇️ Ce bloc permet d'exécuter getxml.py tout seul (terminal) OU en import
if __name__ == "__main__":
    main()