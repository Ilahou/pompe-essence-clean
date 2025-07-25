import os, requests, datetime
from datetime import datetime
from zipfile import ZipFile
from io import BytesIO


# 1. Importer les modules

# 2. Définir l’URL (flux instantané)

# 3. Créer un dossier pour stocker les fichiers si besoin

# 4. Télécharger le fichier XML

# 5. Vérifier que le téléchargement a fonctionné (status_code)

# 6. Sauvegarder le fichier XML localement
#    ➜ Ex : data/prix_essence_2025-07-17_15-20.xml


# Dossier à créer / vérifier
os.makedirs("data/actuel", exist_ok=True)
os.makedirs("data/historique", exist_ok=True)

#récupérer le contenu de l'URL officielle
response = requests.get("https://donnees.roulez-eco.fr/opendata/instantane")

#On vérifie si le code est bien 200 (bonne url)
if response.status_code == 200:
    print("Récupération URL ok : Code 200")
else :  
    print(f"Problème avec la récupération URL : code {response.status_code} ")

# On formate le nom du fichier à la date d'aujourd'hui et au bon format grâce a strftime
date_time_now = datetime.now().strftime("prix_essence_%Y-%m-%d_%H-%M")


# Comme c'est un fichier ZIP (binaire- d'ou l'utilisation de BytesIO) que l'on récupère depuis l'URL
# On utilise la classe ZipFile de la librairie zipfile pour créer un objet manipulable 

fichier_zip = ZipFile(BytesIO(response.content))

nom_fichier_xml = fichier_zip.namelist()[0]
print("Contenu ZIP :", fichier_zip.namelist())

with fichier_zip.open(nom_fichier_xml) as f:
    contenu_xml = f.read()

    with open(f"data/actuel/{nom_fichier_xml}", "wb") as f:
        f.write(contenu_xml)
        print(f"Fichier sauvegardé : data/actuel/{nom_fichier_xml}")

    with open(f"data/historique/{date_time_now}.xml", "wb") as f_hist:
        f_hist.write(contenu_xml)
        print(f"Fichier sauvegardé : data/historique/{date_time_now}.xml")
        





"""
#sauvegarder le fichier XML localement
with open(f"data/{nom_fichier_xml}{date_time_now}.xml", "wb") as f:
    f.write(response.content)
    print(f"Fichier sauvegardé : data/{date_time_now}.xml")

"""
