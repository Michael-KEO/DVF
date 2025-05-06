"""import requests
from datetime import datetime

# Récupérer les données de l'API
loc_url = "https://dvf-api-5wd5.onrender.com/localisations"
mut_url = "https://dvf-api-5wd5.onrender.com/mutations"
bien_url = "https://dvf-api-5wd5.onrender.com/biens"

localisations = requests.get(loc_url).json()
mutations = requests.get(mut_url).json()
biens = requests.get(bien_url).json()

# Dictionnaires pour accès rapide
loc_dict = {loc['ID_Localisation']: loc for loc in localisations}
mut_dict = {mut['ID_Mutation']: mut for mut in mutations}

# Fusionner les données
results = []
for bien in biens:
    id_mut = bien['ID_Mutation']
    id_loc = bien['ID_Localisation']

    mutation = mut_dict.get(id_mut)
    localisation = loc_dict.get(id_loc)

    if mutation and localisation:
        try:
            date = datetime.fromisoformat(mutation['Date_mutation'])
            if localisation['Code_departement'] == '75' and date.year == 2020:
                merged = {**bien, **mutation, **localisation}
                results.append(merged)
        except:
            continue

print(results)
print(len(results))"""

import mysql.connector

# Configuration de la base de données
DB_CONFIG = {
    'host': '172.25.16.1',
    'database': 'dvf_test',
    'user': 'root',
    'password': 'Dertyxx@6624!'
}

connection = mysql.connector.connect(**DB_CONFIG)
if connection.is_connected():
    print("Connexion à MySQL réussie")
else:
    print("Erreur de connexion à MySQL")

connection.close()

