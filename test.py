import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import os
import warnings
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Suppression des avertissements DtypeWarning de pandas
warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

# Configuration de la base de données à partir des variables d'environnement
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
}

print("Connexion à la base de données réussie : nom de la base de données :", DB_CONFIG['database'])
