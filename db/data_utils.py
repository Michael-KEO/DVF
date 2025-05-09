# db_utils.py
"""
Utilitaires pour la connexion à la base de données MySQL.
"""
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

# Charger les variables d'environnement à partir du fichier .env
# Assurez-vous que ce fichier est présent à la racine de votre projet
load_dotenv()

# Configuration de la base de données à partir des variables d'environnement
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME', 'dvf'), # Utilise 'dvf' comme nom de DB par défaut si non spécifié
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    # 'auth_plugin': 'mysql_native_password' # Décommenter si nécessaire
}

def create_db_connection():
    """
    Crée et retourne une connexion à la base de données MySQL.

    Returns:
        mysql.connector.connection.MySQLConnection or None: L'objet de connexion si réussie, None sinon.
    """
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        print("Connexion à la base de données MySQL réussie.")
        return connection
    except Error as e:
        print(f"Erreur de connexion à MySQL: {e}")
        return None

if __name__ == '__main__':
    # Test de la connexion
    conn = create_db_connection()
    if conn and conn.is_connected():
        print(f"Connecté à la base de données '{DB_CONFIG['database']}' sur {DB_CONFIG['host']}.")
        conn.close()
    else:
        print("Échec du test de connexion à la base de données.")