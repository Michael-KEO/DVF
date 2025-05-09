import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import os
import warnings
import dotenv

# Chargement des variables d'environnement
dotenv.load_dotenv()

# Suppression des avertissements pandas
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

# Configuration de la base de données
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
}

# Chemin vers le dossier contenant les fichiers CSV
CSV_FOLDER = 'csv_originaux'


def create_db_connection():
    """Crée une connexion à la base de données MySQL"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        print("Connexion à MySQL réussie")
        return connection
    except Error as e:
        print(f"Erreur de connexion à MySQL: {e}")
        return None


def clean_numeric(value):
    """Nettoie et convertit une valeur numérique"""
    if pd.isna(value) or value == '':
        return None
    try:
        # Gère les nombres avec virgule comme séparateur décimal
        if isinstance(value, str):
            value = value.replace(',', '.')
        return float(value)
    except (ValueError, TypeError):
        return None


def process_csv_files(folder_path):
    """Traite tous les fichiers CSV dans le dossier spécifié"""
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    all_data = pd.DataFrame()

    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        print(f"Traitement du fichier: {file_path}")

        try:
            # Lecture du CSV
            df = pd.read_csv(file_path, low_memory=False, dtype={
                'id_mutation': str,
                'date_mutation': str,
                'numero_disposition': str,
                'valeur_fonciere': str,
                'lot1_numero': str,
                'lot2_numero': str,
                'lot3_numero': str,
                'lot4_numero': str,
                'lot5_numero': str
            })

            # Affichage de contrôle
            print(f"Nombre de lignes lues dans {file}: {len(df)}")

            # Nettoyage des données numériques
            df['valeur_fonciere'] = df['valeur_fonciere'].apply(clean_numeric)
            df['surface_reelle_bati'] = df['surface_reelle_bati'].apply(clean_numeric)
            df['surface_terrain'] = df['surface_terrain'].apply(clean_numeric)
            df['nombre_pieces_principales'] = pd.to_numeric(df['nombre_pieces_principales'], errors='coerce')
            df['longitude'] = df['longitude'].apply(clean_numeric)
            df['latitude'] = df['latitude'].apply(clean_numeric)

            # Conversion de la date avec le bon format
            def convert_date(date_str):
                try:
                    if pd.isna(date_str):
                        return None
                    # Si la date est déjà au format YYYY-MM-DD
                    if '-' in str(date_str):
                        return pd.to_datetime(date_str)
                    # Si la date est au format DD/MM/YYYY
                    elif '/' in str(date_str):
                        return pd.to_datetime(date_str, format='%d/%m/%Y')
                    return None
                except Exception as e:
                    print(f"Erreur de conversion de date pour {date_str}: {e}")
                    return None

            df['date_mutation'] = df['date_mutation'].apply(convert_date)

            # Conservation des lignes avec date valide
            df = df[df['date_mutation'].notna()]

            # Concaténation
            all_data = pd.concat([all_data, df], ignore_index=True)

        except Exception as e:
            print(f"Erreur lors de la lecture du fichier {file_path}: {e}")
            continue

    print(f"Nombre total de lignes valides: {len(all_data)}")
    return all_data

def insert_data_to_db(data):
    """Insère les données dans la base de données MySQL"""

    # Vérification des données avant insertion
    print(f"Types des colonnes:\n{data.dtypes}")


    connection = create_db_connection()
    if connection is None:
        return

    cursor = connection.cursor()

    try:
        # Dictionnaires pour le cache
        localisations_cache = {}
        biens_cache = {}
        mutations_cache = set()

        # Compteur pour le suivi
        total_rows = len(data)
        batch_size = 10000
        inserted_rows = 0

        for index, row in data.iterrows():
            # Conversion de la date pour MySQL
            date_mutation = row['date_mutation'].to_pydatetime() if pd.notna(row['date_mutation']) else None

            # 1. Insérer la MUTATION
            if row['id_mutation'] not in mutations_cache:
                mutation_query = """
                                 INSERT INTO MUTATION (ID_Mutation, Date_mutation, Numero_disposition, Nature_mutation)
                                 VALUES (%s, %s, %s, %s) \
                                 """
                cursor.execute(mutation_query, (
                    str(row['id_mutation']),
                    date_mutation,
                    int(row['numero_disposition']) if pd.notna(row['numero_disposition']) else None,
                    str(row['nature_mutation']) if pd.notna(row['nature_mutation']) else None
                ))
                mutations_cache.add(row['id_mutation'])

            # 2. Insérer la LOCALISATION
            loc_key = (
                str(row['code_departement']) if pd.notna(row['code_departement']) else None,
                str(row['code_postal']) if pd.notna(row['code_postal']) else None,
                str(row['code_commune']) if pd.notna(row['code_commune']) else None,
                str(row['nom_commune']) if pd.notna(row['nom_commune']) else None,
                clean_numeric(row['longitude']),
                clean_numeric(row['latitude'])
            )

            if loc_key not in localisations_cache:
                localisation_query = """
                                     INSERT INTO LOCALISATION (Adresse_numero, Adresse_suffixe, Adresse_nom_voie, \
                                                               Code_departement, Code_postal, Code_commune, Nom_commune, \
                                                               Longitude, Latitude, Date_localisation)
                                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                                     """
                cursor.execute(localisation_query, (
                    str(row['adresse_numero']) if pd.notna(row['adresse_numero']) else None,
                    str(row['adresse_suffixe']) if pd.notna(row['adresse_suffixe']) else None,
                    str(row['adresse_nom_voie']) if pd.notna(row['adresse_nom_voie']) else None,
                    str(row['code_departement']) if pd.notna(row['code_departement']) else None,
                    str(row['code_postal']) if pd.notna(row['code_postal']) else None,
                    str(row['code_commune']) if pd.notna(row['code_commune']) else None,
                    str(row['nom_commune']) if pd.notna(row['nom_commune']) else None,
                    clean_numeric(row['longitude']),
                    clean_numeric(row['latitude']),
                    date_mutation
                ))
                localisation_id = cursor.lastrowid
                localisations_cache[loc_key] = localisation_id
            else:
                localisation_id = localisations_cache[loc_key]

            # 3. Insérer le BIEN
            bien_key = (
                str(row['id_parcelle']) if pd.notna(row['id_parcelle']) else None,
                str(row['type_local']) if pd.notna(row['type_local']) else None,
                clean_numeric(row['surface_reelle_bati']),
                int(row['nombre_pieces_principales']) if pd.notna(row['nombre_pieces_principales']) else None,
                str(row['lot1_numero']) if pd.notna(row['lot1_numero']) else None,
                clean_numeric(row['lot1_surface_carrez'])
            )


            if bien_key and bien_key not in biens_cache:
                bien_query = """
                             INSERT INTO BIEN (ID_Parcelle, Type_local, Surface_reelle_bati,
                                               Surface_terrain, Nombre_pieces_principales, ID_Localisation)
                             VALUES (%s, %s, %s, %s, %s, %s)
                             """
                cursor.execute(bien_query, (
                    bien_key[0],  # id_parcelle
                    bien_key[1],  # type_local
                    bien_key[2],  # surface_reelle_bati
                    clean_numeric(row['surface_terrain']),
                    bien_key[3],  # nombre_pieces_principales
                    localisation_id
                ))
                bien_id = cursor.lastrowid
                biens_cache[bien_key] = bien_id
            else:
                bien_id = biens_cache.get(bien_key)

            # 4. Insérer la relation MUTATION_BIEN
            if bien_id and row['id_mutation']:
                mutation_bien_query = """
                                      INSERT \
                                      IGNORE INTO MUTATION_BIEN (ID_Mutation, ID_Bien, Valeur_fonciere)
                                      VALUES (%s, %s, %s) \
                                      """
                cursor.execute(mutation_bien_query, (
                    str(row['id_mutation']),
                    bien_id,
                    clean_numeric(row['valeur_fonciere'])
                ))

            # 5. Insérer les LOTS
            if bien_id:
                for lot_num in range(1, 6):
                    # Utilisation du bon format pour les noms de colonnes
                    lot_numero = str(row[f'lot{lot_num}_numero']) if pd.notna(row[f'lot{lot_num}_numero']) else None
                    lot_surface = clean_numeric(row[f'lot{lot_num}_surface_carrez'])

                    if lot_numero and lot_surface:
                        lot_query = """
                                    INSERT INTO LOT (ID_Bien, Numero_lot, Surface_carree)
                                    VALUES (%s, %s, %s)
                                    """
                        cursor.execute(lot_query, (
                            bien_id,
                            lot_numero,
                            lot_surface
                        ))

            # Commit par lots pour améliorer les performances
            inserted_rows += 1
            if inserted_rows % batch_size == 0:
                connection.commit()
                print(f"Progression: {inserted_rows}/{total_rows} ({inserted_rows / total_rows:.1%})")

        connection.commit()
        print(f"Importation terminée avec succès! {inserted_rows} lignes traitées.")

    except Error as e:
        print(f"Erreur lors de l'insertion des données (ligne {inserted_rows}): {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


def main():
    print("Début du traitement des fichiers CSV...")
    data = process_csv_files(CSV_FOLDER)

    if not data.empty:
        print(f"Nombre total d'enregistrements à traiter: {len(data)}")
        insert_data_to_db(data)
    else:
        print("Aucune donnée à importer.")


if __name__ == "__main__":
    main()