# populate_db.py
"""
Script pour peupler une base de données MySQL à partir de fichiers CSV contenant des données DVF.

Ce script effectue les opérations suivantes :
1. Lit les fichiers CSV d'un dossier spécifié.
2. Nettoie et transforme les données (types numériques, dates).
3. Se connecte à une base de données MySQL.
4. Insère les données transformées dans les tables appropriées (MUTATION, LOCALISATION, BIEN, LOT, MUTATION_BIEN),
   en utilisant des caches pour éviter les doublons d'entités dimensionnelles et en gérant les relations.
Les configurations de la base de données sont chargées à partir de variables d'environnement.
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import os
import warnings
from dotenv import load_dotenv

# Charger les variables d'environnement à partir du fichier .env
load_dotenv()

# Suppression des avertissements DtypeWarning de pandas qui peuvent survenir avec des colonnes mixtes.
# Il est préférable de spécifier les dtypes à la lecture si possible.
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

# Configuration de la base de données à partir des variables d'environnement
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    # 'auth_plugin': 'mysql_native_password' # Décommenter si nécessaire pour certains environnements MySQL
}

# Chemin vers le dossier contenant les fichiers CSV à importer
CSV_FOLDER = 'csv_originaux'  # Assurez-vous que ce dossier existe à la racine du projet ou spécifiez un chemin absolu

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

def clean_numeric(value):
    """
    Nettoie et convertit une valeur en type numérique (float).
    Gère les valeurs manquantes (NaN, chaînes vides) et les nombres avec virgule comme séparateur décimal.

    Args:
        value (any): La valeur à nettoyer et convertir.

    Returns:
        float or None: La valeur convertie en float, ou None si la conversion échoue ou si la valeur est manquante.
    """
    if pd.isna(value) or str(value).strip() == '':
        return None
    try:
        if isinstance(value, str):
            # Remplace la virgule par un point pour les décimaux de style français
            value_cleaned = value.replace(',', '.')
        else:
            value_cleaned = value
        return float(value_cleaned)
    except (ValueError, TypeError):
        # print(f"Avertissement: Impossible de convertir '{value}' en numérique.") # Décommenter pour le débogage
        return None

def convert_date(date_str):
    """
    Convertit une chaîne de caractères représentant une date en objet datetime.
    Tente de parser les formats 'YYYY-MM-DD' et 'DD/MM/YYYY'.

    Args:
        date_str (str): La chaîne de caractères de la date.

    Returns:
        datetime.datetime or None: L'objet datetime si la conversion est réussie, None sinon.
    """
    if pd.isna(date_str) or str(date_str).strip() == '':
        return None
    try:
        # Si la date contient des '-', on suppose YYYY-MM-DD (ou un format similaire acceptable par to_datetime)
        if '-' in str(date_str):
            return pd.to_datetime(date_str).to_pydatetime() # Convertit en datetime Python natif
        # Si la date contient des '/', on suppose DD/MM/YYYY
        elif '/' in str(date_str):
            return pd.to_datetime(date_str, format='%d/%m/%Y').to_pydatetime()
        print(f"Format de date non reconnu pour : {date_str}")
        return None
    except Exception as e:
        print(f"Erreur de conversion de date pour '{date_str}': {e}")
        return None

def process_csv_files(folder_path):
    """
    Lit tous les fichiers CSV présents dans le dossier spécifié, les nettoie et les concatène.

    Args:
        folder_path (str): Le chemin vers le dossier contenant les fichiers CSV.

    Returns:
        pd.DataFrame: Un DataFrame pandas contenant toutes les données des fichiers CSV,
                      nettoyées et prêtes pour l'importation. Retourne un DataFrame vide si une erreur se produit.
    """
    if not os.path.isdir(folder_path):
        print(f"Erreur: Le dossier spécifié '{folder_path}' n'existe pas.")
        return pd.DataFrame()

    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if not csv_files:
        print(f"Aucun fichier CSV trouvé dans le dossier '{folder_path}'.")
        return pd.DataFrame()

    all_data = [] # Liste pour stocker les DataFrames individuels

    print(f"Début du traitement des fichiers CSV du dossier: {folder_path}")
    for file_name in csv_files:
        file_path = os.path.join(folder_path, file_name)
        print(f"Traitement du fichier: {file_path}")

        try:
            # Spécification des dtypes pour les colonnes critiques pour éviter les DtypeWarning
            # et assurer une lecture correcte des identifiants et autres champs.
            # Les autres colonnes seront inférées par pandas.
            df = pd.read_csv(file_path, low_memory=False, dtype={
                'id_mutation': str,
                'date_mutation': str, # Lu comme str pour conversion personnalisée
                'numero_disposition': str, # Peut être numérique mais lu comme str pour flexibilité
                'valeur_fonciere': str, # Lu comme str pour nettoyage personnalisé (virgules)
                'code_postal': str,
                'code_commune': str,
                'code_departement': str,
                'id_parcelle': str,
                'lot1_numero': str, 'lot2_numero': str, 'lot3_numero': str,
                'lot4_numero': str, 'lot5_numero': str
            })
            print(f"Nombre de lignes lues dans {file_name}: {len(df)}")

            # Standardisation des noms de colonnes (exemple : enlever les espaces, mettre en minuscules)
            df.columns = df.columns.str.strip().str.lower()
            # Renommer des colonnes si nécessaire pour correspondre à la base de données
            # Exemple: df.rename(columns={'ancien_nom': 'nouveau_nom'}, inplace=True)

            # Nettoyage et conversion des colonnes numériques
            numeric_cols = ['valeur_fonciere', 'surface_reelle_bati', 'surface_terrain', 'longitude', 'latitude']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].apply(clean_numeric)
                else:
                    print(f"Avertissement: Colonne '{col}' non trouvée dans {file_name}. Elle sera ignorée.")


            if 'nombre_pieces_principales' in df.columns:
                df['nombre_pieces_principales'] = pd.to_numeric(df['nombre_pieces_principales'], errors='coerce').astype('Int64') # Permet les NaN entiers
            else:
                print(f"Avertissement: Colonne 'nombre_pieces_principales' non trouvée dans {file_name}.")


            # Conversion de la colonne 'date_mutation'
            if 'date_mutation' in df.columns:
                df['date_mutation'] = df['date_mutation'].apply(convert_date)
                # Conserver uniquement les lignes avec une date de mutation valide
                original_rows = len(df)
                df.dropna(subset=['date_mutation'], inplace=True)
                if len(df) < original_rows:
                    print(f"{original_rows - len(df)} lignes supprimées de {file_name} en raison de dates de mutation invalides.")
            else:
                print(f"Avertissement: Colonne 'date_mutation' non trouvée dans {file_name}. Les dates ne seront pas traitées.")


            all_data.append(df)

        except Exception as e:
            print(f"Erreur majeure lors de la lecture ou du traitement du fichier {file_path}: {e}")
            continue # Passe au fichier suivant en cas d'erreur

    if not all_data:
        print("Aucune donnée n'a pu être chargée à partir des fichiers CSV.")
        return pd.DataFrame()

    final_df = pd.concat(all_data, ignore_index=True)
    print(f"Nombre total de lignes valides concaténées: {len(final_df)}")
    return final_df

def insert_data_to_db(data):
    """
    Insère les données du DataFrame dans la base de données MySQL.
    Gère les relations entre les tables et utilise des caches pour éviter les doublons.

    Args:
        data (pd.DataFrame): Le DataFrame contenant les données à insérer.
    """
    connection = create_db_connection()
    if connection is None:
        print("Échec de l'insertion : connexion à la base de données non établie.")
        return

    cursor = connection.cursor()

    # Dictionnaires utilisés comme caches pour stocker les ID des entités déjà insérées
    # et éviter les violations de contrainte d'unicité ou les insertions redondantes.
    localisations_cache = {}  # Cache pour les ID de LOCALISATION
    biens_cache = {}          # Cache pour les ID de BIEN
    mutations_cache = set()   # Ensemble pour suivre les ID_Mutation déjà insérés

    total_rows = len(data)
    batch_size = 10000  # Nombre de lignes à insérer avant chaque commit
    inserted_rows_count = 0
    skipped_due_to_error = 0

    print(f"Début de l'insertion de {total_rows} lignes dans la base de données...")

    try:
        for index, row in data.iterrows():
            try:
                # Convertir la date de mutation en objet datetime Python natif si ce n'est pas déjà fait
                date_mutation_val = row.get('date_mutation')
                # La conversion est maintenant faite dans process_csv_files, donc date_mutation_val devrait être un datetime ou None

                # 1. Insérer dans la table MUTATION (si pas déjà présente)
                # Utilise un cache pour éviter d'insérer des mutations en double.
                id_mutation_val = str(row.get('id_mutation')) if pd.notna(row.get('id_mutation')) else None
                if id_mutation_val and id_mutation_val not in mutations_cache:
                    mutation_query = """
                        INSERT INTO MUTATION (ID_Mutation, Date_mutation, Numero_disposition, Nature_mutation)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE Date_mutation = VALUES(Date_mutation); 
                        -- ON DUPLICATE KEY UPDATE est utile si on relance le script avec des données potentiellement mises à jour
                    """
                    cursor.execute(mutation_query, (
                        id_mutation_val,
                        date_mutation_val,
                        int(row.get('numero_disposition')) if pd.notna(row.get('numero_disposition')) else None,
                        str(row.get('nature_mutation')) if pd.notna(row.get('nature_mutation')) else None
                    ))
                    mutations_cache.add(id_mutation_val)

                # 2. Insérer dans la table LOCALISATION (si pas déjà présente)
                # Clé de cache pour la localisation, basée sur des attributs identifiants.
                loc_key_fields = [
                    'code_departement', 'code_postal', 'code_commune', 'nom_commune',
                    'adresse_numero', 'adresse_suffixe', 'adresse_nom_voie', # Ajout d'éléments d'adresse pour unicité
                    'longitude', 'latitude'
                ]
                loc_key_values = [str(row.get(f)) if pd.notna(row.get(f)) else None for f in loc_key_fields[:7]] # Champs textuels
                loc_key_values.extend([row.get(f) for f in loc_key_fields[7:]]) # Longitude, Latitude (déjà nettoyés en float)
                loc_key = tuple(loc_key_values)


                if loc_key not in localisations_cache:
                    localisation_query = """
                        INSERT INTO LOCALISATION (
                            Adresse_numero, Adresse_suffixe, Adresse_nom_voie,
                            Code_departement, Code_postal, Code_commune, Nom_commune,
                            Longitude, Latitude, Date_localisation
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
                    cursor.execute(localisation_query, (
                        str(row.get('adresse_numero')) if pd.notna(row.get('adresse_numero')) else None,
                        str(row.get('adresse_suffixe')) if pd.notna(row.get('adresse_suffixe')) else None,
                        str(row.get('adresse_nom_voie')) if pd.notna(row.get('adresse_nom_voie')) else None,
                        str(row.get('code_departement')) if pd.notna(row.get('code_departement')) else None,
                        str(row.get('code_postal')) if pd.notna(row.get('code_postal')) else None,
                        str(row.get('code_commune')) if pd.notna(row.get('code_commune')) else None,
                        str(row.get('nom_commune')) if pd.notna(row.get('nom_commune')) else None,
                        row.get('longitude'), # Doit être float ou None
                        row.get('latitude'),  # Doit être float ou None
                        date_mutation_val # Utilise la date de mutation comme date de localisation par défaut
                    ))
                    localisation_id = cursor.lastrowid
                    localisations_cache[loc_key] = localisation_id
                else:
                    localisation_id = localisations_cache[loc_key]

                # 3. Insérer dans la table BIEN (si pas déjà présent et si localisation_id est valide)
                bien_id = None
                if localisation_id:
                    # Clé de cache pour le bien.
                    bien_key_fields = ['id_parcelle', 'type_local', 'surface_reelle_bati', 'nombre_pieces_principales']
                    bien_key_values = [str(row.get(f)) if pd.notna(row.get(f)) and f != 'nombre_pieces_principales' else \
                                       (int(row.get(f)) if pd.notna(row.get(f)) else None) if f == 'nombre_pieces_principales' else \
                                       row.get(f) # pour surface_reelle_bati (déjà float)
                                       for f in bien_key_fields]
                    # Ajout de l'ID de localisation à la clé pour s'assurer de l'unicité par rapport à la localisation
                    bien_key = tuple(bien_key_values + [localisation_id])


                    if bien_key not in biens_cache:
                        bien_query = """
                            INSERT INTO BIEN (
                                ID_Parcelle, Type_local, Surface_reelle_bati,
                                Surface_terrain, Nombre_pieces_principales, ID_Localisation
                            ) VALUES (%s, %s, %s, %s, %s, %s);
                        """
                        cursor.execute(bien_query, (
                            str(row.get('id_parcelle')) if pd.notna(row.get('id_parcelle')) else None,
                            str(row.get('type_local')) if pd.notna(row.get('type_local')) else None,
                            row.get('surface_reelle_bati'), # float ou None
                            row.get('surface_terrain'),     # float ou None
                            int(row.get('nombre_pieces_principales')) if pd.notna(row.get('nombre_pieces_principales')) else None,
                            localisation_id
                        ))
                        bien_id = cursor.lastrowid
                        biens_cache[bien_key] = bien_id
                    else:
                        bien_id = biens_cache[bien_key]

                # 4. Insérer dans la table de liaison MUTATION_BIEN
                # `INSERT IGNORE` est utilisé pour éviter les erreurs si la paire (ID_Mutation, ID_Bien) existe déjà.
                if id_mutation_val and bien_id:
                    mutation_bien_query = """
                        INSERT IGNORE INTO MUTATION_BIEN (ID_Mutation, ID_Bien, Valeur_fonciere)
                        VALUES (%s, %s, %s);
                    """
                    cursor.execute(mutation_bien_query, (
                        id_mutation_val,
                        bien_id,
                        row.get('valeur_fonciere') # float ou None
                    ))

                # 5. Insérer dans la table LOTS (si des données de lot existent)
                if bien_id:
                    for i in range(1, 6): # Pour lot1_ à lot5_
                        lot_numero_col = f'lot{i}_numero'
                        lot_surface_col = f'lot{i}_surface_carrez' # Nom de colonne typique, ajuster si différent

                        numero_lot_val = str(row.get(lot_numero_col)) if pd.notna(row.get(lot_numero_col)) else None
                        surface_carrez_val = clean_numeric(row.get(lot_surface_col)) # Appliquer clean_numeric ici aussi

                        if numero_lot_val: # Un lot doit au moins avoir un numéro
                            lot_query = """
                                INSERT INTO LOT (ID_Bien, Numero_lot, Surface_carree)
                                VALUES (%s, %s, %s)
                                ON DUPLICATE KEY UPDATE Surface_carree = VALUES(Surface_carree); 
                                -- Met à jour la surface si le lot existe déjà pour ce bien
                            """
                            cursor.execute(lot_query, (
                                bien_id,
                                numero_lot_val,
                                surface_carrez_val
                            ))
                inserted_rows_count += 1
            except Exception as e_row:
                skipped_due_to_error +=1
                # print(f"Erreur lors du traitement de la ligne {index} (ID Mutation: {row.get('id_mutation', 'N/A')}): {e_row}. Ligne ignorée.")
                # continue # Passer à la ligne suivante en cas d'erreur sur une ligne spécifique

            # Commit par lots pour améliorer les performances et réduire la charge sur la DB
            if inserted_rows_count > 0 and inserted_rows_count % batch_size == 0:
                connection.commit()
                print(f"Progression: {inserted_rows_count}/{total_rows} lignes traitées ({inserted_rows_count / total_rows:.1%}). {skipped_due_to_error} lignes ignorées.")

        connection.commit() # Commit final pour les lignes restantes
        print(f"Importation terminée. {inserted_rows_count} lignes traitées avec succès.")
        if skipped_due_to_error > 0:
            print(f"{skipped_due_to_error} lignes ont été ignorées en raison d'erreurs pendant leur traitement individuel.")

    except Error as e:
        print(f"Erreur majeure lors de l'insertion des données après {inserted_rows_count} lignes: {e}")
        try:
            connection.rollback() # Annuler les modifications en cas d'erreur majeure
            print("Rollback effectué.")
        except Error as rb_error:
            print(f"Erreur lors du rollback: {rb_error}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("Connexion à la base de données fermée.")

def main():
    """
    Fonction principale du script.
    Orchestre la lecture des fichiers CSV et l'insertion des données en base.
    """
    print("Début du script de peuplement de la base de données...")
    data_df = process_csv_files(CSV_FOLDER)

    if data_df is not None and not data_df.empty:
        print(f"Prêt à insérer {len(data_df)} enregistrements dans la base de données.")
        # Optionnel: Afficher un aperçu des données et des types avant insertion
        # print("Aperçu des données à insérer (5 premières lignes):")
        # print(data_df.head())
        # print("Types des colonnes du DataFrame final:")
        # print(data_df.dtypes)
        insert_data_to_db(data_df)
    else:
        print("Aucune donnée n'a été chargée ou traitée. Fin du script.")

if __name__ == "__main__":
    # S'assure que les variables d'environnement sont chargées si le script est le point d'entrée.
    # load_dotenv() # Déjà appelé globalement, mais peut être utile ici si le scope global n'est pas souhaité.
    main()