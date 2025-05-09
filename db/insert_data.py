# insert_data.py
"""
Script pour insérer les données DVF nettoyées (Mutation, Localisation, Bien, Mutation_Bien)
dans la base de données MySQL.
Lit les données à partir d'un fichier Parquet généré par clean_dvfFiles.py.
Sauvegarde les caches de localisation et de bien pour une utilisation ultérieure (par ex. pour l'insertion des lots).
"""
import pandas as pd
import json  # Pour sauvegarder les caches
import os
from data_utils import create_db_connection  # Import depuis db_utils.py
from clean_dvfFiles import CLEANED_DATA_FILE_PARQUET, OUTPUT_FOLDER  # Importer le chemin du fichier

# Fichiers pour sauvegarder les caches
LOCATIONS_CACHE_FILE = os.path.join(OUTPUT_FOLDER, 'locations_cache.json')
BIENS_CACHE_FILE = os.path.join(OUTPUT_FOLDER, 'biens_cache.json')


def insert_main_data(data_df, connection):
    """
    Insère les données principales (Mutation, Localisation, Bien, Mutation_Bien).
    Retourne les caches pour localisations et biens.
    """
    if data_df.empty:
        print("Aucune donnée à insérer.")
        return {}, {}

    cursor = connection.cursor()
    localisations_cache = {}
    biens_cache = {}
    mutations_cache = set()

    total_rows = len(data_df)
    inserted_rows_count = 0
    skipped_due_to_error = 0
    batch_size = 10000  # Ajustable

    print(f"Début de l'insertion des données principales pour {total_rows} lignes...")

    try:
        for index, row in data_df.iterrows():
            try:
                date_mutation_val = row.get('date_mutation')
                # S'assurer que c'est un objet datetime Python pour la DB, pd.NaT devient None
                if pd.isna(date_mutation_val):
                    date_mutation_val = None
                # else: date_mutation_val = date_mutation_val.to_pydatetime() # Déjà fait si lu de Parquet correctement

                id_mutation_val = str(row.get('id_mutation')) if pd.notna(row.get('id_mutation')) else None

                # 1. MUTATION
                if id_mutation_val and id_mutation_val not in mutations_cache:
                    # ... (code d'insertion MUTATION comme dans le script original) ...
                    mutation_query = """
                                     INSERT INTO MUTATION (ID_Mutation, Date_mutation, Numero_disposition, Nature_mutation)
                                     VALUES (%s, %s, %s, %s)
                                     ON DUPLICATE KEY UPDATE Date_mutation = \
                                     VALUES (Date_mutation); \
                                     """
                    cursor.execute(mutation_query, (
                        id_mutation_val, date_mutation_val,
                        int(row.get('numero_disposition')) if pd.notna(row.get('numero_disposition')) else None,
                        str(row.get('nature_mutation')) if pd.notna(row.get('nature_mutation')) else None
                    ))
                    mutations_cache.add(id_mutation_val)

                # 2. LOCALISATION
                loc_key_fields = ['code_departement', 'code_postal', 'code_commune', 'nom_commune',
                                  'adresse_numero', 'adresse_suffixe', 'adresse_nom_voie',
                                  'longitude', 'latitude']
                loc_key_values = [str(row.get(f)) if pd.notna(row.get(f)) and f not in ['longitude', 'latitude'] else \
                                      row.get(f) for f in loc_key_fields]
                loc_key = tuple(loc_key_values)

                if loc_key not in localisations_cache:
                    # ... (code d'insertion LOCALISATION comme dans le script original) ...
                    localisation_query = """
                                         INSERT INTO LOCALISATION (Adresse_numero, Adresse_suffixe, Adresse_nom_voie,
                                                                   Code_departement, Code_postal, Code_commune, \
                                                                   Nom_commune,
                                                                   Longitude, Latitude, Date_localisation)
                                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s); \
                                         """
                    cursor.execute(localisation_query, (
                        str(row.get('adresse_numero')) if pd.notna(row.get('adresse_numero')) else None,
                        str(row.get('adresse_suffixe')) if pd.notna(row.get('adresse_suffixe')) else None,
                        str(row.get('adresse_nom_voie')) if pd.notna(row.get('adresse_nom_voie')) else None,
                        str(row.get('code_departement')) if pd.notna(row.get('code_departement')) else None,
                        str(row.get('code_postal')) if pd.notna(row.get('code_postal')) else None,
                        str(row.get('code_commune')) if pd.notna(row.get('code_commune')) else None,
                        str(row.get('nom_commune')) if pd.notna(row.get('nom_commune')) else None,
                        row.get('longitude'), row.get('latitude'), date_mutation_val
                    ))
                    localisation_id = cursor.lastrowid
                    # Pour JSON, la clé doit être une chaîne. On peut joindre le tuple.
                    localisations_cache["|".join(map(str, loc_key))] = localisation_id
                else:
                    localisation_id = localisations_cache["|".join(map(str, loc_key))]

                # 3. BIEN
                bien_id = None
                if localisation_id:
                    bien_key_fields = ['id_parcelle', 'type_local', 'surface_reelle_bati', 'nombre_pieces_principales']
                    bien_key_values = []
                    for f in bien_key_fields:
                        val = row.get(f)
                        if pd.notna(val):
                            if f == 'nombre_pieces_principales':
                                bien_key_values.append(int(val))
                            elif f == 'surface_reelle_bati':
                                bien_key_values.append(float(val))
                            else:
                                bien_key_values.append(str(val))
                        else:
                            bien_key_values.append(None)

                    bien_key_tuple = tuple(bien_key_values + [localisation_id])
                    # Clé pour JSON
                    bien_key_str = "|".join(map(str, bien_key_tuple))

                    if bien_key_str not in biens_cache:
                        # ... (code d'insertion BIEN comme dans le script original) ...
                        bien_query = """
                                     INSERT INTO BIEN (ID_Parcelle, Type_local, Surface_reelle_bati,
                                                       Surface_terrain, Nombre_pieces_principales, ID_Localisation)
                                     VALUES (%s, %s, %s, %s, %s, %s); \
                                     """
                        cursor.execute(bien_query, (
                            str(row.get('id_parcelle')) if pd.notna(row.get('id_parcelle')) else None,
                            str(row.get('type_local')) if pd.notna(row.get('type_local')) else None,
                            row.get('surface_reelle_bati'),  # float or None
                            row.get('surface_terrain'),  # float or None
                            int(row.get('nombre_pieces_principales')) if pd.notna(
                                row.get('nombre_pieces_principales')) else None,
                            localisation_id
                        ))
                        bien_id = cursor.lastrowid
                        biens_cache[bien_key_str] = bien_id
                    else:
                        bien_id = biens_cache[bien_key_str]

                # 4. MUTATION_BIEN
                if id_mutation_val and bien_id:
                    # ... (code d'insertion MUTATION_BIEN comme dans le script original) ...
                    mutation_bien_query = """
                                          INSERT \
                                          IGNORE INTO MUTATION_BIEN (ID_Mutation, ID_Bien, Valeur_fonciere)
                                          VALUES (%s, %s, %s); \
                                          """
                    cursor.execute(mutation_bien_query, (
                        id_mutation_val, bien_id, row.get('valeur_fonciere')
                    ))

                inserted_rows_count += 1
            except Exception as e_row:
                skipped_due_to_error += 1
                # print(f"Ligne {index}: Erreur {e_row}, id_mutation {row.get('id_mutation', 'N/A')}. Ligne ignorée.")
                continue

            if inserted_rows_count > 0 and inserted_rows_count % batch_size == 0:
                connection.commit()
                print(
                    f"Progression (données principales): {inserted_rows_count}/{total_rows} ({inserted_rows_count / total_rows:.1%}). {skipped_due_to_error} ignorées.")

        connection.commit()
        print(
            f"Insertion des données principales terminée. {inserted_rows_count} lignes traitées. {skipped_due_to_error} ignorées.")

    except Exception as e:  # mysql.connector.Error
        print(f"Erreur majeure lors de l'insertion des données principales: {e}")
        connection.rollback()
    finally:
        if cursor:
            cursor.close()

    return localisations_cache, biens_cache


def main():
    print("Début du script d'insertion des données filtrées (principales)...")
    if not os.path.exists(CLEANED_DATA_FILE_PARQUET):
        print(
            f"Fichier de données nettoyées '{CLEANED_DATA_FILE_PARQUET}' non trouvé. Exécutez d'abord clean_dvfFiles.py.")
        return

    try:
        cleaned_df = pd.read_parquet(CLEANED_DATA_FILE_PARQUET)
        # Convertir explicitement les colonnes de date après la lecture de Parquet si nécessaire
        if 'date_mutation' in cleaned_df.columns:
            cleaned_df['date_mutation'] = pd.to_datetime(cleaned_df['date_mutation'], errors='coerce')
        print(f"{len(cleaned_df)} lignes lues depuis '{CLEANED_DATA_FILE_PARQUET}'.")
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier Parquet '{CLEANED_DATA_FILE_PARQUET}': {e}")
        return

    connection = create_db_connection()
    if connection and connection.is_connected():
        loc_cache, b_cache = insert_main_data(cleaned_df, connection)
        connection.close()
        print("Connexion à la base de données fermée.")

        # Sauvegarder les caches
        try:
            with open(LOCATIONS_CACHE_FILE, 'w') as f:
                json.dump(loc_cache, f)
            print(f"Cache des localisations sauvegardé dans {LOCATIONS_CACHE_FILE}")
            with open(BIENS_CACHE_FILE, 'w') as f:
                json.dump(b_cache, f)
            print(f"Cache des biens sauvegardé dans {BIENS_CACHE_FILE}")
        except Exception as e:
            print(f"Erreur lors de la sauvegarde des fichiers cache: {e}")
    else:
        print("Échec de l'établissement de la connexion à la DB pour l'insertion.")


if __name__ == "__main__":
    main()