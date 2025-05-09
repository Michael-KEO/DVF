# insert_lots.py
"""
Script pour insérer les données des LOTS dans la base de données MySQL.
afin de maintenir la cohérence avec le reste du projet.

Lit les données DVF nettoyées et les caches de localisation/bien pour lier les lots.
"""
import pandas as pd
import json
import os
from data_utils import create_db_connection  # Import depuis db_utils.py
from clean_dvfFiles import *  # Importer utilitaires
from db.insert_data import LOCATIONS_CACHE_FILE, BIENS_CACHE_FILE


def load_cache(file_path):
    """Charge un fichier cache JSON."""
    if not os.path.exists(file_path):
        print(f"Fichier cache '{file_path}' non trouvé.")
        return None
    try:
        with open(file_path, 'r') as f:
            cache = json.load(f)
        print(f"Cache '{file_path}' chargé avec succès.")
        return cache
    except Exception as e:
        print(f"Erreur lors du chargement du cache '{file_path}': {e}")
        return None


def insert_lots_data(data_df, connection, localisations_cache, biens_cache):
    """
    Insère les données des lots.
    Utilise les caches pour retrouver les ID_Bien correspondants.
    """
    if data_df.empty:
        print("Aucune donnée DVF pour traiter les lots.")
        return
    if not localisations_cache or not biens_cache:
        print("Caches de localisation ou de biens manquants. Impossible d'insérer les lots.")
        return

    cursor = connection.cursor()
    total_rows_df = len(data_df)
    inserted_lots_count = 0
    processed_rows_for_lots = 0
    batch_size = 10000  # Ajustable pour les opérations sur les lots

    print(f"Début de l'insertion des lots pour {total_rows_df} lignes du DataFrame DVF...")

    try:
        for index, row in data_df.iterrows():
            # Reconstruire la loc_key pour retrouver localisation_id
            loc_key_fields = ['code_departement', 'code_postal', 'code_commune', 'nom_commune',
                              'adresse_numero', 'adresse_suffixe', 'adresse_nom_voie',
                              'longitude', 'latitude']
            loc_key_values = [str(row.get(f)) if pd.notna(row.get(f)) and f not in ['longitude', 'latitude'] else \
                                  row.get(f) for f in loc_key_fields]
            loc_key_str = "|".join(map(str, tuple(loc_key_values)))
            localisation_id = localisations_cache.get(loc_key_str)

            if localisation_id:
                # Reconstruire la bien_key pour retrouver bien_id
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
                bien_key_str = "|".join(map(str, bien_key_tuple))
                bien_id = biens_cache.get(bien_key_str)

                if bien_id:
                    lots_inserted_for_this_bien = 0
                    for i in range(1, 6):  # lot1_ à lot5_
                        lot_numero_col = f'lot{i}_numero'
                        lot_surface_col = f'lot{i}_surface_carrez'

                        numero_lot_val = str(row.get(lot_numero_col)) if pd.notna(row.get(lot_numero_col)) else None
                        # La surface carrez a déjà été nettoyée par clean_numeric dans clean_dvfFiles.py
                        surface_carrez_val = row.get(lot_surface_col)  # Devrait être float ou None

                        if numero_lot_val:  # Un lot doit au moins avoir un numéro
                            try:
                                lot_query = """
                                            INSERT INTO LOT (ID_Bien, Numero_lot, Surface_carree)
                                            VALUES (%s, %s, %s)
                                            ON DUPLICATE KEY UPDATE Surface_carree = \
                                            VALUES (Surface_carree); \
                                            """
                                cursor.execute(lot_query, (bien_id, numero_lot_val, surface_carrez_val))
                                lots_inserted_for_this_bien += 1
                            except Exception as e_lot:
                                # print(f"Erreur insertion Lot {numero_lot_val} pour Bien ID {bien_id}: {e_lot}")
                                continue
                    if lots_inserted_for_this_bien > 0:
                        inserted_lots_count += lots_inserted_for_this_bien

            processed_rows_for_lots += 1
            if processed_rows_for_lots > 0 and processed_rows_for_lots % batch_size == 0:
                connection.commit()  # Commit les lots insérés
                print(
                    f"Progression (lots): {processed_rows_for_lots}/{total_rows_df} lignes DVF traitées pour les lots. {inserted_lots_count} lots insérés.")

        connection.commit()  # Commit final
        print(f"Insertion des lots terminée. {inserted_lots_count} lots insérés au total.")
        print(f"{processed_rows_for_lots} lignes DVF ont été vérifiées pour des lots.")

    except Exception as e:  # mysql.connector.Error
        print(f"Erreur majeure lors de l'insertion des lots: {e}")
        connection.rollback()
    finally:
        if cursor:
            cursor.close()


def main():
    print("Début du script d'insertion des lots...")

    if not os.path.exists(CLEANED_DATA_FILE_PARQUET):
        print(f"Fichier de données nettoyées '{CLEANED_DATA_FILE_PARQUET}' non trouvé.")
        return

    loc_cache = load_cache(LOCATIONS_CACHE_FILE)
    b_cache = load_cache(BIENS_CACHE_FILE)

    if not loc_cache or not b_cache:
        print("Caches non chargés. Arrêt de l'insertion des lots.")
        return

    try:
        cleaned_df = pd.read_parquet(CLEANED_DATA_FILE_PARQUET)
        print(f"{len(cleaned_df)} lignes lues depuis '{CLEANED_DATA_FILE_PARQUET}' pour traitement des lots.")
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier Parquet '{CLEANED_DATA_FILE_PARQUET}': {e}")
        return

    connection = create_db_connection()
    if connection and connection.is_connected():
        insert_lots_data(cleaned_df, connection, loc_cache, b_cache)
        connection.close()
        print("Connexion à la base de données fermée (lots).")
    else:
        print("Échec de l'établissement de la connexion à la DB pour l'insertion des lots.")


if __name__ == "__main__":
    main()