# clean_dvfFiles.py
"""
Script pour lire les fichiers CSV DVF, les nettoyer, les transformer et
sauvegarder le DataFrame résultant.
"""
import pandas as pd
import os
import warnings
from datetime import datetime  # Importation directe

# Charger les variables d'environnement si DB_CONFIG est utilisé ici (non requis pour ce fichier)
# from dotenv import load_dotenv
# load_dotenv()

# Suppression des avertissements DtypeWarning
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

# Chemin vers le dossier contenant les fichiers CSV à importer et pour sauvegarder le fichier nettoyé
CSV_FOLDER = 'csvs'
OUTPUT_FOLDER = 'cleaned_data'  # Dossier pour les données nettoyées
CLEANED_DATA_FILE_PARQUET = os.path.join(OUTPUT_FOLDER, 'dvf_cleaned_data.parquet')
CLEANED_DATA_FILE_CSV = os.path.join(OUTPUT_FOLDER, 'dvf_cleaned_data.csv')


def clean_numeric(value):
    """
    Nettoie et convertit une valeur en type numérique (float).
    """
    if pd.isna(value) or str(value).strip() == '':
        return None
    try:
        if isinstance(value, str):
            value_cleaned = value.replace(',', '.')
        else:
            value_cleaned = value
        return float(value_cleaned)
    except (ValueError, TypeError):
        return None


def convert_date_string_to_datetime(date_str):
    """
    Convertit une chaîne de caractères représentant une date en objet datetime.datetime.
    """
    if pd.isna(date_str) or str(date_str).strip() == '':
        return None
    try:
        if '-' in str(date_str):  # Format YYYY-MM-DD
            return pd.to_datetime(date_str).to_pydatetime()
        elif '/' in str(date_str):  # Format DD/MM/YYYY
            return pd.to_datetime(date_str, format='%d/%m/%Y').to_pydatetime()
        # print(f"Format de date non reconnu pour : {date_str}") # Décommenter pour debug
        return None
    except Exception:
        # print(f"Erreur de conversion de date pour '{date_str}': {e}") # Décommenter pour debug
        return None


def process_csv_files(folder_path):
    """
    Lit, nettoie et concatène tous les fichiers CSV du dossier spécifié.
    """
    if not os.path.isdir(folder_path):
        print(f"Erreur: Le dossier CSV '{folder_path}' n'existe pas.")
        return pd.DataFrame()

    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if not csv_files:
        print(f"Aucun fichier CSV trouvé dans le dossier '{folder_path}'.")
        return pd.DataFrame()

    all_data_frames = []
    print(f"Début du traitement des fichiers CSV du dossier: {folder_path}")

    for file_name in csv_files:
        file_path = os.path.join(folder_path, file_name)
        print(f"Traitement du fichier: {file_path}")
        try:
            df = pd.read_csv(file_path, low_memory=False, dtype={
                'id_mutation': str, 'date_mutation': str, 'numero_disposition': str,
                'valeur_fonciere': str, 'code_postal': str, 'code_commune': str,
                'code_departement': str, 'id_parcelle': str,
                'lot1_numero': str, 'lot2_numero': str, 'lot3_numero': str,
                'lot4_numero': str, 'lot5_numero': str
            })
            print(f"Nombre de lignes lues dans {file_name}: {len(df)}")

            df.columns = df.columns.str.strip().str.lower()  # Standardisation des noms de colonnes

            # Nettoyage numérique et dates
            numeric_cols = ['valeur_fonciere', 'surface_reelle_bati', 'surface_terrain', 'longitude', 'latitude',
                            'lot1_surface_carrez', 'lot2_surface_carrez', 'lot3_surface_carrez',
                            'lot4_surface_carrez', 'lot5_surface_carrez']  # Ajout des surfaces carrez ici
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].apply(clean_numeric)
                # else: # Moins verbeux, les colonnes manquantes seront juste absentes
                # print(f"Avertissement: Colonne '{col}' non trouvée dans {file_name}.")

            if 'nombre_pieces_principales' in df.columns:
                df['nombre_pieces_principales'] = pd.to_numeric(df['nombre_pieces_principales'],
                                                                errors='coerce').astype('Int64')

            if 'date_mutation' in df.columns:
                df['date_mutation'] = df['date_mutation'].apply(convert_date_string_to_datetime)
                original_rows = len(df)
                df.dropna(subset=['date_mutation'], inplace=True)  # Crucial: enlève les mutations sans date valide
                if len(df) < original_rows:
                    print(
                        f"{original_rows - len(df)} lignes supprimées de {file_name} (date de mutation invalide/manquante).")

            all_data_frames.append(df)
        except Exception as e:
            print(f"Erreur majeure lors du traitement du fichier {file_path}: {e}")
            continue

    if not all_data_frames:
        print("Aucune donnée n'a pu être chargée des fichiers CSV.")
        return pd.DataFrame()

    final_df = pd.concat(all_data_frames, ignore_index=True)
    print(f"Nombre total de lignes valides concaténées: {len(final_df)}")

    # S'assurer que les colonnes de date sont bien de type datetime64[ns] si elles ne le sont pas déjà
    # Cela est important pour la sérialisation en Parquet.
    if 'date_mutation' in final_df.columns:
        final_df['date_mutation'] = pd.to_datetime(final_df['date_mutation'], errors='coerce')

    return final_df


def main():
    """
    Fonction principale pour nettoyer les fichiers DVF et sauvegarder le résultat.
    """
    print("Début du script de nettoyage des fichiers DVF...")

    # Créer le dossier de sortie s'il n'existe pas
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"Dossier '{OUTPUT_FOLDER}' créé.")

    cleaned_df = process_csv_files(CSV_FOLDER)

    if not cleaned_df.empty:
        try:
            cleaned_df.to_parquet(CLEANED_DATA_FILE_PARQUET, index=False)
            print(f"DataFrame nettoyé sauvegardé avec succès dans : {CLEANED_DATA_FILE_PARQUET}")
            # Optionnel: sauvegarder aussi en CSV pour une inspection facile
            # cleaned_df.to_csv(CLEANED_DATA_FILE_CSV, index=False, sep=';', decimal='.')
            # print(f"DataFrame nettoyé sauvegardé aussi en CSV dans : {CLEANED_DATA_FILE_CSV}")

        except Exception as e:
            print(f"Erreur lors de la sauvegarde du DataFrame nettoyé : {e}")
            print("Tentative de sauvegarde en CSV uniquement...")
            try:
                cleaned_df.to_csv(CLEANED_DATA_FILE_CSV, index=False, sep=';', decimal='.')
                print(f"DataFrame nettoyé sauvegardé en CSV dans : {CLEANED_DATA_FILE_CSV}")
            except Exception as e_csv:
                print(f"Erreur lors de la sauvegarde en CSV : {e_csv}")
    else:
        print("Aucune donnée nettoyée à sauvegarder.")


if __name__ == "__main__":
    main()