import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
from functools import lru_cache
import os
from dotenv import load_dotenv

# Chargement des variables d'environnement
load_dotenv()


def create_db_connection():
    """Crée une connexion à la base de données MySQL"""

    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            # Paramètres d'optimisation de connexion
            use_pure=True,  # Utilise l'implémentation Python pure pour meilleure stabilité
            pool_size=5,  # Taille du pool de connexions
            pool_name="dvf_pool",
            buffered=True  # Utilise des curseurs bufferisés
        )
        return connection
    except Error as e:
        print(f"Erreur de connexion à MySQL: {e}")
        return None


def execute_query(connection, query, params=None):
    """Exécute une requête SQL avec gestion d'erreurs"""
    try:
        cursor = connection.cursor(dictionary=True)
        start_time = time.time()
        cursor.execute(query, params)
        results = cursor.fetchall()
        end_time = time.time()
        print(f"Requête exécutée en {end_time - start_time:.2f} secondes")
        return results
    except Error as e:
        print(f"Erreur d'exécution de la requête: {e}")
        return []
    finally:
        if cursor:
            cursor.close()


def get_data_with_joins(code_departement=None, annee=None, limit=None):
    """
    Récupère les données avec des jointures SQL au lieu de le faire en Python
    Permet de filtrer directement par département et année
    """
    connection = create_db_connection()
    if connection is None:
        return []

    try:
        # Construction des clauses WHERE selon les filtres
        where_clauses = []
        params = []

        if code_departement:
            where_clauses.append("l.Code_departement = %s")
            params.append(code_departement)

        if annee:
            where_clauses.append("YEAR(m.Date_mutation) = %s")
            params.append(annee)

        where_sql = " AND ".join(where_clauses)
        if where_sql:
            where_sql = "WHERE " + where_sql

        # Limite optionnelle
        limit_sql = f"LIMIT {limit}" if limit else ""

        # Requête SQL optimisée avec jointures - adaptée à votre structure de table
        query = f"""
        SELECT 
            b.ID_Bien, b.ID_Parcelle, b.Type_local, b.Surface_reelle_bati, b.Surface_terrain, b.Nombre_pieces_principales,
            l.Adresse_numero, l.Adresse_suffixe, l.Adresse_nom_voie, 
            l.Code_departement, l.Code_postal, l.Code_commune, l.Nom_commune, l.Longitude, l.Latitude,
            m.ID_Mutation, m.Date_mutation, m.Numero_disposition, m.Nature_mutation,
            mb.Valeur_fonciere,
            (SELECT COUNT(*) FROM LOT lt WHERE lt.ID_Bien = b.ID_Bien) AS Nombre_lots
        FROM 
            BIEN b
        JOIN 
            LOCALISATION l ON b.ID_Localisation = l.ID_Localisation
        JOIN 
            MUTATION_BIEN mb ON b.ID_Bien = mb.ID_Bien
        JOIN 
            MUTATION m ON mb.ID_Mutation = m.ID_Mutation
        {where_sql}
        ORDER BY m.Date_mutation DESC
        {limit_sql}
        """

        return execute_query(connection, query, params)
    finally:
        if connection.is_connected():
            connection.close()



@lru_cache(maxsize=32)
def get_departements():
    """Récupère la liste des départements disponibles (avec cache)"""
    connection = create_db_connection()
    if connection is None:
        return []

    try:
        query = """
                SELECT DISTINCT Code_departement
                FROM LOCALISATION
                ORDER BY Code_departement \
                """
        return execute_query(connection, query)
    finally:
        if connection.is_connected():
            connection.close()


@lru_cache(maxsize=32)
def get_annees():
    """Récupère la liste des années disponibles (avec cache)"""
    connection = create_db_connection()
    if connection is None:
        return []

    try:
        query = """
                SELECT DISTINCT YEAR(Date_mutation) as Annee
                FROM MUTATION
                ORDER BY Annee DESC \
                """
        return execute_query(connection, query)
    finally:
        if connection.is_connected():
            connection.close()


def get_lots_for_biens(bien_ids):
    """Récupère les lots pour une liste d'IDs de biens"""
    if not bien_ids:
        return {}

    connection = create_db_connection()
    if connection is None:
        return {}

    try:
        # Utiliser un seul paramètre avec format pour les IDs
        placeholders = ', '.join(['%s'] * len(bien_ids))
        query = f"""
        SELECT * FROM LOT 
        WHERE ID_Bien IN ({placeholders})
        """

        lots = execute_query(connection, query, bien_ids)

        # Organiser les lots par ID_Bien
        lots_by_bien = {}
        for lot in lots:
            bien_id = lot['ID_Bien']
            if bien_id not in lots_by_bien:
                lots_by_bien[bien_id] = []
            lots_by_bien[bien_id].append(lot)

        return lots_by_bien
    finally:
        if connection.is_connected():
            connection.close()


def merge_data(code_departement=None, annee=None, limit=None):
    """
    Version optimisée qui utilise des jointures SQL et des filtrages précoces
    """
    # Récupération des données principales avec jointures SQL
    data = get_data_with_joins(code_departement, annee, limit)

    if not data:
        return []

    # Récupération des IDs de biens pour charger les lots associés
    bien_ids = [record['ID_Bien'] for record in data]
    lots_by_bien = get_lots_for_biens(bien_ids)

    # Transformation pour correspondre au format attendu
    merged_data = []
    for record in data:
        bien_id = record['ID_Bien']

        # Restructurer les données pour correspondre au format attendu
        mutation = {
            'ID_Mutation': record['ID_Mutation'],
            'Date_mutation': record['Date_mutation'],
            'Numero_disposition': record['Numero_disposition'],
            'Nature_mutation': record['Nature_mutation'],
            'Valeur_fonciere': record['Valeur_fonciere']
        }

        # Trouver les lots pour ce bien
        lots = lots_by_bien.get(bien_id, [])

        # Créer l'entrée finale
        merged_record = {k: v for k, v in record.items() if
                         k not in ['ID_Mutation', 'Date_mutation', 'Numero_disposition', 'Nature_mutation',
                                   'Valeur_fonciere']}
        merged_record['Mutations'] = [mutation]
        merged_record['Lots'] = lots

        merged_data.append(merged_record)

    return merged_data


def get_merged_data_as_dataframe(code_departement=None, annee=None, limit=None):
    """Retourne les données fusionnées sous forme de DataFrame pandas"""
    merged_data = merge_data(code_departement, annee, limit)

    if not merged_data:
        return pd.DataFrame()

    # Transformation pour un format plus plat
    flat_data = []
    for record in merged_data:
        for mutation in record['Mutations']:
            flat_record = {
                **{k: v for k, v in record.items() if k not in ['Mutations', 'Lots']},
                **{f'Mutation_{k}': v for k, v in mutation.items()},
                'Lots_count': len(record['Lots'])
            }
            flat_data.append(flat_record)

    return pd.DataFrame(flat_data)


if __name__ == "__main__":
    # Test avec filtrage par département
    dept_test = "75"  # Paris
    print(f"Test avec département {dept_test}")
    start = time.time()
    data = merge_data(code_departement=dept_test, limit=1000)
    end = time.time()
    print(f"Temps d'exécution: {end - start:.2f} secondes")
    print(f"Nb d'enregistrements récupérés: {len(data)}")

    if data:
        print("\nPremier enregistrement:")
        print(f"Bien ID: {data[0]['ID_Bien']}")
        print(f"Type local: {data[0]['Type_local']}")
        print(f"Commune: {data[0]['Nom_commune']}")
        print(f"Nombre de mutations: {len(data[0]['Mutations'])}")
        print(f"Nombre de lots: {len(data[0]['Lots'])}")