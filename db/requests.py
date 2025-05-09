# data_utils_test.py
"""
Module utilitaire pour la gestion des données et l'interaction avec la base de données MySQL.
Fournit des fonctions pour récupérer et traiter les données immobilières (DVF).
Utilise un pool de connexions pour optimiser les performances des requêtes.
Les données récupérées sont mises en cache par Streamlit pour éviter les appels redondants à la base.
"""
import mysql.connector
from mysql.connector import Error, pooling
import pandas as pd
import os
from dotenv import load_dotenv
import streamlit as st

# Chargement des variables d'environnement (identifiants DB, etc.)
load_dotenv()

# Variable globale pour le pool de connexions
connection_pool = None

DEFAULT_YEARS = [2020, 2022, 2024] # Années par défaut pour les requêtes globales
# Condition SQL formatée pour les années par défaut, utilisée dans plusieurs requêtes
YEAR_CONDITION_DEFAULT = f"YEAR(M.Date_mutation) IN ({', '.join(map(str, DEFAULT_YEARS))})"


def get_connection_pool():
    """
    Initialise et retourne un pool de connexions MySQL.
    Si le pool existe déjà, il est retourné. Sinon, il est créé.
    Les erreurs de création sont loggées et stockées dans st.session_state.

    Returns:
        mysql.connector.pooling.MySQLConnectionPool or None: Le pool de connexions ou None en cas d'erreur.
    """
    global connection_pool
    if connection_pool is None:
        try:
            connection_pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name="dvf_pool_main",
                pool_size=10, # Nombre de connexions maintenues dans le pool
                pool_reset_session=True,
                host=os.getenv('DB_HOST'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
            # print("Pool de connexions MySQL (main) créé avec succès.") # Décommenter pour le débogage
        except Error as e:
            print(f"Erreur lors de la création du pool de connexions (main): {e}")
            st.session_state.db_error_message = f"Erreur DB Pool: {e}" # Message d'erreur pour l'UI Streamlit
            connection_pool = None # Assure que le pool est None si la création échoue
    return connection_pool


def execute_query(query, params=None):
    """
    Exécute une requête SQL donnée en utilisant le pool de connexions.
    Gère les erreurs de connexion et d'exécution SQL.

    Args:
        query (str): La requête SQL à exécuter.
        params (tuple, optional): Les paramètres pour la requête SQL. Defaults to None.

    Returns:
        pd.DataFrame: Un DataFrame contenant les résultats de la requête,
                      ou un DataFrame vide en cas d'erreur.
    """
    pool = get_connection_pool()
    if pool is None:
        if 'db_error_message' not in st.session_state:
            st.session_state.db_error_message = "Pool de connexion non disponible."
        st.error(st.session_state.db_error_message)
        return pd.DataFrame() # Retourne un DataFrame vide si le pool n'est pas disponible

    connection = None
    cursor = None
    try:
        connection = pool.get_connection() # Obtient une connexion du pool
        cursor = connection.cursor(dictionary=True) # Retourne les résultats sous forme de dictionnaires
        cursor.execute(query, params)
        results = cursor.fetchall()
        st.session_state.db_status = "Connecté" # Met à jour le statut de la DB dans Streamlit
        if 'db_error_message' in st.session_state:
            del st.session_state.db_error_message # Efface le message d'erreur en cas de succès
        return pd.DataFrame(results)
    except Error as e:
        error_message = f"Erreur SQL: {e} | Query: {query} | Params: {params}"
        print(error_message)
        st.session_state.db_error_message = f"Erreur SQL: {e}" # Stocke le message d'erreur
        st.error(st.session_state.db_error_message) # Affiche l'erreur dans l'UI Streamlit
        return pd.DataFrame() # Retourne un DataFrame vide en cas d'erreur
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected(): # Vérifie si la connexion est valide avant de la fermer
            connection.close()


@st.cache_data
def get_all_distinct_types_locaux():
    """
    Récupère tous les types de locaux distincts et non vides de la table BIEN.
    Les résultats sont mis en cache par Streamlit.

    Returns:
        list: Une liste des types de locaux distincts.
    """
    query = """
        SELECT DISTINCT Type_local
        FROM BIEN
        WHERE Type_local IS NOT NULL AND TRIM(Type_local) != ''
        ORDER BY Type_local;
    """
    df = execute_query(query)
    return df['Type_local'].tolist() if not df.empty else []


@st.cache_data
def get_all_distinct_natures_mutation():
    """
    Récupère toutes les natures de mutation distinctes et non vides de la table MUTATION.
    Les résultats sont mis en cache par Streamlit.

    Returns:
        list: Une liste des natures de mutation distinctes.
    """
    query = """
        SELECT DISTINCT Nature_mutation
        FROM MUTATION
        WHERE Nature_mutation IS NOT NULL AND TRIM(Nature_mutation) != ''
        ORDER BY Nature_mutation;
    """
    df = execute_query(query)
    return df['Nature_mutation'].tolist() if not df.empty else []


@st.cache_data(ttl=3600) # Cache les données pendant 1 heure
def get_kpis_compare():
    """
    Calcule les indicateurs clés de performance (KPIs) pour les départements '75' (Paris)
    et '33' (Gironde) pour les années définies dans DEFAULT_YEARS.
    Les KPIs incluent le nombre de ventes, la valeur foncière totale, le prix au m² moyen,
    et la surface moyenne.
    Des filtres sont appliqués pour assurer la pertinence des données (ex: surface > 0).

    Returns:
        dict: Un dictionnaire contenant les KPIs pour les deux départements.
    """
    kpis = {}
    year_placeholders = ', '.join(['%s'] * len(DEFAULT_YEARS))
    base_query_params = DEFAULT_YEARS

    for dep_code, dep_suffix in [('75', '_75'), ('33', '_33')]:
        query = f"""
            SELECT
                COUNT(*) AS Nombre_ventes{dep_suffix},
                SUM(
                    CASE
                        WHEN B.Surface_reelle_bati > 0
                             AND MB.Valeur_fonciere / B.Surface_reelle_bati BETWEEN 1000 AND 25000
                        THEN MB.Valeur_fonciere
                        ELSE NULL
                    END
                ) AS Valeur_fonciere_totale{dep_suffix},
                AVG(
                    CASE
                        WHEN B.Surface_reelle_bati BETWEEN 9 AND 1000
                             AND MB.Valeur_fonciere > 0
                             AND B.Surface_reelle_bati > 0
                             AND MB.Valeur_fonciere / B.Surface_reelle_bati BETWEEN 1000 AND 25000
                        THEN MB.Valeur_fonciere / B.Surface_reelle_bati
                        ELSE NULL
                    END
                ) AS Prix_m2_moyen{dep_suffix},
                AVG(
                    CASE
                        WHEN B.Surface_reelle_bati BETWEEN 9 AND 1000
                        THEN B.Surface_reelle_bati
                        ELSE NULL
                    END
                ) AS Surface_moyenne{dep_suffix}
            FROM BIEN B
            JOIN LOCALISATION L ON B.ID_Localisation = L.ID_Localisation
            JOIN MUTATION_BIEN MB ON B.ID_Bien = MB.ID_Bien
            JOIN MUTATION M ON MB.ID_Mutation = M.ID_Mutation
            WHERE
                L.Code_departement = %s
                AND YEAR(M.Date_mutation) IN ({year_placeholders});
        """
        params = [dep_code] + base_query_params
        df_dep = execute_query(query, tuple(params))
        if not df_dep.empty:
            kpis.update(df_dep.iloc[0].to_dict())
    return kpis


@st.cache_data(ttl=3600)
def get_top10_communes_prix_m2():
    """
    Récupère le top 10 des communes par prix moyen au m² pour les appartements et maisons
    dans les départements '75' et '33', pour les années définies dans DEFAULT_YEARS.
    Filtre les biens avec des surfaces et prix/m² jugés pertinents.
    Le classement est fait par nombre de ventes décroissant puis par prix/m² moyen décroissant.

    Returns:
        pd.DataFrame: DataFrame du top 10 des communes.
    """
    query = f"""
        SELECT *
        FROM (
            SELECT
                CASE
                    WHEN L.Code_departement = '75' THEN 'Paris'
                    ELSE 'Gironde'
                END AS Departement,
                L.Nom_commune,
                B.Type_local,
                ROUND(AVG(
                    CASE
                        WHEN B.Type_local IN ('Appartement', 'Maison')
                             AND B.Surface_reelle_bati >= 10 AND B.Surface_reelle_bati <= 400
                             AND (MB.Valeur_fonciere / B.Surface_reelle_bati) >= 1500
                             AND (MB.Valeur_fonciere / B.Surface_reelle_bati) <= 30000
                        THEN MB.Valeur_fonciere / B.Surface_reelle_bati
                        ELSE NULL
                    END
                ), 2) AS Prix_m2_moyen,
                COUNT(*) AS Nombre_ventes,
                ROW_NUMBER() OVER (
                    PARTITION BY L.Code_departement, B.Type_local
                    ORDER BY COUNT(*) DESC, AVG(MB.Valeur_fonciere / B.Surface_reelle_bati) DESC
                ) AS rang
            FROM MUTATION M
            JOIN MUTATION_BIEN MB ON M.ID_Mutation = MB.ID_Mutation
            JOIN BIEN B ON MB.ID_Bien = B.ID_Bien
            JOIN LOCALISATION L ON B.ID_Localisation = L.ID_Localisation
            WHERE
                L.Code_departement IN ('75', '33')
                AND {YEAR_CONDITION_DEFAULT}
                AND B.Type_local IN ('Appartement', 'Maison')
                AND MB.Valeur_fonciere > 0
                AND B.Surface_reelle_bati > 0
            GROUP BY
                L.Code_departement, L.Nom_commune, B.Type_local
            HAVING
                Prix_m2_moyen IS NOT NULL
                AND Nombre_ventes >= 30 -- Seuil minimum de ventes pour la significativité
        ) ranked_communes
        WHERE rang <= 10
        ORDER BY Departement, Type_local, rang;
    """
    return execute_query(query)


@st.cache_data(ttl=3600)
def get_top_communes_valeur():
    """
    Récupère le top 5 des communes par valeur foncière totale pour les départements '75' et '33',
    pour les années définies dans DEFAULT_YEARS.

    Returns:
        pd.DataFrame: DataFrame du top 5 des communes par valeur foncière.
    """
    query = f"""
        WITH RankedCommunes AS (
            SELECT
                CASE
                    WHEN L.Code_departement = '75' THEN 'Paris'
                    ELSE 'Gironde'
                END AS Departement,
                L.Nom_commune,
                COUNT(DISTINCT M.ID_Mutation) AS Nombre_transactions,
                ROUND(SUM(MB.Valeur_fonciere), 2) AS Valeur_fonciere_totale,
                ROUND(AVG(MB.Valeur_fonciere), 2) AS Valeur_fonciere_moyenne,
                ROW_NUMBER() OVER (
                    PARTITION BY L.Code_departement
                    ORDER BY SUM(MB.Valeur_fonciere) DESC
                ) AS rang
            FROM MUTATION_BIEN MB
            JOIN BIEN B ON MB.ID_Bien = B.ID_Bien
            JOIN LOCALISATION L ON B.ID_Localisation = L.ID_Localisation
            JOIN MUTATION M ON MB.ID_Mutation = M.ID_Mutation
            WHERE
                L.Code_departement IN ('75', '33')
                AND {YEAR_CONDITION_DEFAULT}
                AND MB.Valeur_fonciere > 0
            GROUP BY
                L.Code_departement, L.Nom_commune
        )
        SELECT Departement, Nom_commune, Nombre_transactions, Valeur_fonciere_totale, Valeur_fonciere_moyenne
        FROM RankedCommunes
        WHERE rang <= 5
        ORDER BY Departement, Valeur_fonciere_totale DESC;
    """
    return execute_query(query)


@st.cache_data(ttl=3600)
def get_prix_m2_par_mois_compare():
    """
    Récupère l'évolution mensuelle du prix moyen au m² pour les appartements et maisons
    dans les départements '75' et '33', pour les années définies dans DEFAULT_YEARS.
    Filtre sur des plages de surface et de prix/m² pour la pertinence.

    Returns:
        pd.DataFrame: DataFrame de l'évolution mensuelle des prix au m².
    """
    query = f"""
        SELECT
            CASE
                WHEN L.Code_departement = '75' THEN 'Paris'
                ELSE 'Gironde'
            END AS Departement,
            YEAR(M.Date_mutation) AS Annee,
            MONTH(M.Date_mutation) AS Mois,
            ROUND(AVG(
                CASE
                    WHEN B.Type_local IN ('Appartement', 'Maison')
                         AND B.Surface_reelle_bati BETWEEN 10 AND 400
                         AND (MB.Valeur_fonciere / B.Surface_reelle_bati) BETWEEN 1500 AND 30000
                    THEN MB.Valeur_fonciere / B.Surface_reelle_bati
                    ELSE NULL
                END
            ), 2) AS Prix_m2_moyen
        FROM MUTATION M
        JOIN MUTATION_BIEN MB ON M.ID_Mutation = MB.ID_Mutation
        JOIN BIEN B ON MB.ID_Bien = B.ID_Bien
        JOIN LOCALISATION L ON B.ID_Localisation = L.ID_Localisation
        WHERE
            L.Code_departement IN ('75', '33')
            AND {YEAR_CONDITION_DEFAULT}
            AND MB.Valeur_fonciere > 0
            AND B.Surface_reelle_bati > 0
        GROUP BY
            Departement, Annee, Mois
        HAVING Prix_m2_moyen IS NOT NULL
        ORDER BY
            Departement, Annee, Mois;
    """
    return execute_query(query)


@st.cache_data(ttl=3600)
def get_ventes_par_mois_compare():
    """
    Récupère l'évolution mensuelle du nombre de ventes (tous types de biens et natures de mutation)
    dans les départements '75' et '33', pour les années définies dans DEFAULT_YEARS.

    Returns:
        pd.DataFrame: DataFrame de l'évolution mensuelle du nombre de ventes.
    """
    query = f"""
        SELECT
            CASE
                WHEN L.Code_departement = '75' THEN 'Paris'
                ELSE 'Gironde'
            END AS Departement,
            YEAR(M.Date_mutation) AS Annee,
            MONTH(M.Date_mutation) AS Mois,
            COUNT(*) AS Nombre_ventes
        FROM MUTATION M
        JOIN MUTATION_BIEN MB ON M.ID_Mutation = MB.ID_Mutation
        JOIN BIEN B ON MB.ID_Bien = B.ID_Bien
        JOIN LOCALISATION L ON B.ID_Localisation = L.ID_Localisation
        WHERE
            L.Code_departement IN ('75', '33')
            AND {YEAR_CONDITION_DEFAULT}
        GROUP BY
            Departement, Annee, Mois
        ORDER BY
            Departement, Annee, Mois;
    """
    return execute_query(query)


@st.cache_data(ttl=3600)
def get_correlation_surface_prix():
    """
    Récupère les données pour analyser la corrélation entre la surface moyenne,
    le prix moyen et le prix moyen au m² par type de local, pour les départements '75' et '33',
    et pour les années définies dans DEFAULT_YEARS.
    Filtre sur des plages de surface et de prix/m² pour la pertinence.

    Returns:
        pd.DataFrame: DataFrame contenant les données de surface et de prix pour corrélation.
    """
    query = f"""
        SELECT
            L.Code_departement,
            B.Type_local,
            ROUND(AVG(
                CASE
                    WHEN B.Surface_reelle_bati BETWEEN 9 AND 1000 -- Filtre surface aberrante
                    THEN B.Surface_reelle_bati
                    ELSE NULL
                END
            ), 2) AS Surface_moyenne,
            ROUND(AVG(
                CASE
                    WHEN B.Surface_reelle_bati BETWEEN 9 AND 1000
                         AND MB.Valeur_fonciere > 0
                         AND B.Surface_reelle_bati > 0
                         AND MB.Valeur_fonciere / B.Surface_reelle_bati BETWEEN 1000 AND 25000 -- Filtre prix/m² aberrant
                    THEN MB.Valeur_fonciere
                    ELSE NULL
                END
            ), 2) AS Prix_moyen,
            ROUND(AVG(
                CASE
                    WHEN B.Surface_reelle_bati BETWEEN 9 AND 1000
                         AND MB.Valeur_fonciere > 0
                         AND B.Surface_reelle_bati > 0
                         AND MB.Valeur_fonciere / B.Surface_reelle_bati BETWEEN 1000 AND 25000
                    THEN MB.Valeur_fonciere / B.Surface_reelle_bati
                    ELSE NULL
                END
            ), 2) AS Prix_moyen_m2
        FROM MUTATION M
        JOIN MUTATION_BIEN MB ON M.ID_Mutation = MB.ID_Mutation
        JOIN BIEN B ON MB.ID_Bien = B.ID_Bien
        JOIN LOCALISATION L ON B.ID_Localisation = L.ID_Localisation
        WHERE
            L.Code_departement IN ('75', '33')
            AND {YEAR_CONDITION_DEFAULT}
            AND B.Surface_reelle_bati > 0
            AND MB.Valeur_fonciere > 0
        GROUP BY
            L.Code_departement, B.Type_local
        HAVING
            Surface_moyenne IS NOT NULL
            AND Prix_moyen IS NOT NULL
            AND Prix_moyen_m2 IS NOT NULL
        ORDER BY
            L.Code_departement, B.Type_local;
    """
    return execute_query(query)


@st.cache_data(ttl=3600)
def get_transactions_par_nature():
    """
    Récupère le nombre de transactions par nature de mutation pour les départements '75' et '33',
    pour les années définies dans DEFAULT_YEARS.

    Returns:
        pd.DataFrame: DataFrame du nombre de transactions par nature.
    """
    query = f"""
        SELECT
            L.Code_departement,
            M.Nature_mutation,
            COUNT(*) AS Nombre_transactions
        FROM MUTATION M
        JOIN MUTATION_BIEN MB ON M.ID_Mutation = MB.ID_Mutation
        JOIN BIEN B ON MB.ID_Bien = B.ID_Bien # Jointure nécessaire pour filtrer par localisation indirectement
        JOIN LOCALISATION L ON B.ID_Localisation = L.ID_Localisation
        WHERE
            L.Code_departement IN ('75', '33')
            AND {YEAR_CONDITION_DEFAULT}
        GROUP BY
            L.Code_departement, M.Nature_mutation
        ORDER BY
            L.Code_departement, Nombre_transactions DESC;
    """
    return execute_query(query)


@st.cache_data(ttl=3600)
def get_evolution_prix_m2():
    """
    Alias pour get_prix_m2_par_mois_compare.
    Récupère l'évolution mensuelle du prix moyen au m² pour les appartements et maisons
    dans les départements '75' et '33', pour les années définies dans DEFAULT_YEARS.

    Returns:
        pd.DataFrame: DataFrame de l'évolution mensuelle des prix au m².
    """
    return get_prix_m2_par_mois_compare()


@st.cache_data(ttl=3600)
def get_prix_m2_par_type_local_compare():
    """
    Récupère le prix moyen au m² et le nombre de ventes par type de local
    pour les départements '75' et '33', pour les années définies dans DEFAULT_YEARS.
    Filtre sur des plages de surface et de prix/m² pour la pertinence.

    Returns:
        pd.DataFrame: DataFrame des prix au m² par type de local.
    """
    query = f"""
        SELECT
            CASE
                WHEN L.Code_departement = '75' THEN 'Paris'
                ELSE 'Gironde'
            END AS Departement,
            B.Type_local,
            COUNT(*) AS Nombre_ventes,
            ROUND(AVG(
                CASE
                    WHEN B.Surface_reelle_bati BETWEEN 9 AND 1000
                         AND MB.Valeur_fonciere > 0
                         AND B.Surface_reelle_bati > 0
                         AND MB.Valeur_fonciere / B.Surface_reelle_bati BETWEEN 1000 AND 25000
                    THEN MB.Valeur_fonciere / B.Surface_reelle_bati
                    ELSE NULL
                END
            ), 2) AS Prix_m2_moyen
        FROM BIEN B
        JOIN LOCALISATION L ON B.ID_Localisation = L.ID_Localisation
        JOIN MUTATION_BIEN MB ON B.ID_Bien = MB.ID_Bien
        JOIN MUTATION M ON MB.ID_Mutation = M.ID_Mutation
        WHERE
            L.Code_departement IN ('75', '33')
            AND {YEAR_CONDITION_DEFAULT}
            AND B.Surface_reelle_bati > 0
            AND MB.Valeur_fonciere > 0
            AND B.Type_local IS NOT NULL
        GROUP BY
            Departement, B.Type_local
        HAVING
            Prix_m2_moyen IS NOT NULL
            AND Nombre_ventes > 0
        ORDER BY
            Departement, Nombre_ventes DESC;
    """
    return execute_query(query)