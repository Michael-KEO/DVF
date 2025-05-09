# Analyse DVF - Application Streamlit

Ce projet a pour objectif d'analyser et de visualiser les données des Demandes de Valeurs Foncières (DVF) pour les départements de Paris (75) et de la Gironde (33), en se concentrant sur les années 2020, 2022 et 2024. L'application Streamlit permet une exploration interactive de ces données.

## Prérequis

* voir [requirements.txt](requirements.txt) pour la liste des dépendances Python.

## Installation

1. **Créer et activer un environnement virtuel :**
    ```bash
    python -m venv venv
    ```
    Sous Linux/macOS :
    ```bash
    source venv/bin/activate
    ```
    Sous Windows :
    ```bash
    venv\Scripts\activate
    ```

2. **Installer les dépendances :**
    ```bash
    pip install -r requirements.txt
    ```

## Configuration de la Base de Données

1.  **S'assurer que le serveur MySQL est démarré.**

2.  **Créer un fichier `.env`** à la racine du projet. Ce fichier contiendra les informations de connexion à votre base de données. Voici un modèle :
    ```env
    DB_HOST=localhost
    DB_NAME=dvf_database  # Ou le nom de votre choix
    DB_USER=votre_utilisateur_mysql
    DB_PASSWORD=votre_mot_de_passe_mysql
    ```
    Remplacez `localhost`, `dvf_database`, `votre_utilisateur_mysql`, et `votre_mot_de_passe_mysql` par vos propres informations.

3.  **Créer la base de données et sa structure :**
    * Connectez-vous à votre serveur MySQL (via la ligne de commande ou un client graphique comme DBeaver, MySQL Workbench).
    * Créez la base de données si elle n'existe pas (utilisez le `DB_NAME` défini dans votre fichier `.env`) :
        ```sql
        CREATE DATABASE IF NOT EXISTS dvf_databse CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        ```
    * Sélectionnez cette base de données :
        ```sql
        USE dvf_database;
        ```
    * Exécutez le script SQL pour créer les tables :
        ```sql
        SOURCE db/schema_jmerise.sql;
        ```
        (Adaptez le chemin `db/schema_jmerise.sql` si vous exécutez la commande `SOURCE` depuis un emplacement différent de la racine du projet).

## Peuplement de la Base de Données

Avant de lancer l'application, la base de données doit être peuplée avec les données DVF.

1.  **Placer les fichiers CSV DVF :**
    Les fichiers CSV bruts des Demandes de Valeurs Foncières doivent être placés dans le dossier `db/csv_originaux/`. Les fichiers attendus sont, par exemple :
    * `db/csv_originaux/2020_33.csv`
    * `db/csv_originaux/2020_75.csv`
    * `db/csv_originaux/2022_33.csv`
    * `db/csv_originaux/2022_75.csv`
    * `db/csv_originaux/2024_33.csv`
    * `db/csv_originaux/2024_75.csv`

2.  **Exécuter le script de peuplement :**
    Assurez-vous que votre environnement virtuel est activé. Depuis la racine du projet, lancez le script `populate_db.py` :
    ```bash
    python db/populate_db.py
    ```
    Ce script va lire les fichiers CSV du dossier `db/csv_originaux/`, nettoyer les données et les insérer dans les tables de votre base de données MySQL. Le processus peut prendre un certain temps en fonction du volume de données.


## Lancement de l'Application

1.  **Activer l'environnement virtuel** (si ce n'est pas déjà fait) :
    Sous Linux/macOS :
    ```bash
    source venv/bin/activate
    ```
    Sous Windows :
    ```bash
    venv\Scripts\activate
    ```

2.  **Lancer l'application Streamlit :**
    Depuis la racine du projet :
    ```bash
    streamlit run main.py
    ```
    L'application devrait s'ouvrir automatiquement dans votre navigateur web par défaut.

## Utilisation de l'Application

Une fois l'application lancée :
1.  L'interface principale affiche différents onglets pour explorer les données (Indicateurs Clés, Analyse par Commune, Évolutions Temporelles, etc.).
2.  Utilisez la barre latérale pour appliquer des filtres sur les données affichées, tels que :
    * Années à afficher.
    * Types de biens.
    * Natures de mutation.
3.  Les graphiques et les tableaux se mettront à jour dynamiquement en fonction de vos sélections.

## Structure du Projet (fichiers principaux)

* `main.py`: Fichier principal de l'application Streamlit. Contient la logique de l'interface utilisateur.
* `db/requests.py` (ou `db/data_utils.py`): Module contenant les fonctions Python qui encapsulent les requêtes SQL pour récupérer les données de la base MySQL.
* `db/populate_db.py`: Script Python utilisé pour nettoyer les données des fichiers CSV DVF et les insérer dans la base de données MySQL. *(Voir note dans la section "Peuplement de la Base de Données" si ce script a été refactorisé)*.
* `db/schema_jmerise.sql`: Script SQL contenant la définition de la structure des tables de la base de données.
* `db/csv_originaux/`: Dossier destiné à contenir les fichiers CSV DVF bruts.
* `requirements.txt`: Liste des dépendances Python nécessaires au projet.
* `.env`: Fichier (à créer par l'utilisateur) pour stocker les variables d'environnement, notamment les identifiants de connexion à la base de données.

## Résolution des Problèmes Courants

* **Erreur de connexion à la base de données :**
    * Vérifiez que votre serveur MySQL est en cours d'exécution.
    * Assurez-vous que les informations dans votre fichier `.env` (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD) sont correctes et que l'utilisateur MySQL a les droits nécessaires sur la base de données.
    * Contrôlez que la base de données et les tables ont bien été créées avec le script `schema_jmerise.sql`.
* **ModuleNotFoundError :**
    * Vérifiez que votre environnement virtuel est activé.
    * Assurez-vous d'avoir installé toutes les dépendances avec `pip install -r requirements.txt`.
* **Script `populate_db.py` échoue :**
    * Vérifiez que les fichiers CSV sont correctement placés dans `db/csv_originaux/` et que leurs noms correspondent à ceux attendus par le script (si applicable).
    * Consultez les messages d'erreur affichés dans la console pour identifier la cause (problème de format de fichier, données manquantes critiques, etc.).