```markdown
# Analyse DVF - Application Streamlit

## 📋 Prérequis

- Python 3.8 ou supérieur
- MySQL Server
- Base de données DVF importée

## 🚀 Installation

1. Cloner le dépôt :
```bash
git clone [url-du-depot]
```

2. Créer un environnement virtuel :
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

3. Installer les dépendances :
```bash
pip install -r requirements.txt
```

## ⚙️ Configuration

1. Vérifier que MySQL est en cours d'exécution

2. Créer un fichier `.env` à la racine du projet :
```env
DB_HOST=ip_mysql
DB_NAME=nom_base
DB_USER=utilisateur
DB_PASSWORD=mot_de_passe
```

## 🏃‍♂️ Lancement

1. Activer l'environnement virtuel si ce n'est pas déjà fait

2. Lancer l'application :
```bash
streamlit run main.py
```

## 📊 Utilisation

1. Sélectionner un département dans le menu déroulant
2. Choisir une année
3. Ajuster la limite de données si nécessaire
4. Cliquer sur "Charger les données"

## 📑 Structure du projet

- `main.py` : Application Streamlit principale
- `db/data_utils.py` : Utilitaires d'accès aux données
- `db/populate_db.py` : Script de peuplement de la base
- `db/schema_jmerise.sql` : Structure de la base de données

## 🚧 Résolution des problèmes courants

- Si l'erreur "Données insuffisantes" apparaît, vérifier que :
  - La base de données contient des données pour le département/année sélectionnés
  - Les colonnes Surface_reelle_bati et Mutation_Valeur_fonciere ne sont pas nulles
  - La limite de données n'est pas trop basse

## 📝 Notes

- Les données sont mises en cache pendant 30 minutes
- La limite par défaut est de 5000 enregistrements
- L'affichage de la carte est limité à 1000 points pour des raisons de performance
```