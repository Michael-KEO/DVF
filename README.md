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

2. Créer un fichier `.env` à la racine du projet en suivant le modèle dans `.env.example` :
```bash
```env
DB_HOST=ip_mysql
DB_NAME=nom_base
DB_USER=utilisateur
DB_PASSWORD=mot_de_passe
```

Initialiser la base de données :
```bash
mysql -u votre_utilisateur -p nom_base < db/schema_jmerise.sql
```

Ou via un client MySQL :
Connectez-vous à votre serveur MySQL
Créez une base de données : 
```sql
CREATE DATABASE nom_base;
```
Sélectionnez la base :
```sql
USE nom_base;
```
Exécutez le script :
```sql
SOURCE chemin/vers/schema_jmerise.sql;
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

- 

## 📝 Notes

- 
```