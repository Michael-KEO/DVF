import streamlit as st
import pandas as pd
import plotly.express as px
import time
from db.data_utils import (
    merge_data,
    get_departements,
    get_annees,
    get_merged_data_as_dataframe
)

# Configuration de la page
st.set_page_config(
    page_title="Analyse DVF",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Titre de l'application
st.title("🏘️ Analyse DVF - Données de ventes immobilières")
st.sidebar.header("Filtres")


# Cache pour les listes de départements et années
@st.cache_data(ttl=3600)  # Mise en cache pour 1 heure
def load_departements():
    dept_data = get_departements()
    return [d['Code_departement'] for d in dept_data]


@st.cache_data(ttl=3600)  # Mise en cache pour 1 heure
def load_annees():
    annee_data = get_annees()
    return [a['Annee'] for a in annee_data]


# Chargement des filtres
with st.sidebar:
    departements = load_departements()
    departement = st.selectbox("📍 Choisir un département", departements)

    annees = load_annees()
    annee = st.selectbox("📆 Choisir une année", annees if annees else [])

    limite = st.slider("Limite de données", 100, 600000, 5000, 100)
    st.caption(f"Limite: {limite} enregistrements")

    st.info("Utilisez une limite plus basse pour des temps de chargement plus rapides")

    load_button = st.button("🔄 Charger les données")


# Fonction de chargement avec cache
@st.cache_data(ttl=1800)  # Cache de 30 minutes
def load_filtered_data(dept, year, limit):
    start_time = time.time()
    df = get_merged_data_as_dataframe(code_departement=dept, annee=year, limit=limit)
    end_time = time.time()

    # Traitement supplémentaire du DataFrame
    if not df.empty and 'Date_mutation' in df.columns:
        df['Date_mutation'] = pd.to_datetime(df['Date_mutation'], errors='coerce')
        df = df[df['Date_mutation'].notna()]
        df['Année'] = df['Date_mutation'].dt.year
        df['Mois'] = df['Mutation_Date_mutation'].dt.month

        # Calcul du prix au m²
        df['Prix_m2'] = df.apply(
            lambda x: x['Mutation_Valeur_fonciere'] / x['Surface_reelle_bati']
            if x['Surface_reelle_bati'] > 0 else None,
            axis=1
        )

        # Suppression des valeurs aberrantes
        if 'Prix_m2' in df.columns:
            q1 = df['Prix_m2'].quantile(0.01)
            q3 = df['Prix_m2'].quantile(0.99)
            df = df[(df['Prix_m2'] >= q1) & (df['Prix_m2'] <= q3)]

    print(f"Données chargées en {end_time - start_time:.2f} secondes")
    return df


# Placeholder pour le chargement
loading_container = st.empty()

# Charger les données
if load_button or 'data_loaded' in st.session_state:
    with loading_container.container():
        with st.spinner(f"Chargement des données pour {departement} en {annee}..."):
            df = load_filtered_data(departement, annee, limite)
            st.session_state['data_loaded'] = True
            st.session_state['df'] = df

    # Suppression du spinner après chargement
    loading_container.empty()

    if 'df' in st.session_state:
        df = st.session_state['df']

        if df.empty:
            st.warning("Aucune donnée trouvée pour les filtres sélectionnés.")
        else:
            # Affichage des KPIs
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Nombre de ventes", len(df))
            with col2:
                st.metric("Valeur foncière totale", f"{df['Mutation_Valeur_fonciere'].sum():,.0f} €")
            with col3:
                if 'Prix_m2' in df.columns:
                    prix_moyen = df['Prix_m2'].mean()
                    st.metric("Prix moyen au m²", f"{prix_moyen:,.0f} €/m²")
            with col4:
                surface_moy = df['Surface_reelle_bati'].mean()
                st.metric("Surface moyenne", f"{surface_moy:.1f} m²")

            # Création d'onglets pour les visualisations
            tab1, tab2, tab3, tab4 = st.tabs(
                ["Ventes par commune", "Prix par commune", "Carte des ventes", "Analyse temporelle"])

            with tab1:
                # Nombre de ventes par commune
                st.markdown(f"### 📊 Nombre de ventes par commune")
                ventes_communes = df.groupby("Nom_commune").size().reset_index(name='Nombre de ventes')
                ventes_communes = ventes_communes.sort_values("Nombre de ventes", ascending=False)
                fig1 = px.bar(
                    ventes_communes.head(20),
                    x='Nom_commune',
                    y='Nombre de ventes',
                    title=f"Top 20 des communes par nombre de ventes ({departement}, {annee})",
                    color='Nombre de ventes',
                    color_continuous_scale='Bluered'
                )
                st.plotly_chart(fig1, use_container_width=True)

            with tab2:
                # Debug des données dans l'onglet
                st.write("### Informations de debug pour le calcul du prix au m²")
                st.write("Colonnes disponibles:", df.columns.tolist())
                st.write("Valeurs nulles Surface:", df['Surface_reelle_bati'].isna().sum())
                st.write("Valeurs nulles Prix:", df['Mutation_Valeur_fonciere'].isna().sum())
                st.write("Valeurs zéro Surface:", (df['Surface_reelle_bati'] == 0).sum())

                # Prix moyen au m² par commune
                if 'Prix_m2' in df.columns:
                    st.markdown("### 💰 Prix moyen par m² par commune")
                    prix_par_commune = df.groupby("Nom_commune")['Prix_m2'].mean().reset_index()
                    prix_par_commune = prix_par_commune.sort_values("Prix_m2", ascending=False)
                    fig2 = px.bar(
                        prix_par_commune.head(20),
                        x='Nom_commune',
                        y='Prix_m2',
                        title=f"Top 20 des communes par prix moyen au m² ({departement}, {annee})",
                        color='Prix_m2',
                        color_continuous_scale='Viridis'
                    )
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.warning("Données insuffisantes pour calculer le prix au m²")

            with tab3:
               # Carte des ventes
               st.markdown("### 🗺️ Carte des ventes")

               # Conversion explicite des colonnes numériques
               geo_data = df.copy()
               geo_data['Surface_reelle_bati'] = pd.to_numeric(geo_data['Surface_reelle_bati'], errors='coerce')
               geo_data['Mutation_Valeur_fonciere'] = pd.to_numeric(geo_data['Mutation_Valeur_fonciere'], errors='coerce')

               # Nettoyage des données
               geo_data = geo_data.dropna(subset=['Latitude', 'Longitude', 'Surface_reelle_bati', 'Mutation_Valeur_fonciere'])

               if not geo_data.empty:
                   # Normalisation de la taille des points
                   size_min = 5
                   size_max = 20
                   geo_data['size'] = size_min + (size_max - size_min) * (
                       (geo_data['Surface_reelle_bati'] - geo_data['Surface_reelle_bati'].min()) /
                       (geo_data['Surface_reelle_bati'].max() - geo_data['Surface_reelle_bati'].min())
                   )

                   # Limiter le nombre de points pour performance
                   if len(geo_data) > 1000:
                       st.info(f"Affichage limité à 1000 points sur {len(geo_data)} pour des raisons de performance")
                       geo_data = geo_data.sample(1000, random_state=42)

                   fig3 = px.scatter_mapbox(
                       geo_data,
                       lat="Latitude",
                       lon="Longitude",
                       hover_name="Nom_commune",
                       hover_data={
                           "Mutation_Valeur_fonciere": ":,.0f",
                           "Surface_reelle_bati": ":.0f",
                           "Type_local": True
                       },
                       color="Mutation_Valeur_fonciere",
                       size="size",  # Utilisation de la colonne normalisée
                       color_continuous_scale=px.colors.cyclical.IceFire,
                       zoom=8,
                       height=600
                   )
                   fig3.update_layout(mapbox_style="open-street-map")
                   st.plotly_chart(fig3, use_container_width=True)
               else:
                   st.warning("Pas de données géographiques disponibles pour ce filtre")

            with tab4:
                # Analyse temporelle
                st.markdown("### 📈 Évolution temporelle")

                # Création des colonnes Année et Mois à partir de Date_mutation
                df['Date_mutation'] = pd.to_datetime(df['Mutation_Date_mutation'])
                df['Mois'] = df['Date_mutation'].dt.month
                df['Année'] = df['Date_mutation'].dt.year

                # Ventes par mois
                ventes_mois = df.groupby('Mois').size().reset_index(name='Nombre de ventes')
                fig4 = px.line(
                    ventes_mois,
                    x='Mois',
                    y='Nombre de ventes',
                    title=f"Nombre de ventes par mois en {annee}",
                    markers=True,
                )
                fig4.update_xaxes(
                    tickvals=list(range(1, 13)),
                    ticktext=['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Juin',
                             'Juil', 'Août', 'Sep', 'Oct', 'Nov', 'Déc']
                )
                st.plotly_chart(fig4, use_container_width=True)

                # Prix au m² par mois
                if 'Prix_m2' in df.columns:
                    prix_mois = df.groupby('Mois')['Prix_m2'].mean().reset_index()
                    fig5 = px.line(
                        prix_mois,
                        x='Mois',
                        y='Prix_m2',
                        title=f"Prix moyen au m² par mois en {annee}",
                        markers=True,
                    )
                    fig5.update_xaxes(
                        tickvals=list(range(1, 13)),
                        ticktext=['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Juin',
                                 'Juil', 'Août', 'Sep', 'Oct', 'Nov', 'Déc']
                    )
                    st.plotly_chart(fig5, use_container_width=True)


            # Analyse des types de biens
            st.markdown("### 🏠 Analyse par type de bien")
            col1, col2 = st.columns(2)

            with col1:
                # Distribution des types locaux
                type_local_counts = df['Type_local'].value_counts().reset_index()
                type_local_counts.columns = ['Type de bien', 'Nombre de ventes']
                fig6 = px.pie(
                    type_local_counts,
                    values='Nombre de ventes',
                    names='Type de bien',
                    title="Répartition des ventes par type de bien"
                )
                st.plotly_chart(fig6, use_container_width=True)

            with col2:
                # Prix moyen par type local
                if 'Prix_m2' in df.columns:
                    prix_type = df.groupby('Type_local')['Prix_m2'].mean().reset_index()
                    prix_type.columns = ['Type de bien', 'Prix moyen au m²']
                    fig7 = px.bar(
                        prix_type,
                        x='Type de bien',
                        y='Prix moyen au m²',
                        title="Prix moyen au m² par type de bien",
                        color='Prix moyen au m²'
                    )
                    st.plotly_chart(fig7, use_container_width=True)

            # Données brutes (optionnel)
            with st.expander("🔍 Afficher les données brutes"):
                # Ajout d'un champ de recherche
                search = st.text_input("Rechercher une commune:")
                filtered_df = df
                if search:
                    filtered_df = df[df['Nom_commune'].str.contains(search, case=False)]
                st.dataframe(filtered_df.head(500))

                if len(filtered_df) > 500:
                    st.info(f"Affichage limité à 500 lignes sur {len(filtered_df)}")

                # Option d'export
                csv = filtered_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Télécharger les données (CSV)",
                    data=csv,
                    file_name=f'dvf_{departement}_{annee}.csv',
                    mime='text/csv',
                )
else:
    st.info("👈 Sélectionnez un département et une année dans le menu de gauche, puis cliquez sur 'Charger les données'")