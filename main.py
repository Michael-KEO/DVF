"""
Application Streamlit pour l'analyse comparative des données DVF (Demandes de Valeurs Foncières)
entre Paris (75) et la Gironde (33).

L'application charge les données une fois au démarrage à partir de la base de données
pour les années de référence (DEFAULT_YEARS) et pour les deux départements cibles.
Les filtres de la barre latérale (années, types de biens, natures de mutation)
s'appliquent ensuite sur ces données pré-chargées pour affiner l'affichage des graphiques et KPIs.

Structure :
- Configuration de la page Streamlit.
- Barre latérale pour les filtres d'affichage.
- Chargement global des données (mises en cache par Streamlit via data_utils_test).
- Interface utilisateur principale avec plusieurs onglets pour différentes analyses.
"""
import streamlit as st
import pandas as pd
import plotly.express as px

# Import des fonctions de récupération de données depuis le module data_utils_test
# Ces fonctions utilisent @st.cache_data pour mettre en cache leurs résultats.
from db.requests import (
    get_kpis_compare, get_top10_communes_prix_m2, get_top_communes_valeur,
    get_prix_m2_par_mois_compare, get_ventes_par_mois_compare,
    get_correlation_surface_prix, get_transactions_par_nature,
    get_evolution_prix_m2, get_prix_m2_par_type_local_compare,
    get_all_distinct_types_locaux, get_all_distinct_natures_mutation,
    DEFAULT_YEARS # Années de référence pour le chargement initial des données
)

# --- CONFIGURATION DE LA PAGE STREAMLIT ---
st.set_page_config(
    page_title="Analyse DVF - Paris vs Gironde",
    layout="wide", # Utilise toute la largeur de la page
    initial_sidebar_state="expanded" # Barre latérale ouverte par défaut
)

# --- BARRE LATÉRALE DE FILTRES D'AFFICHAGE ---
# Ces filtres agissent sur les données déjà chargées en mémoire.
# Ils ne déclenchent pas de nouvelles requêtes SQL (sauf pour peupler les options des filtres eux-mêmes).
st.sidebar.title("🖼️ Filtres d'Affichage")
st.sidebar.markdown("Sélectionnez les options pour affiner l'affichage des données.")

# Filtre pour les années
# Les données sont initialement chargées pour DEFAULT_YEARS.
# Ce filtre permet de choisir quelles de ces années afficher dans les graphiques.
selected_years_display = st.sidebar.multiselect(
    "Années à afficher",
    options=DEFAULT_YEARS,
    default=DEFAULT_YEARS,
    help="Filtre l'affichage des données déjà chargées pour ces années."
)
if not selected_years_display: # Si aucune année n'est sélectionnée, on les affiche toutes par défaut
    selected_years_display = DEFAULT_YEARS

# Filtre pour les types de biens
default_types_static = ['Appartement', 'Maison', 'Local industriel. commercial ou assimilé', 'Dépendance']
try:
    # Tente de récupérer les types de locaux depuis la DB pour les options du filtre
    options_types = get_all_distinct_types_locaux()
    if not options_types: # Si la DB ne retourne rien, utilise la liste statique
        options_types = default_types_static
except Exception: # En cas d'erreur de connexion DB ou autre
    options_types = default_types_static
selected_types_display = st.sidebar.multiselect(
    "Types de biens à afficher",
    options=options_types,
    default=options_types, # Par défaut, tous les types disponibles sont sélectionnés
    help="Filtre l'affichage pour ces types de biens."
)
if not selected_types_display: # Si aucun type n'est sélectionné
    selected_types_display = options_types

# Filtre pour les natures de mutation
default_natures_static = ['Vente', 'Vente en l\'état futur d\'achèvement', 'Adjudication']
try:
    # Tente de récupérer les natures de mutation depuis la DB
    options_natures = get_all_distinct_natures_mutation()
    if not options_natures: # Si la DB ne retourne rien, utilise la liste statique
        options_natures = default_natures_static
except Exception: # En cas d'erreur
    options_natures = default_natures_static
selected_natures_display = st.sidebar.multiselect(
    "Natures de mutation à afficher",
    options=options_natures,
    default=options_natures, # Par défaut, toutes les natures disponibles sont sélectionnées
    help="Filtre l'affichage pour ces natures de mutation."
)
if not selected_natures_display: # Si aucune nature n'est sélectionnée
    selected_natures_display = options_natures

# --- CHARGEMENT GLOBAL DES DONNÉES (MISES EN CACHE) ---
# Ces fonctions sont appelées une fois (par session utilisateur, ou jusqu'à expiration du cache TTL).
# Elles récupèrent les données pour les DEFAULT_YEARS et pour les départements 75 et 33.
# Les filtres de la sidebar seront appliqués sur ces DataFrames en mémoire.
st.info(f"Chargement des données de référence pour les années: {', '.join(map(str, DEFAULT_YEARS))}...")

df_kpis_full = get_kpis_compare()
df_top10_full = get_top10_communes_prix_m2()
df_top_valeur_full = get_top_communes_valeur()
df_prix_mois_full = get_prix_m2_par_mois_compare()
df_ventes_mois_full = get_ventes_par_mois_compare()
df_correlation_full = get_correlation_surface_prix()
df_transactions_full = get_transactions_par_nature()
df_evolution_prix_full = get_evolution_prix_m2() # Note: c'est un alias de get_prix_m2_par_mois_compare
df_typologie_full = get_prix_m2_par_type_local_compare()

st.success("Données de référence chargées.")

# --- APPLICATION PRINCIPALE ---
st.title("🏢 Analyse DVF - Comparaison Paris (75) vs Gironde (33)")
st.markdown(f"### Données de ventes immobilières (Affichage filtré pour : {', '.join(map(str, selected_years_display))})")

# Définition des onglets de l'application
tab_kpis, tab_communes, tab_evolutions, tab_types_biens, tab_autres = st.tabs([
    "📊 Indicateurs Clés", "🏘️ Analyse par Commune", "📈 Évolutions Temporelles",
    "🏠 Analyse par Type de Bien", "🔍 Autres Analyses"
])

# --- ONGLET 1: Indicateurs Clés (KPIs) ---
with tab_kpis:
    st.header("Indicateurs Clés de Performance (KPIs)")
    st.caption(
        f"Ces KPIs sont calculés sur les données des années de référence : {', '.join(map(str, DEFAULT_YEARS))}. "
        "Ils ne sont pas affectés par les filtres d'années de la barre latérale (qui ne servent qu'à filtrer l'affichage des graphiques d'évolution)."
    )
    kpis_data = df_kpis_full # Utilise les données KPI pré-chargées et non filtrées par la sidebar

    if kpis_data: # Vérifie si des données KPI ont été retournées
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🏙️ Paris (75)")
            # Extraction des KPIs pour Paris
            metrics_paris = {k: v for k, v in kpis_data.items() if '_75' in k and pd.notnull(v)}
            col_a, col_b, col_c, col_d = st.columns(4) # Colonnes pour l'affichage des métriques
            with col_a: st.metric("Nombre de ventes", f"{metrics_paris.get('Nombre_ventes_75', 0):,}")
            with col_b:
                full_value = f"{metrics_paris.get('Valeur_fonciere_totale_75', 0):,.0f} €"
                st.markdown(f"""
                    <div title="{full_value}">
                        <b>Valeur foncière totale</b><br/>
                        {full_value}
                    </div>
                """, unsafe_allow_html=True)

            with col_c:
                full_price = f"{metrics_paris.get('Prix_m2_moyen_75', 0):,.0f} €/m²"
                st.markdown(f"""
                    <div title="{full_price}">
                        <b>Prix moyen au m²</b><br/>
                        {full_price}
                    </div>
                """, unsafe_allow_html=True)
            with col_d: st.metric("Surface moyenne", f"{metrics_paris.get('Surface_moyenne_75', 0):.1f} m²")

        with col2:
            st.subheader("🌆 Gironde (33)")
            # Extraction des KPIs pour la Gironde
            metrics_gironde = {k: v for k, v in kpis_data.items() if '_33' in k and pd.notnull(v)}
            col_a, col_b, col_c, col_d = st.columns(4)
            with col_a: st.metric("Nombre de ventes", f"{metrics_gironde.get('Nombre_ventes_33', 0):,}")
            with col_b:
                full_value = f"{metrics_gironde.get('Valeur_fonciere_totale_33', 0):,.0f} €"
                st.markdown(f"""
                    <div title="{full_value}">
                        <b>Valeur foncière totale</b><br/>
                        {full_value}
                    </div>
                """, unsafe_allow_html=True)
            with col_c:
                full_price = f"{metrics_gironde.get('Prix_m2_moyen_33', 0):,.0f} €/m²"
                st.markdown(f"""
                    <div title="{full_price}">
                        <b>Prix moyen au m²</b><br/>
                        {full_price}
                    </div>
                """, unsafe_allow_html=True)
            with col_d: st.metric("Surface moyenne", f"{metrics_gironde.get('Surface_moyenne_33', 0):.1f} m²")
    else:
        st.warning("Données KPIs non disponibles. Vérifiez la connexion à la base de données ou les requêtes.")
    st.markdown("---")

# --- ONGLET 2: Analyse par Commune ---
with tab_communes:
    st.header("Analyse Détaillée par Commune")

    st.subheader("Top 10 communes par prix moyen au m² (Appartements & Maisons)")
    st.caption(f"Le Top 10 est calculé sur les années de référence {DEFAULT_YEARS} pour les Appartements et Maisons. "
               "Les filtres de la barre latérale (types de biens) s'appliquent sur ce Top 10 pour l'affichage.")

    df_top10_display = df_top10_full.copy() # Copie pour ne pas modifier le DataFrame global
    if 'Type_local' in df_top10_display.columns:
        # Filtre l'affichage du Top 10 basé sur les types de locaux sélectionnés dans la sidebar
        df_top10_display = df_top10_display[df_top10_display['Type_local'].isin(selected_types_display)]
    # Note: Le filtrage par 'selected_years_display' ici ne changerait pas le "Top 10" lui-même,
    # car il est calculé sur DEFAULT_YEARS. Pour un "Top 10 des années X", la fonction SQL devrait être paramétrée.

    if not df_top10_display.empty:
        types_dans_top10 = df_top10_display['Type_local'].unique()
        couleurs_map = {'Appartement': 'Blues', 'Maison': 'Reds'} # Palette de couleurs par type

        for type_bien_iter in types_dans_top10:
            # On se concentre sur Appartements et Maisons pour ce graphique spécifique
            if type_bien_iter not in ['Appartement', 'Maison']:
                continue

            df_type_specifique = df_top10_display[df_top10_display['Type_local'] == type_bien_iter]
            if df_type_specifique.empty:
                continue

            st.markdown(f"#### {type_bien_iter}s")
            col1_comm, col2_comm = st.columns(2)
            for dep_name, col_target, suffix_dep in [("Paris", col1_comm, "Paris"), ("Gironde", col2_comm, "Gironde")]:
                df_dep = df_type_specifique[df_type_specifique['Departement'] == dep_name].sort_values(
                    'Prix_m2_moyen', ascending=False
                )
                with col_target:
                    st.write(f"##### {dep_name} ({'75' if dep_name == 'Paris' else '33'}) - {type_bien_iter}s")
                    if not df_dep.empty:
                        fig = px.bar(df_dep, x='Nom_commune', y='Prix_m2_moyen', color='Prix_m2_moyen',
                                     color_continuous_scale=couleurs_map.get(type_bien_iter, 'Viridis'),
                                     title=f"Top communes - {type_bien_iter}s ({suffix_dep})",
                                     text='Prix_m2_moyen',
                                     labels={'Nom_commune': 'Commune', 'Prix_m2_moyen': 'Prix moyen au m² (€)'})
                        fig.update_traces(texttemplate='%{text:,.0f} €/m²', textposition='outside')
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info(f"Aucune donnée Top 10 pour les {type_bien_iter.lower()}s à {dep_name} avec les filtres d'affichage actuels.")
    else:
        st.warning("Aucune donnée Top 10 communes (prix/m²) à afficher avec les filtres actuels.")
    st.markdown("---")

    st.subheader("Top 5 communes par valeur foncière totale")
    st.caption(f"Le Top 5 est calculé sur les années de référence: {', '.join(map(str, DEFAULT_YEARS))}. "
               "Non affecté par les filtres de la barre latérale.")
    if not df_top_valeur_full.empty:
        col1, col2 = st.columns(2)
        for dep_name, col_target, suffix, couleur in [("Paris", col1, "Paris (75)", "Blues"),
                                                      ("Gironde", col2, "Gironde (33)", "Reds")]:
            df_dep = df_top_valeur_full[df_top_valeur_full['Departement'] == dep_name]
            with col_target:
                st.write(f"##### {suffix}")
                if not df_dep.empty:
                    fig = px.bar(df_dep, x='Nom_commune', y='Valeur_fonciere_totale',
                                 title=f"Top 5 Valeur Foncière ({suffix})",
                                 text=df_dep['Valeur_fonciere_totale'].apply(lambda x: f"{x:,.0f} €"),
                                 color='Valeur_fonciere_totale',
                                 color_continuous_scale=couleur,
                                 labels={'Nom_commune': 'Commune', 'Valeur_fonciere_totale': 'Valeur foncière totale (€)'})
                    fig.update_traces(textposition='outside')
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"Aucune donnée Top 5 Valeur Foncière pour {suffix} (probablement aucune vente enregistrée).")
    else:
        st.warning("Données Top 5 communes (valeur foncière) non disponibles.")
    st.markdown("---")

# --- ONGLET 3: Évolutions Temporelles ---
with tab_evolutions:
    st.header("Évolutions Temporelles du Marché Immobilier")

    st.subheader("Évolution du prix au m² par mois (Appartements & Maisons)")
    st.caption(f"Affichage pour les années sélectionnées: {', '.join(map(str, selected_years_display))}. "
               "Données de base calculées pour Appartements et Maisons sur les années de référence.")

    df_prix_mois_display = df_prix_mois_full.copy()
    if 'Annee' in df_prix_mois_display.columns:
        # Filtre l'affichage basé sur les années sélectionnées dans la sidebar
        df_prix_mois_display = df_prix_mois_display[df_prix_mois_display['Annee'].isin(selected_years_display)]

    if not df_prix_mois_display.empty:
        fig = px.line(df_prix_mois_display, x='Mois', y='Prix_m2_moyen', color='Departement', facet_col='Annee',
                      markers=True,
                      title="Évolution du prix moyen au m² par mois (Appartements & Maisons)",
                      labels={"Prix_m2_moyen": "Prix moyen (€/m²)", "Mois": "Mois", "Annee": "Année"},
                      color_discrete_map={"Paris": "#1E88E5", "Gironde": "#D81B60"})
        fig.update_xaxes(tickvals=list(range(1, 13)),
                         ticktext=['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Juin', 'Juil', 'Août', 'Sep', 'Oct', 'Nov', 'Déc'])
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Aucune donnée d'évolution des prix/m² par mois à afficher avec les filtres actuels.")
    st.markdown("---")

    st.subheader("Évolution du nombre de ventes par mois")
    st.caption(f"Affichage pour les années sélectionnées: {', '.join(map(str, selected_years_display))}. "
               "Comprend tous types de biens et toutes natures de mutation des années de référence.")

    df_ventes_mois_display = df_ventes_mois_full.copy()
    if 'Annee' in df_ventes_mois_display.columns:
        # Filtre l'affichage basé sur les années sélectionnées
        df_ventes_mois_display = df_ventes_mois_display[df_ventes_mois_display['Annee'].isin(selected_years_display)]
    # Note: La fonction get_ventes_par_mois_compare charge tous types/natures.
    # Pour appliquer les filtres selected_types_display et selected_natures_display ici,
    # il faudrait que la fonction SQL retourne ces colonnes et qu'elles soient présentes dans df_ventes_mois_full.

    if not df_ventes_mois_display.empty:
        fig = px.line(df_ventes_mois_display, x='Mois', y='Nombre_ventes', color='Departement', facet_col='Annee',
                      markers=True,
                      title="Évolution du nombre de ventes par mois",
                      labels={"Nombre_ventes": "Nombre de ventes", "Mois": "Mois", "Annee": "Année"},
                      color_discrete_map={"Paris": "#1E88E5", "Gironde": "#D81B60"})
        fig.update_xaxes(tickvals=list(range(1, 13)),
                         ticktext=['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Juin', 'Juil', 'Août', 'Sep', 'Oct', 'Nov', 'Déc'])
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Aucune donnée d'évolution du nombre de ventes par mois à afficher avec les filtres actuels.")
    st.markdown("---")

    st.subheader("Évolution des prix au m² (Moyenne Annuelle - Appartements & Maisons)")
    st.caption(f"Affichage pour les années sélectionnées: {', '.join(map(str, selected_years_display))}.")
    df_evol_prix_display = df_evolution_prix_full.copy() # df_evolution_prix_full est un alias de df_prix_mois_full
    if 'Annee' in df_evol_prix_display.columns:
        df_evol_prix_display = df_evol_prix_display[df_evol_prix_display['Annee'].isin(selected_years_display)]

    if not df_evol_prix_display.empty:
        # Calcul de la moyenne annuelle à partir des données mensuelles filtrées
        df_evol_annuelle = df_evol_prix_display.groupby(['Annee', 'Departement'])['Prix_m2_moyen'].mean().reset_index()
        if not df_evol_annuelle.empty:
            fig = px.line(df_evol_annuelle, x="Annee", y="Prix_m2_moyen", color="Departement", markers=True,
                          title="Évolution des prix au m² (Moyenne Annuelle - Appartements & Maisons)",
                          labels={"Annee": "Année", "Prix_m2_moyen": "Prix moyen (€/m²)"},
                          color_discrete_map={"Paris": "#1E88E5", "Gironde": "#D81B60"})
            fig.update_xaxes(type='category') # Assure que toutes les années sont affichées comme des catégories distinctes
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Données agrégées vides pour l'évolution annuelle des prix avec les filtres actuels.")
    else:
        st.warning("Aucune donnée d'évolution des prix/m² (annuelle) à afficher avec les filtres actuels.")
    st.markdown("---")

# --- ONGLET 4: Analyse par Type de Bien ---
with tab_types_biens:
    st.header("Analyse par Type de Bien Immobilier")
    st.caption(f"Les données sont calculées sur les années de référence {DEFAULT_YEARS}. "
               "Le filtre 'Types de biens' de la barre latérale s'applique à l'affichage.")

    st.subheader("Typologie des biens vendus par département")
    df_typologie_display = df_typologie_full.copy()
    if 'Type_local' in df_typologie_display.columns:
        # Filtre l'affichage basé sur les types de locaux sélectionnés
        df_typologie_display = df_typologie_display[df_typologie_display['Type_local'].isin(selected_types_display)]
    # Le filtre selected_years_display n'est pas appliqué ici car la fonction SQL agrège déjà sur les DEFAULT_YEARS.

    if not df_typologie_display.empty:
        col1, col2 = st.columns(2)
        for dep_name, col_target, suffix in [("Paris", col1, "Paris (75)"), ("Gironde", col2, "Gironde (33)")]:
            df_dep = df_typologie_display[df_typologie_display['Departement'] == dep_name]
            with col_target:
                st.write(f"#### {suffix}")
                if not df_dep.empty:
                    # Diagramme en barres: Prix moyen au m² par type
                    fig_bar = px.bar(df_dep, x="Type_local", y="Prix_m2_moyen", color="Type_local",
                                     title=f"{suffix} - Prix moyen au m² par type",
                                     text=df_dep['Prix_m2_moyen'].apply(
                                         lambda x: f"{x:,.0f} €/m²" if pd.notnull(x) else "N/A"),
                                     labels={"Type_local": "Type de bien", "Prix_m2_moyen": "Prix moyen (€/m²)"},
                                     category_orders={"Type_local": sorted(df_dep['Type_local'].unique())})
                    st.plotly_chart(fig_bar, use_container_width=True)

                    # Diagramme circulaire: Répartition des ventes par type
                    fig_pie = px.pie(df_dep, values="Nombre_ventes", names="Type_local",
                                     title=f"{suffix} - Répartition des ventes par type")
                    st.plotly_chart(fig_pie, use_container_width=True)
                else:
                    st.info(f"Aucune donnée de typologie pour {suffix} avec les filtres d'affichage actuels.")
    else:
        st.warning("Aucune donnée de typologie de biens à afficher avec les filtres actuels.")
    st.markdown("---")

    st.subheader("Corrélation entre surface et prix par type de local")
    st.caption(f"Les données sont calculées sur les années de référence {DEFAULT_YEARS}. "
               "Le filtre 'Types de biens' de la barre latérale s'applique à l'affichage.")
    df_correlation_display = df_correlation_full.copy()
    if 'Type_local' in df_correlation_display.columns:
        # Filtre l'affichage basé sur les types de locaux sélectionnés
        df_correlation_display = df_correlation_display[
            df_correlation_display['Type_local'].isin(selected_types_display)]
    # Le filtre selected_years_display n'est pas appliqué ici (agrégation sur DEFAULT_YEARS dans la fonction SQL).

    if not df_correlation_display.empty:
        col1, col2 = st.columns(2)
        for dep_code, col_target, suffix in [('75', col1, "Paris (75)"), ('33', col2, "Gironde (33)")]:
            df_dep = df_correlation_display[df_correlation_display['Code_departement'] == dep_code]
            with col_target:
                st.write(f"#### {suffix}")
                if not df_dep.empty:
                    # Nettoyage des données pour le graphique de corrélation (suppression des NaN)
                    df_dep_clean = df_dep.dropna(subset=['Prix_moyen_m2', 'Surface_moyenne', 'Prix_moyen'])
                    if not df_dep_clean.empty:
                        fig_corr = px.scatter(df_dep_clean, x="Surface_moyenne", y="Prix_moyen", color="Type_local",
                                              # La taille des points peut représenter le prix au m²
                                              size=[float(x) if pd.notnull(x) else 1 for x in df_dep_clean["Prix_moyen_m2"]],
                                              hover_data=["Type_local", "Surface_moyenne", "Prix_moyen", "Prix_moyen_m2"],
                                              title=f"{suffix} - Corrélation surface/prix",
                                              labels={"Surface_moyenne": "Surface moyenne (m²)",
                                                      "Prix_moyen": "Prix moyen (€)",
                                                      "Prix_moyen_m2": "Prix au m² (€/m²)"})
                        st.plotly_chart(fig_corr, use_container_width=True)
                    else:
                        st.info(f"Données insuffisantes pour le graphique de corrélation ({suffix}) après nettoyage des valeurs manquantes.")
                else:
                    st.info(f"Aucune donnée de corrélation pour {suffix} avec les filtres d'affichage actuels.")
    else:
        st.warning("Aucune donnée de corrélation surface/prix à afficher avec les filtres actuels.")
    st.markdown("---")

# --- ONGLET 5: Autres Analyses ---
with tab_autres:
    st.header("Analyses Diverses")
    st.subheader("Nombre de transactions par nature de mutation")
    st.caption(f"Les données sont calculées sur les années de référence {DEFAULT_YEARS}. "
               "Le filtre 'Natures de mutation' de la barre latérale s'applique à l'affichage.")

    df_transactions_display = df_transactions_full.copy()
    if 'Nature_mutation' in df_transactions_display.columns:
        # Filtre l'affichage basé sur les natures de mutation sélectionnées
        df_transactions_display = df_transactions_display[
            df_transactions_display['Nature_mutation'].isin(selected_natures_display)]
    # Le filtre selected_years_display n'est pas appliqué ici.
    # On pourrait aussi filtrer par selected_types_display si la fonction SQL ramenait Type_local.

    if not df_transactions_display.empty:
        col1, col2 = st.columns(2)
        for dep_code, col_target, suffix in [('75', col1, "Paris (75)"), ('33', col2, "Gironde (33)")]:
            df_dep = df_transactions_display[df_transactions_display['Code_departement'] == dep_code].sort_values(
                'Nombre_transactions', ascending=False)
            with col_target:
                st.write(f"#### {suffix}")
                if not df_dep.empty:
                    fig_trans = px.bar(df_dep, x="Nature_mutation", y="Nombre_transactions", color="Nature_mutation",
                                       text='Nombre_transactions', title=f"{suffix} - Transactions par nature",
                                       labels={"Nature_mutation": "Nature de mutation",
                                               "Nombre_transactions": "Nombre de transactions"},
                                       category_orders={"Nature_mutation": sorted(df_dep['Nature_mutation'].unique())})
                    st.plotly_chart(fig_trans, use_container_width=True)
                else:
                    st.info(f"Aucune transaction par nature pour {suffix} avec les filtres d'affichage actuels.")
    else:
        st.warning("Aucune donnée de transactions par nature à afficher avec les filtres actuels.")
    st.markdown("---")

# --- PIED DE PAGE ET INFORMATIONS DANS LA SIDEBAR ---
st.sidebar.markdown("---")
st.sidebar.caption("Source: Données DVF (Demandes de Valeurs Foncières)")
st.sidebar.caption(f"Données de base chargées pour les années: {', '.join(map(str, DEFAULT_YEARS))}")

st.markdown("---")
# Affiche le statut de la connexion à la base de données ou les messages d'erreur
db_status_message = st.session_state.get('db_status', 'Statut DB non vérifié')
if 'db_error_message' in st.session_state and st.session_state.db_error_message:
    st.error(f"Dernière erreur base de données: {st.session_state.db_error_message}")
    db_status_message = f"Erreur DB: {st.session_state.db_error_message}"
st.caption(f"Application d'analyse des données DVF - {db_status_message}")