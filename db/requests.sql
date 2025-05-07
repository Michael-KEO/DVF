-- Requête pour obtenir le nombre de transactions par trimestre et département
SELECT
    YEAR(m.Date_mutation) AS annee,
    QUARTER(m.Date_mutation) AS trimestre,
    l.Code_departement,
    COUNT(*) AS nombre_transactions,
    ROUND(AVG(mb.Valeur_fonciere), 2) AS prix_moyen,
    ROUND(SUM(mb.Valeur_fonciere), 2) AS valeur_totale
FROM MUTATION m
JOIN MUTATION_BIEN mb ON m.ID_Mutation = mb.ID_Mutation
JOIN BIEN b ON mb.ID_Bien = b.ID_Bien
JOIN LOCALISATION l ON b.ID_Localisation = l.ID_Localisation
GROUP BY annee, trimestre, l.Code_departement
ORDER BY l.Code_departement, annee, trimestre;

-- Requête pour obtenir le nombre de biens par commune et nombre de pièces principales
SELECT
    l.Nom_commune,
    b.Nombre_pieces_principales,
    COUNT(*) AS nombre_biens,
    ROUND(AVG(b.Surface_reelle_bati), 2) AS surface_moyenne,
    ROUND(AVG(mb.Valeur_fonciere), 2) AS prix_moyen,
    ROUND(MIN(mb.Valeur_fonciere), 2) AS prix_min,
    ROUND(MAX(mb.Valeur_fonciere), 2) AS prix_max
FROM BIEN b
JOIN LOCALISATION l ON b.ID_Localisation = l.ID_Localisation
JOIN MUTATION_BIEN mb ON b.ID_Bien = mb.ID_Bien
JOIN MUTATION m ON mb.ID_Mutation = m.ID_Mutation
WHERE b.Nombre_pieces_principales BETWEEN 1 AND 10
  AND l.Nom_commune IS NOT NULL
GROUP BY l.Nom_commune, b.Nombre_pieces_principales
ORDER BY l.Nom_commune, b.Nombre_pieces_principales;

-- Requête pour obtenir le nombre de mutations par département et nature de mutation
SELECT
    l.Code_departement,
    m.Nature_mutation,
    COUNT(*) AS nombre_mutations,
    ROUND(SUM(mb.Valeur_fonciere), 2) AS valeur_totale,
    ROUND(AVG(mb.Valeur_fonciere), 2) AS valeur_moyenne
FROM MUTATION m
JOIN MUTATION_BIEN mb ON m.ID_Mutation = mb.ID_Mutation
JOIN BIEN b ON mb.ID_Bien = b.ID_Bien
JOIN LOCALISATION l ON b.ID_Localisation = l.ID_Localisation
GROUP BY l.Code_departement, m.Nature_mutation
HAVING COUNT(*) > 5
ORDER BY l.Code_departement, nombre_mutations DESC;

-- Requête pour obtenir le prix moyen au mètre carré par commune et type de local
SELECT
    l.Code_postal,
    l.Nom_commune,
    b.Type_local,
    COUNT(*) AS nombre_transactions,
    ROUND(AVG(b.Surface_reelle_bati), 2) AS surface_moyenne,
    ROUND(AVG(mb.Valeur_fonciere), 2) AS prix_moyen,
    ROUND(SUM(mb.Valeur_fonciere) / SUM(b.Surface_reelle_bati), 2) AS prix_moyen_m2
FROM MUTATION_BIEN mb
JOIN BIEN b ON mb.ID_Bien = b.ID_Bien
JOIN LOCALISATION l ON b.ID_Localisation = l.ID_Localisation
WHERE b.Surface_reelle_bati > 0
  AND mb.Valeur_fonciere > 0
GROUP BY l.Code_postal, l.Nom_commune, b.Type_local
HAVING nombre_transactions >= 10
ORDER BY prix_moyen_m2 DESC;

-- Requête pour obtenir le nombre de biens par type de local et surface
SELECT
    b.ID_Bien,
    b.Type_local,
    l.Code_departement,
    l.Nom_commune,
    COUNT(DISTINCT lot.ID_Lot) AS nombre_lots,
    SUM(lot.Surface_carree) AS surface_totale_lots,
    b.Surface_reelle_bati,
    mb.Valeur_fonciere
FROM BIEN b
JOIN LOCALISATION l ON b.ID_Localisation = l.ID_Localisation
JOIN LOT lot ON b.ID_Bien = lot.ID_Bien
JOIN MUTATION_BIEN mb ON b.ID_Bien = mb.ID_Bien
GROUP BY b.ID_Bien, b.Type_local, l.Code_departement, l.Nom_commune, b.Surface_reelle_bati, mb.Valeur_fonciere
HAVING COUNT(DISTINCT lot.ID_Lot) > 1
ORDER BY nombre_lots DESC, mb.Valeur_fonciere DESC;