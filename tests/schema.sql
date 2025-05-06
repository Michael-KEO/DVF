-- Table: MUTATION (représente l'acte de mutation global)
CREATE TABLE MUTATION (
    ID_Mutation VARCHAR(30) NOT NULL,
    Date_mutation DATE NOT NULL,
    Numero_disposition INT NOT NULL,
    Nature_mutation VARCHAR(50),
    PRIMARY KEY (ID_Mutation)
) ENGINE=InnoDB;

-- Sur la table MUTATION pour les filtres par date
CREATE INDEX idx_date_mutation ON MUTATION(Date_mutation);


-- Table: LOCALISATION
CREATE TABLE LOCALISATION (
    ID_Localisation INT AUTO_INCREMENT NOT NULL,
    Adresse_numero VARCHAR(10),
    Adresse_suffixe VARCHAR(5),
    Adresse_nom_voie VARCHAR(100),
    Code_departement VARCHAR(10) NOT NULL,
    Code_postal VARCHAR(10),
    Code_commune VARCHAR(10),
    Nom_commune VARCHAR(50),
    Longitude DECIMAL(15,10),
    Latitude DECIMAL(15,10),
    Date_localisation DATE NOT NULL,  -- Ajout d'une date pour suivre l'historique des localisations
    PRIMARY KEY (ID_Localisation)
) ENGINE=InnoDB;

-- Indices pour améliorer les performances de recherche
CREATE INDEX idx_departement ON LOCALISATION(Code_departement);
CREATE INDEX idx_commune ON LOCALISATION(Code_commune);
CREATE INDEX idx_nom_commune ON LOCALISATION(Nom_commune);  -- Index supplémentaire sur le nom de la commune
CREATE INDEX idx_longitude_latitude ON LOCALISATION(Longitude, Latitude);  -- Index sur les coordonnées géographiques

-- Table: BIEN (représente chaque bien individuel dans une mutation)
CREATE TABLE BIEN (
    ID_Bien INT AUTO_INCREMENT NOT NULL,
    ID_Parcelle VARCHAR(20),
    Type_local VARCHAR(50),
    Surface_reelle_bati DECIMAL(15,2) CHECK (Surface_reelle_bati >= 0),  -- Validation pour la surface réelle bâtie
    Surface_terrain DECIMAL(15,2) CHECK (Surface_terrain >= 0),  -- Validation pour la surface du terrain
    Nombre_pieces_principales INT CHECK (Nombre_pieces_principales >= 0),  -- Validation pour le nombre de pièces
    ID_Localisation INT NOT NULL,
    PRIMARY KEY (ID_Bien),
    FOREIGN KEY (ID_Localisation) REFERENCES LOCALISATION(ID_Localisation)
) ENGINE=InnoDB;

-- Sur la table BIEN pour les recherches par type et les jointures fréquentes
CREATE INDEX idx_type_local ON BIEN(Type_local);
CREATE INDEX idx_bien_localisation ON BIEN(ID_Localisation);


-- Table: MUTATION_BIEN (fait le lien entre mutation et biens)
CREATE TABLE MUTATION_BIEN (
    ID_Mutation VARCHAR(30) NOT NULL,
    ID_Bien INT NOT NULL,
    Valeur_fonciere DECIMAL(15,2) CHECK (Valeur_fonciere >= 0),  -- Validation pour la valeur foncière
    PRIMARY KEY (ID_Mutation, ID_Bien),
    FOREIGN KEY (ID_Mutation) REFERENCES MUTATION(ID_Mutation),
    FOREIGN KEY (ID_Bien) REFERENCES BIEN(ID_Bien)
) ENGINE=InnoDB;

-- Sur la table MUTATION_BIEN pour optimiser les jointures et les calculs de valeur
CREATE INDEX idx_mutation_bien_valeur ON MUTATION_BIEN(Valeur_fonciere);

-- Table: LOT (pour gérer les lots éventuels)
CREATE TABLE LOT (
    ID_Lot INT AUTO_INCREMENT NOT NULL,
    ID_Bien INT NOT NULL,
    Numero_lot VARCHAR(10),
    Surface_carree DECIMAL(15,2),
    PRIMARY KEY (ID_Lot),
    FOREIGN KEY (ID_Bien) REFERENCES BIEN(ID_Bien)
) ENGINE=InnoDB;


