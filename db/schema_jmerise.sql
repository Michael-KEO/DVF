#------------------------------------------------------------
#        Script MySQL.
#------------------------------------------------------------


#------------------------------------------------------------
# Table: MUTATION
#------------------------------------------------------------

CREATE TABLE MUTATION(
        ID_Mutation        Varchar (30) NOT NULL ,
        Numero_disposition Int NOT NULL ,
        Nature_mutation    Varchar (50) ,
        Date_mutation      Date NOT NULL,
	    INDEX (Date_mutation),
        CONSTRAINT MUTATION_PK PRIMARY KEY (ID_Mutation)
)ENGINE=InnoDB;


#------------------------------------------------------------
# Table: LOCALISATION
#------------------------------------------------------------

#------------------------------------------------------------
# Table: LOCALISATION
#------------------------------------------------------------

CREATE TABLE LOCALISATION(
        ID_Localisation   Int  Auto_increment  NOT NULL ,
        Adresse_numero    Varchar (10) ,
        Adresse_suffixe   Varchar (5) ,
        Adresse_nom_voie  Varchar (100) ,
        Code_postal       Varchar (10) ,
        Date_localisation Date NOT NULL ,
        Code_departement  Varchar (10) NOT NULL ,
        Code_commune      Varchar (10) ,
        Nom_commune       Varchar (50) ,
        Longitude         Decimal (15,10) ,
        Latitude          Decimal (15,10) ,
        INDEX (Code_departement,Code_commune,Nom_commune,Longitude,Latitude),
        CONSTRAINT LOCALISATION_PK PRIMARY KEY (ID_Localisation)
)ENGINE=InnoDB;


#------------------------------------------------------------
# Table: BIEN
#------------------------------------------------------------

CREATE TABLE BIEN(
        ID_Bien                   Int  Auto_increment  NOT NULL ,
        ID_Parcelle               Varchar (20) ,
        Surface_reelle_bati       Decimal (15,2) ,
        Surface_terrain           Decimal (15,2) ,
        Nombre_pieces_principales Int ,
        Type_local                Varchar (50) ,
        ID_Localisation           Int NOT NULL ,
        INDEX (Type_local),
        CONSTRAINT BIEN_PK PRIMARY KEY (ID_Bien),
        CONSTRAINT BIEN_LOCALISATION_FK FOREIGN KEY (ID_Localisation) REFERENCES LOCALISATION(ID_Localisation)
)ENGINE=InnoDB;

#------------------------------------------------------------
# Table: LOT
#------------------------------------------------------------

CREATE TABLE LOT(
        ID_Lot         Int  Auto_increment  NOT NULL ,
        Numero_lot     Varchar (10) NOT NULL ,
        Surface_carree Decimal (15,2) NOT NULL ,
        ID_Bien        Int NOT NULL
	,CONSTRAINT LOT_PK PRIMARY KEY (ID_Lot)

	,CONSTRAINT LOT_BIEN_FK FOREIGN KEY (ID_Bien) REFERENCES BIEN(ID_Bien)
)ENGINE=InnoDB;


#------------------------------------------------------------
# Table: MUTATION_BIEN
#------------------------------------------------------------

CREATE TABLE MUTATION_BIEN(
        ID_Bien         Int NOT NULL ,
        ID_Mutation     Varchar (30) NOT NULL ,
        Valeur_fonciere Decimal (15,2) NOT NULL
	,CONSTRAINT MUTATION_BIEN_PK PRIMARY KEY (ID_Bien,ID_Mutation)

	,CONSTRAINT MUTATION_BIEN_BIEN_FK FOREIGN KEY (ID_Bien) REFERENCES BIEN(ID_Bien)
	,CONSTRAINT MUTATION_BIEN_MUTATION0_FK FOREIGN KEY (ID_Mutation) REFERENCES MUTATION(ID_Mutation)
)ENGINE=InnoDB;

