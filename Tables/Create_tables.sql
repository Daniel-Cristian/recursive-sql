CREATE TABLE coduri_postale (
    cod_postal numeric(6,0) NOT NULL,
    localitate character varying(50),
    judet character varying(50) NOT NULL,
    CONSTRAINT coduripostale_pkey PRIMARY KEY (cod_postal)
);

CREATE TABLE depozite (
    id_depozit numeric(3,0) NOT NULL,
    adresa character varying(50) NOT NULL,
    cod_postal numeric(6,0),
    CONSTRAINT depozite_pkey PRIMARY KEY (id_depozit),
    CONSTRAINT depozite_codpostal_fkey FOREIGN KEY (cod_postal) REFERENCES coduri_postale(cod_postal) NOT DEFERRABLE
);

CREATE TABLE angajati (
    id_angajat numeric(4,0) NOT NULL,
    nume character varying(30) NOT NULL,
    prenume character varying(30) NOT NULL,
    adresa character varying(100) NOT NULL,
    cod_postal numeric(6,0) NOT NULL,
    functie character varying(25) NOT NULL,
    id_manager numeric(3,0),
    salariu integer,
    CONSTRAINT angajati_pkey PRIMARY KEY (id_angajat),
    CONSTRAINT angajati_codpostal_fkey FOREIGN KEY (cod_postal) REFERENCES coduri_postale(cod_postal) NOT DEFERRABLE
);

CREATE TABLE soferi (
    id_sofer numeric(4,0) NOT NULL,
    id_angajat numeric(4,0),
    cod_permis numeric(3,0) NOT NULL,
    km_parcursi character varying(10) NOT NULL,
    CONSTRAINT soferi_pkey PRIMARY KEY (id_sofer),
    CONSTRAINT soferi_idangajat_fkey FOREIGN KEY (id_angajat) REFERENCES angajati(id_angajat) NOT DEFERRABLE
);

CREATE TABLE vehicule (
    id_vehicul numeric(3,0) NOT NULL,
    model character varying(40),
    nr_inmatriculare character varying(10) NOT NULL,
    data_inmatriculare date NOT NULL,
    an_fabricatie numeric(4,0) NOT NULL,
    km numeric(10,0),
    CONSTRAINT vehicule_pkey PRIMARY KEY (id_vehicul)
);

CREATE TABLE expeditori (
    id_expeditor numeric(10,0) NOT NULL,
    nume character varying(50) NOT NULL,
    prenume character varying(50),
    cod_postal numeric(6,0),
    adresa character varying(100) NOT NULL,
    nr_telefon character varying(10) NOT NULL,
    CONSTRAINT expeditori_pkey PRIMARY KEY (id_expeditor),
    CONSTRAINT expeditori_codpostal_fkey FOREIGN KEY (cod_postal) REFERENCES coduri_postale(cod_postal) NOT DEFERRABLE
);

CREATE TABLE destinatari (
    id_destinatar numeric(10,0) NOT NULL,
    nume character varying(50) NOT NULL,
    prenume character varying(50),
    cod_postal numeric(6,0),
    adresa character varying(100) NOT NULL,
    nr_telefon character varying(10) NOT NULL,
    CONSTRAINT destinatari_pkey PRIMARY KEY (id_destinatar),
    CONSTRAINT destinatari_codpostal_fkey FOREIGN KEY (cod_postal) REFERENCES coduri_postale(cod_postal) NOT DEFERRABLE
);

CREATE TABLE categorii_colete (
    id_categorie numeric(5,0) NOT NULL,
    denumire_categorie character varying(40) NOT NULL,
    greutate_min numeric(5,0) NOT NULL,
    greutate_max numeric(5,0) NOT NULL,
    valoare_min numeric(5,0) NOT NULL,
    valoare_max numeric(7,0) NOT NULL,
    categorie_parinte numeric(5,0),
    CONSTRAINT id_categorie PRIMARY KEY (id_categorie)
);

CREATE TABLE colete (
    id_colet numeric(10,0) NOT NULL,
    id_destinatar numeric(10,0) NOT NULL,
    id_expeditor numeric(10,0),
    greutate numeric(10,2) NOT NULL,
    valoare numeric(10,2) NOT NULL,
    data_expediere date NOT NULL,
    data_primire_colet date,
    continut_colet character varying(50),
    categorie_colet numeric(5,0) NOT NULL,
    CONSTRAINT colete_pkey PRIMARY KEY (id_colet),
    CONSTRAINT colete_id_categorie_fkey FOREIGN KEY (categorie_colet) REFERENCES categorii_colete(id_categorie) NOT DEFERRABLE,
    CONSTRAINT colete_iddestinatar_fkey FOREIGN KEY (id_destinatar) REFERENCES destinatari(id_destinatar) NOT DEFERRABLE,
    CONSTRAINT colete_idexpeditor_fkey FOREIGN KEY (id_expeditor) REFERENCES expeditori(id_expeditor) NOT DEFERRABLE
);

CREATE TABLE facturi (
    nr_factura numeric(10,0) NOT NULL,
    id_expeditor numeric(10,0),
    id_destinatar numeric(10,0),
    data date,
    CONSTRAINT facturi_pkey PRIMARY KEY (nr_factura),
    CONSTRAINT facturi_id_destinatar_fkey FOREIGN KEY (id_destinatar) REFERENCES destinatari(id_destinatar) NOT DEFERRABLE,
    CONSTRAINT facturi_id_expeditor_fkey FOREIGN KEY (id_expeditor) REFERENCES expeditori(id_expeditor) NOT DEFERRABLE
); 

CREATE TABLE detalii_facturi (
    nr_factura numeric(10,0) NOT NULL,
    linie_factura numeric(10,0),
    id_colet numeric(10,0),
    CONSTRAINT detalii_factura_id_colet_fkey FOREIGN KEY (id_colet) REFERENCES colete(id_colet) NOT DEFERRABLE,
    CONSTRAINT detalii_factura_id_factura FOREIGN KEY (nr_factura) REFERENCES facturi(nr_factura) NOT DEFERRABLE
);

CREATE TABLE transporturi (
    id_transport numeric(10,0) NOT NULL,
    id_depozit_1 numeric(3,0),
    id_depozit_2 numeric(3,0),
    id_sofer numeric(3,0),
    id_vehicul numeric(3,0),
    data_plecare date,
    ora_plecare character varying(5),
    data_sosire date,
    ora_sosire character varying(5),
    CONSTRAINT transporturi_pkey PRIMARY KEY (id_transport),
    CONSTRAINT transporturi_id_depozit_1_fkey FOREIGN KEY (id_depozit_1) REFERENCES depozite(id_depozit) NOT DEFERRABLE,
    CONSTRAINT transporturi_id_depozit_2_fkey FOREIGN KEY (id_depozit_2) REFERENCES depozite(id_depozit) NOT DEFERRABLE,
    CONSTRAINT transporturi_id_sofer_fkey FOREIGN KEY (id_sofer) REFERENCES soferi(id_sofer) NOT DEFERRABLE,
    CONSTRAINT transporturi_id_vehicul_fkey FOREIGN KEY (id_vehicul) REFERENCES vehicule(id_vehicul) NOT DEFERRABLE
);

CREATE TABLE detalii_transport (
    id_transport numeric(10,0),
    linie_transport numeric(10,0),
    id_colet numeric(10,0),
    CONSTRAINT detalii_transport_id_colet_fkey FOREIGN KEY (id_colet) REFERENCES colete(id_colet) NOT DEFERRABLE,
    CONSTRAINT detalii_transport_id_transport_fkey FOREIGN KEY (id_transport) REFERENCES transporturi(id_transport) NOT DEFERRABLE
);

CREATE TABLE chitante (
    nr_chitanta numeric(10,0) NOT NULL,
    nr_factura numeric(10,0),
    mod_achitare character varying(25),
    data_achitare date,
    CONSTRAINT chitante_pkey PRIMARY KEY (nr_chitanta),
    CONSTRAINT chitante_nr_factura_fkey FOREIGN KEY (nr_factura) REFERENCES facturi(nr_factura) NOT DEFERRABLE
);

CREATE TABLE urmarire_colete (
    trackingnumber numeric(12,0) NOT NULL,
    id_colet numeric(10,0),
    id_depozit numeric(3,0),
    stare_colet character varying(30),
    CONSTRAINT urmarire_colete_trackingnumber PRIMARY KEY (trackingnumber),
    CONSTRAINT urmarire_colete_id_colet_fkey FOREIGN KEY (id_colet) REFERENCES colete(id_colet) NOT DEFERRABLE,
    CONSTRAINT urmarire_colete_id_depozit_fkey FOREIGN KEY (id_depozit) REFERENCES depozite(id_depozit) NOT DEFERRABLE
);

CREATE TABLE retururi (
    id_retur numeric(10,0) NOT NULL,
    nr_factura numeric(10,0),
    motiv_retur character varying(50),
    data_retur date,
    CONSTRAINT retururi_pkey PRIMARY KEY (id_retur),
    CONSTRAINT retururi_nr_factura_fkey FOREIGN KEY (nr_factura) REFERENCES facturi(nr_factura) NOT DEFERRABLE
);

CREATE TABLE incidente (
    id_incident numeric(10,0) NOT NULL,
    tip_incident character varying(30) NOT NULL,
    id_transport numeric(10,0),
    id_sofer numeric(3,0),
    data_raportarii date,
    status character varying(10) NOT NULL,
    CONSTRAINT incidente_pkey PRIMARY KEY (id_incident),
    CONSTRAINT incidente_id_sofer_fkey FOREIGN KEY (id_sofer) REFERENCES soferi(id_sofer) NOT DEFERRABLE,
    CONSTRAINT incidente_id_transport_fkey FOREIGN KEY (id_transport) REFERENCES transporturi(id_transport) NOT DEFERRABLE
);



