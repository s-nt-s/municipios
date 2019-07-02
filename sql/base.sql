select InitSpatialMetadata(1);
create table MUNICIPIOS (
  ID TEXT,
  nombre TEXT,
  cercanias TEXT,
  cerca TEXT,
  tipo INTEGER DEFAULT 1,
  PRIMARY KEY (ID)
);

SELECT AddGeometryColumn('MUNICIPIOS', 'geom', 4326, 'MULTIPOLYGON', 2);
SELECT CreateSpatialIndex('MUNICIPIOS', 'geom');
SELECT AddGeometryColumn('MUNICIPIOS', 'point', 4326, 'POINT', 2);
SELECT CreateSpatialIndex('MUNICIPIOS', 'point');

create table PROVINCIAS (
  ID TEXT,
  nombre TEXT,
  PRIMARY KEY (ID)
);

SELECT AddGeometryColumn('PROVINCIAS', 'geom', 4326, 'MULTIPOLYGON', 2);
SELECT CreateSpatialIndex('PROVINCIAS', 'geom');
SELECT AddGeometryColumn('PROVINCIAS', 'point', 4326, 'POINT', 2);
SELECT CreateSpatialIndex('PROVINCIAS', 'point');

create table AEMET_BASES (
  ID TEXT,
  provincia TEXT,
  nombre TEXT,
  indsinop TEXT,
  PRIMARY KEY (ID),
  FOREIGN KEY(provincia) REFERENCES PROVINCIAS(ID)
);
SELECT AddGeometryColumn('AEMET_BASES', 'point', 4326, 'POINT', 2);
SELECT CreateSpatialIndex('AEMET_BASES', 'point');
