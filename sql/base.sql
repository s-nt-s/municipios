select InitSpatialMetadata(1);
create table MUNICIPIOS (
  ID TEXT,
  nombre TEXT,
  lat REAL,
  lon REAL,
  cercanias TEXT,
  cerca TEXT,
  tipo INTEGER DEFAULT 1,
  PRIMARY KEY (ID)
);

SELECT AddGeometryColumn('MUNICIPIOS', 'geom', 4326, 'MULTIPOLYGON', 2);
SELECT CreateSpatialIndex('MUNICIPIOS', 'geom');
--SELECT AddGeometryColumn('MUNICIPIOS', 'main_geom', 4326, 'POLYGON', 2);
--SELECT CreateSpatialIndex('MUNICIPIOS', 'main_geom');

create table PROVINCIAS (
  ID TEXT,
  nombre TEXT,
  lat REAL,
  lon REAL,
  PRIMARY KEY (ID)
);

SELECT AddGeometryColumn('PROVINCIAS', 'geom', 4326, 'MULTIPOLYGON', 2);
SELECT CreateSpatialIndex('PROVINCIAS', 'geom');
--SELECT AddGeometryColumn('PROVINCIAS', 'main_geom', 4326, 'POLYGON', 2);
--SELECT CreateSpatialIndex('PROVINCIAS', 'main_geom');

DROP TABLE IF EXISTS DST_PROVINCIAS;
create table DST_PROVINCIAS (
  A TEXT,
  B TEXT,
  km REAL,
  PRIMARY KEY (A, B),
  FOREIGN KEY(A) REFERENCES provincias(ID),
  FOREIGN KEY(B) REFERENCES provincias(ID)
);

DROP TABLE IF EXISTS DST_MUNICIPIOS;
create table DST_MUNICIPIOS (
  A TEXT,
  B TEXT,
  km REAL,
  PRIMARY KEY (A, B),
  FOREIGN KEY(A) REFERENCES municipios(ID),
  FOREIGN KEY(B) REFERENCES municipios(ID)
);
