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
--SELECT AddGeometryColumn('MUNICIPIOS', 'main_geom', 4326, 'POLYGON', 2);
--SELECT CreateSpatialIndex('MUNICIPIOS', 'main_geom');

create table PROVINCIAS (
  ID TEXT,
  nombre TEXT,
  PRIMARY KEY (ID)
);

SELECT AddGeometryColumn('PROVINCIAS', 'geom', 4326, 'MULTIPOLYGON', 2);
SELECT CreateSpatialIndex('PROVINCIAS', 'geom');
SELECT AddGeometryColumn('PROVINCIAS', 'point', 4326, 'POINT', 2);
SELECT CreateSpatialIndex('PROVINCIAS', 'point');
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
