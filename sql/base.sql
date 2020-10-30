select InitSpatialMetadata(1);
create table MUNICIPIOS (
  ID TEXT,
  nombre TEXT,
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

create table CRS_KM (
  crs REAL,
  km INTEGER,
  PRIMARY KEY (crs, km)
);

create table AREA_INFLUENCIA (
  A TEXT,
  B TEXT,
  km INTEGER,
  porcion REAL,
  PRIMARY KEY (A, B, km),
  FOREIGN KEY(A) REFERENCES MUNICIPIOS(ID),
  FOREIGN KEY(B) REFERENCES MUNICIPIOS(ID)
);
