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

SELECT AddGeometryColumn('municipios', 'geom', 4326, 'MULTIPOLYGON', 2);
SELECT CreateSpatialIndex('municipios', 'geom');
