select InitSpatialMetadata(1);
create table municipios (
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


--DROP VIEW IF EXISTS DISTANCIAS;
CREATE VIEW DISTANCIAS AS
select
  A.ID A,
  B.ID B,
  ST_Distance(A.geom, B.geom, 1)/1000 km
from
  municipios A JOIN municipios B ON A.ID>B.ID
;
