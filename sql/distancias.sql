DROP TABLE IF EXISTS PROV_DISTANCIAS;
create table PROV_DISTANCIAS (
  A TEXT,
  B TEXT,
  km REAL,
  PRIMARY KEY (A, B),
  FOREIGN KEY(A) REFERENCES provincias(ID),
  FOREIGN KEY(B) REFERENCES provincias(ID)
);

INSERT INTO PROV_DISTANCIAS (A, B, km)
select
  A.ID A,
  B.ID B,
  ST_Distance(A.geom, B.geom, 0)/1000 km
from
  provincias A JOIN provincias B ON A.ID>B.ID
;

INSERT INTO PROV_DISTANCIAS (A, B, km)
select
  B,
  A,
  km
from
  PROV_DISTANCIAS
;
