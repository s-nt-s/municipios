INSERT INTO DST_PROVINCIAS (A, B, km)
select ID, ID, 0 from provincias;
INSERT INTO DST_MUNICIPIOS (A, B, km)
select ID, ID, 0 from municipios;

INSERT INTO DST_PROVINCIAS (A, B, km)
select
  A.ID A,
  B.ID B,
  ST_Distance(A.point, B.point, 1)/1000 km
from
  provincias A JOIN provincias B on A.ID>B.ID
;
INSERT INTO DST_PROVINCIAS (A, B, km)
select B, A, km from DST_PROVINCIAS where A>B;

INSERT INTO DST_MUNICIPIOS (A, B, km)
select
  A.ID A,
  B.ID B,
  ST_Distance(A.point, B.point, 1)/1000 km
from
  municipios A JOIN municipios B on A.ID>B.ID
;
INSERT INTO DST_MUNICIPIOS (A, B, km)
select B, A, km from DST_MUNICIPIOS where A>B;
