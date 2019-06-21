INSERT INTO DST_PROVINCIAS (A, B, km)
select
  A.ID A,
  B.ID B,
  ST_Distance(A.geom, B.geom, 0)/1000 km
from
  provincias A JOIN provincias B ON A.ID>B.ID
;

INSERT INTO DST_PROVINCIAS (A, B, km)
select
  B,
  A,
  km
from
  DST_PROVINCIAS
;
