INSERT INTO DST_MUNICIPIOS (A, B, crs)
select
  A.ID A,
  B.ID B,
  ST_Distance(A.point, B.point) crs
from
  municipios A JOIN municipios B on A.ID>B.ID
;
