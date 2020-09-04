INSERT INTO DST_PROVINCIAS (A, B, crs)
select
  A.ID A,
  B.ID B,
  case
      when Intersects(A.geom, B.geom) = 1 then 0
      when ST_Touches(A.geom, B.geom) = 1 then 0
      else ST_Distance(A.geom, B.geom)
  end crs
from
  provincias A JOIN provincias B on A.ID>B.ID
;
