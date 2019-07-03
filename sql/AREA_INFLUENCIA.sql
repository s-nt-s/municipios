INSERT INTO AREA_INFLUENCIA (A, B, km, factor)
select
	A.ID A, B.ID B, km,
	Area(Intersection(area, B.geom))/Area(B.geom) factor
from (
  select
  	M.ID, km, ST_Buffer(M.point, crs) area
  from
  	municipios M, CRS_KM
) A, municipios B
where
	A.ID!=B.ID or
	Intersects(area, B.geom) = 1
;
