INSERT INTO AREA_INFLUENCIA (A, B, km, porcion)
select
	A.ID A, B.ID B, km,
	Area(Intersection(area, B.geom))/Area(B.geom) porcion
from (
  select
  	M.ID, km, ST_Buffer(M.point, crs) area
  from
  	municipios M, CRS_KM
	where
		km < 21
) A, municipios B
where
	A.ID!=B.ID and
	Intersects(area, B.geom) = 1
;
