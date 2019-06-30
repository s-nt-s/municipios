INSERT INTO DST_PROVINCIAS (A, B, km, crs)
select B, A, km, crs from DST_PROVINCIAS;

INSERT INTO DST_PROVINCIAS (A, B, km, crs)
select ID A, ID B, 0, 0 from PROVINCIAS;
