INSERT INTO DST_PROVINCIAS (A, B, crs)
select B, A, crs from DST_PROVINCIAS;

INSERT INTO DST_PROVINCIAS (A, B, crs)
select ID A, ID B, 0 from PROVINCIAS;

INSERT INTO DST_MUNICIPIOS (A, B, crs)
select B, A, crs from DST_MUNICIPIOS;

-- INSERT INTO DST_MUNICIPIOS (A, B, crs)
-- select ID A, ID B, 0 from MUNICIPIOS;
