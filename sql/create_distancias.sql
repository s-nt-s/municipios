create table DST_PROVINCIAS (
  A TEXT,
  B TEXT,
  crs REAL,
  PRIMARY KEY (A, B),
  FOREIGN KEY(A) REFERENCES PROVINCIAS(ID),
  FOREIGN KEY(B) REFERENCES PROVINCIAS(ID)
);

-- INSERT INTO DST_PROVINCIAS (A, B, crs)
-- select
--   A.ID A,
--   B.ID B,
--   ST_Distance(A.geom, B.geom) crs
-- from
--   provincias A JOIN provincias B on A.ID>B.ID
-- ;
