DROP TABLE IF EXISTS AEMET_SEMANA;

CREATE TABLE AEMET_SEMANA (
  BASE TEXT,
  "fecha" TEXT,
  "SEMANA" REAL,
  "tmed" REAL,
  "tdesviacion" REAL,
  "tmax" REAL,
  "velmedia" REAL,
  "racha" REAL,
  "presmax" REAL,
  "presmin" REAL,
  "sol" REAL,
  "prec" REAL,
  "hr" REAL,
  PRIMARY KEY (BASE, SEMANA)
);
