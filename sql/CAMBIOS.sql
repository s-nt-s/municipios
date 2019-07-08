DROP TABLE IF EXISTS CAMBIOS;

CREATE TABLE CAMBIOS (
  "viejo" TEXT,
  "nuevo" TEXT,
  "wiki" TEXT,
  "fecha" DATE,
  "notas" TEXT,
  "remplaza" INTEGER,
  "municipio" INTEGER,
  "cod" TEXT
);