create table AGRARIO (
  MUN TEXT,
  YR INTEGER,
  "sau" REAL,
  "uta" REAL,
  "explotaciones" INTEGER,
  "unidadesganaderas" REAL,
  PRIMARY KEY (MUN, YR),
  FOREIGN KEY(MUN) REFERENCES municipios(ID)
)