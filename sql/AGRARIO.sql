create table AGRARIO (
  MUN TEXT,
  YR INTEGER,
  "sau" INTEGER,
"uta" INTEGER,
"explotaciones" INTEGER,
"unidadesganaderas" INTEGER,
  PRIMARY KEY (MUN, YR),
  FOREIGN KEY(MUN) REFERENCES municipios(ID)
)