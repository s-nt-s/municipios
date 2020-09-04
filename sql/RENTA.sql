create table RENTA (
  MUN TEXT,
  YR INTEGER,
  "declaraciones" INTEGER,
  "renta" REAL,
  "tipo" INTEGER,
  PRIMARY KEY (MUN, YR),
  FOREIGN KEY(MUN) REFERENCES municipios(ID)
)