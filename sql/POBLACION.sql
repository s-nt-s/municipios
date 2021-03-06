create table POBLACION (
  MUN TEXT,
  YR INTEGER,
  "04ymenos" INTEGER,
  "05a09" INTEGER,
  "10a14" INTEGER,
  "15a19" INTEGER,
  "16ymas" INTEGER,
  "16a65" INTEGER,
  "18ymas" INTEGER,
  "20a24" INTEGER,
  "25a29" INTEGER,
  "30a34" INTEGER,
  "35a39" INTEGER,
  "40a44" INTEGER,
  "45a49" INTEGER,
  "50a54" INTEGER,
  "55a59" INTEGER,
  "60a64" INTEGER,
  "65a69" INTEGER,
  "70a74" INTEGER,
  "75a79" INTEGER,
  "80a84" INTEGER,
  "85ymas" INTEGER,
  "85a89" INTEGER,
  "90a94" INTEGER,
  "95a99" INTEGER,
  "100ymas" INTEGER,
  "total" INTEGER,
  "hombres_04ymenos" INTEGER,
  "hombres_05a09" INTEGER,
  "hombres_10a14" INTEGER,
  "hombres_15a19" INTEGER,
  "hombres_20a24" INTEGER,
  "hombres_25a29" INTEGER,
  "hombres_30a34" INTEGER,
  "hombres_35a39" INTEGER,
  "hombres_40a44" INTEGER,
  "hombres_45a49" INTEGER,
  "hombres_50a54" INTEGER,
  "hombres_55a59" INTEGER,
  "hombres_60a64" INTEGER,
  "hombres_65a69" INTEGER,
  "hombres_70a74" INTEGER,
  "hombres_75a79" INTEGER,
  "hombres_80a84" INTEGER,
  "hombres_85ymas" INTEGER,
  "hombres_85a89" INTEGER,
  "hombres_90a94" INTEGER,
  "hombres_95a99" INTEGER,
  "hombres_100ymas" INTEGER,
  "hombres_total" INTEGER,
  "mujeres_04ymenos" INTEGER,
  "mujeres_05a09" INTEGER,
  "mujeres_10a14" INTEGER,
  "mujeres_15a19" INTEGER,
  "mujeres_20a24" INTEGER,
  "mujeres_25a29" INTEGER,
  "mujeres_30a34" INTEGER,
  "mujeres_35a39" INTEGER,
  "mujeres_40a44" INTEGER,
  "mujeres_45a49" INTEGER,
  "mujeres_50a54" INTEGER,
  "mujeres_55a59" INTEGER,
  "mujeres_60a64" INTEGER,
  "mujeres_65a69" INTEGER,
  "mujeres_70a74" INTEGER,
  "mujeres_75a79" INTEGER,
  "mujeres_80a84" INTEGER,
  "mujeres_85ymas" INTEGER,
  "mujeres_85a89" INTEGER,
  "mujeres_90a94" INTEGER,
  "mujeres_95a99" INTEGER,
  "mujeres_100ymas" INTEGER,
  "mujeres_total" INTEGER,
  PRIMARY KEY (MUN, YR),
  FOREIGN KEY(MUN) REFERENCES municipios(ID)
)