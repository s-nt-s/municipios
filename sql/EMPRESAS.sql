create table EMPRESAS (
  MUN TEXT,
  YR INTEGER,
  "actividades_profesionales_y_tecnicas" INTEGER,
"b_e_industria" INTEGER,
"comercio_transporte_y_hosteleria" INTEGER,
"educacion_sanidad_y_servicios_sociales" INTEGER,
"f_construccion" INTEGER,
"j_informacion_y_comunicaciones" INTEGER,
"k_actividades_financieras_y_de_seguros" INTEGER,
"l_actividades_inmobiliarias" INTEGER,
"otros_servicios_personales" INTEGER,
"total_empresas" INTEGER,
"total_servicios" INTEGER,
  PRIMARY KEY (MUN, YR),
  FOREIGN KEY(MUN) REFERENCES municipios(ID)
)