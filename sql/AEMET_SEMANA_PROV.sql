DROP TABLE IF EXISTS AEMET_SEMANA_PROV;

CREATE TABLE AEMET_SEMANA_PROV (
  PROVINCIA TEXT,
  SEMANA REAL,
  tmed_desviacion REAL,
  tmax_desviacion REAL,
  tmin_desviacion REAL,
  e REAL,
  	hr REAL,
  	prec REAL,
  	presmax REAL,
  	presmin REAL,
  	q_mar REAL,
  	racha REAL,
  	sol REAL,
  	tmax REAL,
  	tmed REAL,
  	tmin REAL,
  	velmedia REAL,
  PRIMARY KEY (PROVINCIA, SEMANA),
  FOREIGN KEY(PROVINCIA) REFERENCES PROVINCIAS(ID)
);

INSERT INTO AEMET_SEMANA_PROV (
  PROVINCIA,
  SEMANA,
  e,
  hr,
  prec,
  presmax,
  presmin,
  q_mar,
  racha,
  sol,
  tmax,
  tmed,
  tmin,
  velmedia
)
select
  PROVINCIA,
  SEMANA,
  avg(e) e,
  	avg(hr) hr,
  	avg(prec) prec,
  	max(presmax) presmax,
  	min(presmin) presmin,
  	avg(q_mar) q_mar,
  	max(racha) racha,
  	avg(sol) sol,
  	max(tmax) tmax,
  	avg(tmed) tmed,
  	min(tmin) tmin,
  	avg(velmedia) velmedia
from (
  select
    week_ISO_8601(fecha) SEMANA,
    DP.*
  from
    AEMET_DIA_PROV DP
)
group by
  PROVINCIA, SEMANA
;

DROP TABLE IF EXISTS TMP_DESVIACION;

CREATE TABLE TMP_DESVIACION (
  PROVINCIA TEXT,
  SEMANA REAL,
  tdesviacion REAL
);

INSERT INTO TMP_DESVIACION (
  PROVINCIA,
  SEMANA,
  tdesviacion
)
select
  PROVINCIA,
  SEMANA,
  sqrt(
    avg(dif*dif)
  ) tmed_desviacion
from (
  select
    S.PROVINCIA,
    S.SEMANA,
    (D.tmed - S.tmed) dif
  from
    AEMET_SEMANA_PROV S join AEMET_DIA_PROV D on week_ISO_8601(D.fecha)=S.SEMANA and S.PROVINCIA=D.PROVINCIA
  where
    D.tmed is not null and S.tmed is not null
)
group by
  PROVINCIA, SEMANA
;

UPDATE AEMET_SEMANA_PROV SET tmed_desviacion=(
  select tdesviacion from TMP_DESVIACION TMP where TMP.PROVINCIA=AEMET_SEMANA_PROV.PROVINCIA and TMP.SEMANA=AEMET_SEMANA_PROV.SEMANA
);

delete from TMP_DESVIACION;

INSERT INTO TMP_DESVIACION (
  PROVINCIA,
  SEMANA,
  tdesviacion
)
select
  PROVINCIA,
  SEMANA,
  sqrt(
    avg(dif*dif)
  ) tmax_desviacion
from (
  select
    S.PROVINCIA,
    S.SEMANA,
    (D.tmax - S.tmax) dif
  from
    AEMET_SEMANA_PROV S join AEMET_DIA_PROV D on week_ISO_8601(D.fecha)=S.SEMANA and S.PROVINCIA=D.PROVINCIA
  where
    D.tmed is not null and S.tmed is not null
)
group by
  PROVINCIA, SEMANA
;

UPDATE AEMET_SEMANA_PROV SET tmax_desviacion=(
  select tdesviacion from TMP_DESVIACION TMP where TMP.PROVINCIA=AEMET_SEMANA_PROV.PROVINCIA and TMP.SEMANA=AEMET_SEMANA_PROV.SEMANA
);

delete from TMP_DESVIACION;

INSERT INTO TMP_DESVIACION (
  PROVINCIA,
  SEMANA,
  tdesviacion
)
select
  PROVINCIA,
  SEMANA,
  sqrt(
    avg(dif*dif)
  ) tmin_desviacion
from (
  select
    S.PROVINCIA,
    S.SEMANA,
    (D.tmin - S.tmin) dif
  from
    AEMET_SEMANA_PROV S join AEMET_DIA_PROV D on week_ISO_8601(D.fecha)=S.SEMANA and S.PROVINCIA=D.PROVINCIA
  where
    D.tmed is not null and S.tmed is not null
)
group by
  PROVINCIA, SEMANA
;

UPDATE AEMET_SEMANA_PROV SET tmin_desviacion=(
  select tdesviacion from TMP_DESVIACION TMP where TMP.PROVINCIA=AEMET_SEMANA_PROV.PROVINCIA and TMP.SEMANA=AEMET_SEMANA_PROV.SEMANA
);

DROP TABLE IF EXISTS TMP_DESVIACION;