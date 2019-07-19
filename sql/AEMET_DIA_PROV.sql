DROP VIEW IF EXISTS AEMET_DIA_PROV;
CREATE VIEW AEMET_DIA_PROV
AS
select
  PD.provincia,
  PD.FECHA,
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
from
(
  select
    provincia,
  	FECHA,
  	sum(prec) prec,
  	max(presmax) presmax,
  	min(presmin) presmin,
  	max(racha) racha,
  	max(sol) sol,
  	max(tmax) tmax,
  	avg(tmed) tmed,
  	min(tmin) tmin,
  	avg(velmedia) velmedia
  from AEMET_DIA S join AEMET_BASES B on S.BASE = B.ID
  group by B.provincia, S.FECHA
) PD left join (
  select
    provincia,
  	FECHA,
    avg(e) e,
  	avg(hr) hr,
  	avg(q_mar) q_mar
  from AEMET_MES M join AEMET_BASES B on M.BASE = B.ID
  group by B.provincia, M.FECHA
) PM on PD.provincia=PM.provincia and PM.FECHA=strftime('%Y-%m', PD.fecha)
;