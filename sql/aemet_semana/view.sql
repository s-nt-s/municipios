DROP VIEW IF EXISTS AEMET_SEMANA_PROV;
CREATE VIEW AEMET_SEMANA_PROV
AS
select
  provincia,
	semana,
	avg(tmed) tmed,
	avg(tdesviacion) tdesviacion,
	avg(tmax) tmax,
	avg(velmedia) velmedia,
	avg(racha) racha,
	avg(presmax) presmax,
	avg(presmin) presmin,
	avg(sol) sol,
	avg(prec) prec,
	avg(hr) hr
from AEMET_SEMANA S join AEMET_BASES B on S.BASE = B.ID
group by B.provincia, S.semana;
