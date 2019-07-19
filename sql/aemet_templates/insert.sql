INSERT INTO AEMET_SEMANA (BASE, "fecha", "SEMANA", "tmed", "tmax", "velmedia", "racha", "presmax", "presmin", "sol", "prec", "hr")
select
    D.BASE,
    min(D.fecha) fecha,
    D.SEMANA,
    AVG(D.tmed) tmed,
    max(D.tmax) tmax,
    AVG(D.velmedia) velmedia,
    max(D.racha) racha,
    max(D.presmax) presmax,
    min(D.presmin) presmin,
    sum(D.sol) sol,
    avg(D.prec) prec,
    avg(M.hr) hr
from
    (
      select
        week_ISO_8601(AD.fecha) SEMANA,
        strftime('%Y-%m', AD.fecha) YM,
        AD.*
      from AEMET_DIA AD
    ) D left join AEMET_MES M on M.FECHA=D.YM
where
    D.SEMANA>{0}
group by
    D.BASE, D.SEMANA
;
