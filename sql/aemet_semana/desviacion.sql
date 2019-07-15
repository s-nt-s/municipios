select
  BASE,
  SEMANA,
  sqrt(
    avg(dif*dif)
  ) tdesviacion
from (
  select
    S.BASE,
    S.SEMANA,
    (D.tmed - S.tmed) dif
  from
    AEMET_SEMANA S join AEMET_DIA D on week_ISO_8601(D.fecha)=S.SEMANA and S.BASE=D.BASE
  where
    S.SEMANA>{0} and D.tmed is not null and S.tmed is not null
)
group by
  BASE, SEMANA
