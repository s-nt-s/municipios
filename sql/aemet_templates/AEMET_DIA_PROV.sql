DROP VIEW IF EXISTS AEMET_DIA_PROV;
CREATE VIEW AEMET_DIA_PROV
AS
select
  PD.provincia,
  PD.FECHA,
  {2}
from
(
  select
    provincia,
  	FECHA,
  	{0}
  from AEMET_DIA S join AEMET_BASES B on S.BASE = B.ID
  group by B.provincia, S.FECHA
) PD left join (
  select
    provincia,
  	FECHA,
    {1}
  from AEMET_MES M join AEMET_BASES B on M.BASE = B.ID
  group by B.provincia, M.FECHA
) PM on PD.provincia=PM.provincia and PM.FECHA=strftime('%Y-%m', PD.fecha)
;
