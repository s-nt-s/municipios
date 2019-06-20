DROP VIEW IF EXISTS RENTA_TRANSFORMADA;
CREATE VIEW RENTA_TRANSFORMADA AS
select
  MUN,
  YR,
  case
    when tipo=1 then (rt*1.0*declaraciones/my)
    when tipo=2 then (((rt*1.0*declaraciones)*ocupados)/pq_ocupados)/my
    else null
  end renta,
  tipo,
  rt renta_base
from
(
select
  r.MUN,
  r.YR,
  r.renta rt,
  r.tipo,
  r.declaraciones,
  p."18ymas" my,
  (p."16a65" - s.total) ocupados,
  (pq."16a65" - pq.total) pq_ocupados
from
  (select * from renta where tipo in (1, 2)) r
  left join poblacion p on r.MUN=p.MUN and r.YR=p.YR
  left join sepe_year s on r.MUN=s.MUN and r.YR=s.YR
  left join (
    select
      substr(s1.MUN, 1,2) PROV,
      s1.YR,
      sum(p1."16a65") "16a65",
      sum(s1.total) total
    from sepe_year s1 join poblacion p1 on s1.MUN=p1.MUN and s1.YR=p1.YR
    where exists (select * from renta rnt where rnt.tipo=2 and rnt.MUN=s1.MUN and rnt.YR=s1.YR)
    group by substr(s1.MUN, 1,2), s1.YR
  ) pq on substr(r.MUN, 1,2)=pq.PROV and r.YR=pq.YR
) aux
union
select
  MUN,
  YR,
  renta,
  tipo,
  renta renta_base
from
  renta
where tipo=3
;
