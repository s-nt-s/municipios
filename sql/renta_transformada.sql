CREATE VIEW RENTA_TRANSFORMADA AS
select
  MUN,
  YR,
  case
    when tipo=1 then (rt*declaraciones/my)
    when tipo=2 then (((rt*declaraciones)*ocupados)/pq_ocupados)/my
    else null
  end renta
from
(
select
  r.MUN,
  r.YR,
  r.renta*1.0 rt,
  r.tipo,
  r.declaraciones,
  p."18ymas" my,
  s.total paro,
  (p."18ymas" - s.total) ocupados,
  pq.paro pq_total,
  pq.my pq_my,
  (pq.my - pq.paro) pq_ocupados
from
  (select * from renta where tipo in (1, 2)) r left join poblacion p on r.MUN=p.MUN and r.YR=p.YR
  left join sepe_year s on r.MUN=s.MUN and r.YR=s.YR
  left join (
    select
      s1.MUN,
      s1.YR,
      sum(p1."18ymas") my,
      sum(s1.total) paro
    from sepe_year s1 join poblacion p1 on s1.MUN=p1.MUN and s1.YR=p1.YR
    where exists (select MUN, YR from renta where tipo=2 and MUN=s1.MUN and YR=s1.YR)
    group by s1.MUN, s1.YR
  ) pq  on r.MUN=pq.MUN and r.YR=pq.YR
) aux
union
select
  MUN,
  YR,
  renta
from
  renta
where tipo=3
