CREATE VIEW SEPE_YEAR AS
select
	s1.MUN, s1.YR, "agricultura"/C "agricultura", "construccion"/C "construccion", "industria"/C "industria", "servicios"/C "servicios", "sin_empleo_anterior"/C "sin_empleo_anterior", "hombre_25a45"/C "hombre_25a45", "hombre_<25"/C "hombre_<25", "hombre_>=45"/C "hombre_>=45", "mujer_25a45"/C "mujer_25a45", "mujer_<25"/C "mujer_<25", "mujer_>=45"/C "mujer_>=45", "total"/C "total"
from
(
select MUN, YR, sum("agricultura")*1.0 "agricultura", sum("construccion")*1.0 "construccion", sum("industria")*1.0 "industria", sum("servicios")*1.0 "servicios", sum("sin_empleo_anterior")*1.0 "sin_empleo_anterior", sum("hombre_25a45")*1.0 "hombre_25a45", sum("hombre_<25")*1.0 "hombre_<25", sum("hombre_>=45")*1.0 "hombre_>=45", sum("mujer_25a45")*1.0 "mujer_25a45", sum("mujer_<25")*1.0 "mujer_<25", sum("mujer_>=45")*1.0 "mujer_>=45", sum("total")*1.0 "total"
from sepe group by MUN, YR
) s1
join
(select MUN, YR, count(*) C from sepe group by MUN, YR) s2
on s2.MUN = s1.MUN and s1.YR = s2.YR