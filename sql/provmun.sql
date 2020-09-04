DROP TABLE IF EXISTS PROVMUN;
create table PROVMUN (
  provincia TEXT,
  municipio TEXT,
  area REAL,
  PRIMARY KEY (provincia, municipio),
  FOREIGN KEY (provincia) REFERENCES PROVINCIAS(ID),
  FOREIGN KEY (municipio) REFERENCES MUNICIPIOS(ID)
);

INSERT INTO PROVMUN (provincia, municipio, area)
select distinct P, M, A from (
	select M.ID M, P.ID P, 100 A from
	municipios M join provincias P ON  substr(M.ID, 1,2)=P.ID
	union
	select M.ID M, P.ID P, (ST_Area(Intersection(M.geom,P.geom))/ST_Area(M.geom))*100 A from
	municipios M, provincias P
	where substr(M.ID, 1,2) not in (select ID from provincias) and ST_Area(Intersection(M.geom,P.geom))>0
) order by P, M
;
