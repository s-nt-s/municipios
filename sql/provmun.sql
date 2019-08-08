create table PROVMUN (
  provincia TEXT,
  municipio TEXT,
  PRIMARY KEY (provincia, municipio),
  FOREIGN KEY (provincia) REFERENCES PROVINCIAS(ID),
  FOREIGN KEY (municipio) REFERENCES MUNICIPIOS(ID)
);

INSERT INTO PROVMUN (provincia, municipio)
select distinct P, M from (
	select M.ID M, P.ID P from
	municipios M join provincias P ON  substr(M.ID, 1,2)=P.ID
	union
	select M.ID M, P.ID P from
	municipios M, provincias P
	where substr(M.ID, 1,2) not in (select ID from provincias) and Intersects(M.geom,P.geom)
) order by P, M
;
