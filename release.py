#!/usr/bin/env python3

from core.writer import MDWriter
from core.common import readfile
from core.db import DBshp
from textwrap import dedent
from subprocess import DEVNULL, STDOUT, check_call
import os
from shutil import copyfile
import sys
from core.schemaspy import SchemasPy

def run(*cmd, stdout=DEVNULL, stderr=DEVNULL, shell=False):
    if len(cmd)==1 and isinstance(cmd, str):
        if shell:
            cmd = cmd[0]
        else:
            cmd = cmd[0].split()
    check_call(cmd, stdout=stdout, stderr=stderr, shell=shell)


def zipfile(file, out):
    if os.path.isfile(out):
        os.remove(out)
    run("7z", "a", out, "./"+file)

zipfile("dataset/municipios.db", "_out/municipios.full.7z")
md = MDWriter("_out/README.md")

copyfile("dataset/municipios.db", "_out/municipios.db")
db = DBshp("_out/municipios.db")

tables = []
item = "* {ini} - {fin}: {rows} registros en {table}"
_ini = set()
_fin = set()
for t, cols in db.tables.items():
    if "YR" in cols and "_" not in t:
        ini, fin = db.one("select min(YR), max(YR) from "+t)
        rows = db.one("select count(*) from "+t)
        tables.append(item.format(table=t.upper(), ini=ini, fin=fin, rows=rows))
        _ini.add(ini)
        _fin.add(fin)

md.write("Datos desde el año {ini} al año {fin}:".format(ini=min(_ini), fin=max(_fin)))
md.write("")
md.write("\n".join(tables))
md.write("")
md.write(dedent('''
    Descargas:

    * municipios.full.7z base de datos con información Gis (usar con [SpatiaLite](https://live.osgeo.org/es/overview/spatialite_overview.html))
    * municipios.lite.7z base de datos sin información Gis
    * municipios.mini.7z base de datos sin información Gis ni tabla AREA_INFLUENCIA
'''))
md.close()

with open("_out/clean_spatialite.sql", "w") as f:
    f.write(dedent('''
        .output _out/create.sql
        SELECT sql || ';' FROM sqlite_master WHERE type = 'table' AND name = 'PROVINCIAS';
        SELECT sql || ';' FROM sqlite_master WHERE type = 'table' AND name = 'MUNICIPIOS';
        .mode insert PROVINCIAS
        select ID, nombre, ST_Y(point), ST_X(point) from PROVINCIAS;
        .mode insert MUNICIPIOS
        select ID, nombre, ST_Y(point), ST_X(point) from MUNICIPIOS;
        .mode list
    ''').strip()+"\n")
    for t, in db.select('''
        SELECT name FROM sqlite_master WHERE
        type='table' and upper(name)=name and
        name not in ('PROVINCIAS', 'MUNICIPIOS', 'CRS_KM')
        union
        SELECT name FROM sqlite_master WHERE
        type='view' and upper(name)=name
    '''):
        f.write(".dump "+t+"\n")
    f.write("vacuum;\n")
with open("_out/create.sh", "w") as f:
    f.write(dedent('''
        #!/bin/bash
        spatialite _out/municipios.db < _out/clean_spatialite.sql
        sed 's/, "geom" MULTIPOLYGON, "point" POINT,/,\\n  lat INTEGER,\\n  lon INTEGER,/g' -i _out/create.sql
        sed "s/INSERT INTO \\(MUNICIPIOS\\|PROVINCIAS\\) VALUES(\\([0-9][0-9]*\\),/INSERT INTO \\1 VALUES('\\2',/" -i _out/create.sql
        rm _out/municipios.db
        sqlite3 _out/municipios.db < _out/create.sql
    ''').strip())
db.close()
run("bash", "_out/create.sh")
zipfile("_out/municipios.db", "_out/municipios.lite.7z")
s = SchemasPy()
s.save_diagram(
    "_out/municipios.db",
    "dataset/municipios.png",
    "-norows",
    "-noviews",
    # size="large",
    I=".*(spatial|geometry|CAMBIOS|CRS_KM|AREA_INFLUENCIA|idx_|SpatialIndex|sql_statements_log|ElementaryGeometries).*",
)
db = DBshp("_out/municipios.db")
db.execute("DROP TABLE AREA_INFLUENCIA")
db.close(vacuum=True)
zipfile("_out/municipios.db", "_out/municipios.mini.7z")
os.remove("_out/municipios.db")
