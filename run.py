#!/usr/bin/env python3

import sys

from core.dataset import Dataset
from core.db import DBshp, plain_parse_col
from core.common import readlines, zipfile
from core.jfile import jFile
from scipy.interpolate import interp1d
import os

def insert(db, table, shps):
    for key, data in shps.items():
        poli, nombre = data
        centroid = poli.centroid
        if not centroid.within(poli):
            centroid = poli.representative_point()
        db.insert(table, id=key, nombre=nombre, point=centroid, geom=poli)
    db.commit()

def load_csv(db, table, insert):
    table = table.upper()
    file = "dataset/tablas/%s.csv" % table
    j = jFile(file)
    if j.files:
        print("Cargando", file)
        for item in j.items():
            db.insert(table, **item)
        return
    print("Creando ", file)
    db.execute(insert)
    db.save_csv(file, separator=" ", mb=47)

def _setKm(db, j1, j2, min_km, max_km=None, step=5):
    if j1.empty:
        print("Creando ", j1.fullname)
        crs=[]
        for r in range(1, (min_km*2)+4, 3):
            crs.append("select %s crs" % (r/100))
        sql='''
            select
            	R.crs crs,
                Avg(St_Distance(A.point, ST_Buffer(A.point, R.crs), 1)/1000) km
            from
            	municipios A, (%s) R
            group by R.crs
            order by R.crs
        ''' % " union ".join(crs)
        x=[0]
        y=[0]
        for crs, km in db.select(sql, to_tuples=True):
            x.append(km)
            y.append(crs)
        if max_km is None:
            max_km = int(max(x))
        else:
            max_km = min(max_km, int(max(x)))
        kms = list(range(step, max_km+1, step))
        f = interp1d(x, y, kind='quadratic')
        for km, crs in zip(kms, f(kms)):
            db.insert("CRS_KM", crs=crs, km=km)
        db.commit()
        db.save_csv(j1.fullname, separator=" ", mb=47)

    if j1.empty or j2.empty:
        print("Creando ", j2.fullname)
        db.execute("sql/AREA_INFLUENCIA.sql")
        db.save_csv(j2.fullname, separator=" ", mb=47)

def setKm(db):
    file1 = "dataset/tablas/CRS_KM.csv"
    file2 = "dataset/tablas/AREA_INFLUENCIA.csv"
    j1 = jFile(file1)
    j2 = jFile(file2)
    if not j1.empty:
        print("Cargando", file1)
        for item in j1.items():
            db.insert("CRS_KM", **item)
    if not j1.empty and not j2.empty:
        print("Cargando", file2)
        for item in j2.items():
            db.insert("AREA_INFLUENCIA", **item)
    _setKm(db, j1, j2, 500, max_km=700)


database="dataset/municipios.db"
#database="debug.db"
if len(sys.argv)==2:
    database=sys.argv[1]

dataset = Dataset()
dataset.unzip()
db = DBshp(database, parse_col=plain_parse_col, reload=True)
db.execute("sql/base.sql")
db.to_table("CAMBIOS", dataset.cambios, to_file="sql/CAMBIOS.sql")
insert(db, "provincias", dataset.provincias)
insert(db, "municipios", dataset.municipios)
setKm(db)
if False:
    db.execute("sql/distancias/01-create.sql")
    load_csv(db, "dst_provincias", "sql/distancias/02-insert.sql")
    load_csv(db, "dst_municipios", "sql/distancias/03-insert.sql")
    db.execute("sql/distancias/11-complete.sql")
    db.execute("sql/distancias/21-delete.sql")
dataset.populate_datamun(db)

db.commit()
db.close(vacuum=False)
#print(db.zip())
