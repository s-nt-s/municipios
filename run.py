#!/usr/bin/env python3

import sys

from core.dataset import Dataset
from core.db import DBshp, plain_parse_col
from core.common import readlines
import os

def insert(db, table, shps):
    for key, data in shps.items():
        poli, nombre = data
        centroid = poli.centroid
        if not centroid.within(poli):
            centroid = poli.representative_point()
        db.insert(table, id=key, nombre=nombre, point=centroid, geom=poli)
    db.commit()

database="dataset/municipios.db"
if len(sys.argv)==2:
    database=sys.argv[1]

dataset = Dataset()
dataset.unzip()
db = DBshp(database, parse_col=plain_parse_col, reload=True)
db.execute("sql/base.sql")
insert(db, "provincias", dataset.provincias)
insert(db, "municipios", dataset.municipios)
distancias="dataset/geografia/dst.7z"
if os.path.isfile(distancias):
    dstM=[]
    dstP=[]
    for a, b, km in readlines(distancias, fields=3):
        km=float(km)
        r = (a, b, km)
        if len(a)==2:
            dstP.append(r)
        else:
            dstM.append(r)
    db.cursor.executemany("insert into DST_PROVINCIAS (A, B, km) values (?, ?, ?)", dstP)
    db.cursor.executemany("insert into DST_MUNICIPIOS (A, B, km) values (?, ?, ?)", dstM)
else:
    db.execute("sql/distancias.sql")
    db.select_to_file('''
        select a, b, km from DST_PROVINCIAS where a>b and km>0
        union
        select a, b, km from DST_MUNICIPIOS where a>b and km>0
    ''',distancias)
db.commit()
dataset.populate_datamun(db)
db.commit()
db.close()
print(db.zip())
