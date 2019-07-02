#!/usr/bin/env python3

import sys

from core.dataset import Dataset
from core.db import DBshp, plain_parse_col
from core.common import readlines, zipfile
from core.jfile import jFile
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
        j.close()
        return
    print("Creando ", file)
    db.execute(insert)
    db.save_csv(file, separator=" ")
    zipfile(file, only_if_bigger=True, delete=True)

database="dataset/municipios.db"
if len(sys.argv)==2:
    database=sys.argv[1]

dataset = Dataset()
dataset.unzip()
db = DBshp(database, parse_col=plain_parse_col, reload=True)
db.execute("sql/base.sql")
db.to_table("CAMBIOS", dataset.cambios, to_file="sql/CAMBIOS.sql")
insert(db, "provincias", dataset.provincias)
insert(db, "municipios", dataset.municipios)
db.execute("sql/distancias/01-create.sql")
load_csv(db, "dst_provincias", "sql/distancias/02-insert.sql")
load_csv(db, "dst_municipios", "sql/distancias/03-insert.sql")
db.execute("sql/distancias/11-complete.sql")
db.execute("sql/distancias/21-delete.sql")
dataset.populate_datamun(db)

db.commit()
db.close(vacuum=False)
#print(db.zip())
