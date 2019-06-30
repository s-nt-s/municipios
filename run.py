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

def load_csv(db, file, sql, *other_sql):
    if os.path.isfile(file):
        db.load_csv(file, separator=" ")
    else:
        db.execute(sql)
        db.save_csv(file, separator=" ", sorted=True)
    for sql in other_sql:
        db.execute(sql)

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
load_csv(db, "dataset/DST_PROVINCIAS.csv", "sql/create_distancias.sql", "sql/insert_distancias.sql")
dataset.populate_datamun(db)
db.commit()
db.close()
#print(db.zip())
