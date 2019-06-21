#!/usr/bin/env python3

from core.dataset import Dataset
from core.db import DBshp, plain_parse_col


def insert(db, table, shps):
    for key, data in shps.items():
        poli, nombre = data
        centroid = poli.centroid
        if not centroid.within(poli):
            centroid = poli.representative_point()
        db.insert(table, id=key, nombre=nombre,
                  lat=centroid.y, lon=centroid.x, geom=poli)
    db.commit()


dataset = Dataset()
dataset.unzip()
db = DBshp("debug.db", parse_col=plain_parse_col)
db.execute("sql/base.sql")
insert(db, "provincias", dataset.provincias)
insert(db, "municipios", dataset.municipios)
dataset.populate_datamun(db)
db.close()
db.zip()
