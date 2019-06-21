#!/usr/bin/env python3

from core.dataset import Dataset
from core.db import DBshp, plain_parse_col
import sys

def insert(db, table, shps, distancias):
    for key, data in shps.items():
        poli, nombre = data
        centroid = poli.centroid
        if not centroid.within(poli):
            centroid = poli.representative_point()
        db.insert(table, id=key, nombre=nombre,
                lat=centroid.y, lon=centroid.x, geom=poli)
    db.commit()
    ok = set()
    for key, km in distancias:
        if km == 0:
            a, b = key
            ok.add((a, b))
            db.insert("DST_"+table, a=a, b=b, km=0)
    db.commit()
    return None
    sql='''
        select
          A.ID A,
          B.ID B,
          ST_Distance(A.geom, B.geom, 0)/1000 km
        from
          provincias A JOIN %s B ON A.ID>B.ID
        where
          A.ID>B.ID and not(
    '''.rstrip() % table
    for a, b in ok:
        sql=sql+" or (A.ID='%s' and B.ID='%s')" % (a, b)
    sql = sql+")"
    for r in db.select(sql, to_bunch=True):
        db.insert("DST_"+table, a=r.a, b=r.b, km=r.km)
        db.insert("DST_"+table, a=r.b, b=r.a, km=r.km)
    db.commit()

dataset = Dataset()
dataset.unzip()
db = DBshp("debug.db", parse_col=plain_parse_col)
db.execute("sql/base.sql")
insert(db, "provincias", dataset.provincias, dataset.distancias)
insert(db, "municipios", dataset.municipios, dataset.distancias)
#db.execute("sql/distancias.sql")
db.close()
sys.exit()
dataset.populate_datamun(db)
db.close()
