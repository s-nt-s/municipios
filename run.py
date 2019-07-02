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

def get_cache(file):
    try:
        j = jFile(file)
        gen=j.tuples()
        next(gen)
        d = {(a, b): crs for a, b, crs in gen}
        return d
    except:
        pass
    return {}

def create_distancias(db, table, capas=None, field="geom"):
    table = table.upper()
    file = "dataset/tablas/DST_%s.csv" % table
    dsts = get_cache(file)
    db.execute('''
        create table DST_{0} (
          A TEXT,
          B TEXT,
          crs REAL,
          PRIMARY KEY (A, B),
          FOREIGN KEY(A) REFERENCES {0}(ID),
          FOREIGN KEY(B) REFERENCES {0}(ID)
        )
    '''.format(table), to_file="sql/DST_%s.sql" % table)
    sql='''
        select
            A.ID A,
            B.ID B,'''
    if field == "geom":
        sql = sql + '''
            case
                when Intersects(A.geom, B.geom) = 1 then 0
                when ST_Touches(A.geom, B.geom) = 1 then 0
                else null
            end crs'''
    else:
        sql = sql + '''
            null crs'''
    sql = sql + '''
        from {0} A JOIN {0} B on A.ID>B.ID
    '''.format(table)
    ab = db.select(sql, to_tuples=True)
    total = len(ab)
    with open(file, "w") as f:
        f.write("A B crs\n")
        for i, (a, b, crs) in enumerate(ab):
            prc = int((i/total)*100)
            print("Creando DST_{} {}% [{}]        ".format(table, prc, total-i), end="\r")
            if crs is None:
                crs = dsts.get((a,b))
                if crs is None:
                    if capas:
                        iA = capas[a][0]
                        iB = capas[b][0]
                        crs = iA.distance(iB)
                    else:
                        crs = db.select('''
                            select
                                ST_Distance(A.{1}, B.{1}) crs
                            from
                                {0} A, {0} B
                            where
                                A.ID='{2}' and B.ID='{3}'
                            '''.format(table, field, a, b), to_one=True)
                    if int(crs)==crs:
                        crs=int(crs)
            f.write("%s %s %s\n" %(a, b, crs))
            if i % 100 == 0:
                f.flush()
    print("Creando DST_{} 100%           ".format(table))
    db.load_csv(file, separator=" ")
    db.commit()
    db.execute('''
        INSERT INTO DST_{0} (A, B, crs)
        select B, A, crs from DST_{0};
        INSERT INTO DST_{0} (A, B, crs)
        select ID A, ID B, 0 from {0};
    '''.format(table))
    db.commit()
    zipfile(file, only_if_bigger=True)

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
create_distancias(db, "provincias", capas=dataset.provincias, field="geom")
create_distancias(db, "municipios", field="point")
dataset.populate_datamun(db)

db.commit()
db.close()
#print(db.zip())
