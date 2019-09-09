import logging
import os
from pathlib import Path

from scipy.interpolate import interp1d

from .common import readfile, readlines, zipfile
from .dataset import Dataset
from .db import DBshp, plain_parse_col
from .jfile import jFile


def insert(db, table, shps):
    db.openTransaction()
    for key, data in shps.items():
        poli, nombre = data
        centroid = poli.centroid
        if not centroid.within(poli):
            centroid = poli.representative_point()
        db.insert(table, id=key, nombre=nombre, point=centroid, geom=poli)
    db.closeTransaction()


def load_csv(db, table, insert):
    table = table.upper()
    file = "dataset/tablas/%s.csv" % table
    j = jFile(file)
    if j.files:
        logging.info("Cargando " + file)
        db.openTransaction()
        for item in j.items():
            db.insert(table, **item)
        db.closeTransaction()
        return
    logging.info("Creando " + file)
    db.execute(insert)
    db.save_csv(file, separator=" ", mb=47)


def _setKm(db, j1, j2, min_km, max_km=None, step=5):
    if j1.empty:
        logging.info("Creando " + j1.fullname)
        crs = []
        for r in range(1, (min_km*2)+4, 3):
            crs.append("select %s crs" % (r/100))
        sql = '''
            select
            	R.crs crs,
                Avg(St_Distance(A.point, ST_Buffer(A.point, R.crs), 1)/1000) km
            from
            	municipios A, (%s) R
            group by R.crs
            order by R.crs
        ''' % " union ".join(crs)
        x = [0]
        y = [0]
        for crs, km in db.select(sql, to_tuples=True):
            x.append(km)
            y.append(crs)
        if max_km is None:
            max_km = int(max(x))
        else:
            max_km = min(max_km, int(max(x)))
        kms = list(range(step, max_km+1, step))
        f = interp1d(x, y, kind='quadratic')
        db.openTransaction()
        for km, crs in zip(kms, f(kms)):
            db.insert("CRS_KM", crs=crs, km=km)
        db.closeTransaction()
        db.save_csv(j1.fullname, separator=" ", mb=47)

    if j1.empty or j2.empty:
        logging.info("Creando " + j2.fullname)
        db.execute("sql/AREA_INFLUENCIA.sql")
        db.save_csv(j2.fullname, separator=" ", mb=47)


def setKm(db):
    file1 = "dataset/tablas/CRS_KM.csv"
    file2 = "dataset/tablas/AREA_INFLUENCIA.csv"
    j1 = jFile(file1)
    j2 = jFile(file2)
    db.openTransaction()
    if not j1.empty:
        logging.info("Cargando " + file1)
        for item in j1.items():
            db.insert("CRS_KM", **item)
    if not j1.empty and not j2.empty:
        logging.info("Cargando " + file2)
        for item in j2.items():
            db.insert("AREA_INFLUENCIA", **item)
    db.closeTransaction()
    _setKm(db, j1, j2, 500, max_km=700)


def completeAemet(db):
    logging.info("Completando AEMET")
    db.load_tables()
    cols = set()
    create_cols1 = ''
    for c in db.tables["AEMET_DIA"]:
        if c not in ("BASE", "FECHA", "dir") and not c.startswith("hora"):
            cols.add(c)
            fun = "avg"
            if c == "prec":
                fun = "sum"
            elif c in ("racha", "sol") or "max" in c:
                fun = "max"
            elif "min" in c:
                fun = "min"
            create_cols1 = create_cols1+'\n  	{1}({0}) {0},'.format(c, fun)
    create_cols1 = create_cols1[:-1].strip()

    create_cols2 = ''
    for c in db.tables["AEMET_MES"]:
        if c in ("hr", "e", "q_mar"):
            cols.add(c)
            fun = "avg"
            create_cols2 = create_cols2+'\n  	{1}({0}) {0},'.format(c, fun)
    create_cols2 = create_cols2[:-1].strip()
    cols = sorted(cols)
    sql = readfile("sql/aemet_templates/AEMET_DIA_PROV.sql",
                   create_cols1, create_cols2, ",\n  ".join(cols))
    db.execute(sql, to_file="sql/AEMET_DIA_PROV.sql")

    create_cols3 = ''
    create_cols4 = ''
    for c in cols:
        create_cols3 = create_cols3+'\n  	{0} REAL,'.format(c)
        fun = "avg"
        if c in ("sol", "prec"):
            func = "sum"
        elif c in ("racha",) or "max" in c:
            fun = "max"
        elif "min" in c:
            fun = "min"
        create_cols4 = create_cols4+'\n  	{1}({0}) {0},'.format(c, fun)
    create_cols3 = create_cols3[:-1].strip()
    create_cols4 = create_cols4[:-1].strip()
    sql = readfile("sql/aemet_templates/AEMET_SEMANA_PROV.sql",
                   create_cols3, ",\n  ".join(cols), create_cols4)
    db.execute(sql, to_file="sql/AEMET_SEMANA_PROV.sql")


def create_db(salida):
    salida = os.path.realpath(salida)
    logging.info("Salida en "+salida)
    wks_dir = Path(os.path.realpath(__file__))
    wks_dir = wks_dir.parent.parent
    wks_dir = str(wks_dir)
    os.chdir(wks_dir)
    logging.info("cd "+wks_dir)
    dataset = Dataset()
    # dataset.collect()
    dataset.unzip()
    db = DBshp(salida, parse_col=plain_parse_col, reload=True)
    db.execute("sql/base.sql")
    db.to_table("CAMBIOS", dataset.cambios, to_file="sql/CAMBIOS.sql")
    insert(db, "provincias", dataset.provincias)
    insert(db, "municipios", dataset.municipios)
    db.execute("sql/provmun.sql")
    setKm(db)
    if False:
        db.execute("sql/distancias/01-create.sql")
        load_csv(db, "dst_provincias", "sql/distancias/02-insert.sql")
        load_csv(db, "dst_municipios", "sql/distancias/03-insert.sql")
        db.execute("sql/distancias/11-complete.sql")
        db.execute("sql/distancias/21-delete.sql")
    dataset.populate_datamun(db)
    completeAemet(db)

    db.close(vacuum=True)
    # print(db.zip())
