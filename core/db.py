import os
import re
import sqlite3
from glob import iglob
from subprocess import DEVNULL, STDOUT, check_call
import unidecode

import shapefile
import yaml
from bunch import Bunch
from shapely.geometry import MultiPolygon, Point, Polygon, shape
from shapely.ops import cascaded_union

re_select = re.compile(r"^\s*select\b")
re_sp = re.compile(r"\s+")

def plain_parse_col(c):
    c = re_sp.sub(" ", c).strip()
    c = c.lower()
    c = unidecode.unidecode(c)
    c = c.replace(" ", "_")
    return c


class CaseInsensitiveDict(dict):
    def __setitem__(self, key, value):
        dict.__setitem__(self, key.lower(), value)
    def __getitem__(self, key):
        return dict.__getitem__(self, key.lower())

def build_result(c, to_tuples=False, to_bunch=False):
    results = c.fetchall()
    if len(results) == 0:
        return results
    if isinstance(results[0], tuple) and len(results[0]) == 1:
        return [a[0] for a in results]
    if to_tuples:
        return results
    cols = [(i, col[0]) for i, col in enumerate(c.description)]
    n_results = []
    for r in results:
        d = {}
        for i, col in cols:
            d[col] = r[i]
        if to_bunch:
            d = Bunch(**d)
        n_results.append(d)
    return n_results


class DBLite:
    def __init__(self, file, extensions=None, reload=True, parse_col=None):
        self.file = file
        if reload and os.path.isfile(self.file):
            os.remove(self.file)
        self.con = sqlite3.connect(file)
        if extensions:
            self.con.enable_load_extension(True)
            for e in extensions:
                self.con.load_extension(e)
        self.cursor = self.con.cursor()
        self.cursor.execute('pragma foreign_keys = on')
        self.tables = None
        self.parse_col=parse_col if parse_col is not None else lambda x: x
        self.load_tables()

    def execute(self, sql_file):
        if os.path.isfile(sql_file):
            with open(sql_file, 'r') as schema:
                qry = schema.read()
                self.cursor.executescript(qry)
                self.con.commit()
        else:
            self.cursor.execute(sql_file.strip())
            self.con.commit()
        self.load_tables()

    def load_tables(self):
        self.tables = CaseInsensitiveDict()
        for t in self.select("SELECT name FROM sqlite_master WHERE type='table'"):
            self.cursor.execute("select * from "+t+" limit 0")
            self.tables[t] = tuple(col[0] for col in self.cursor.description)

    def insert(self, table, **kargv):
        ok_keys = [k.upper() for k in self.tables[table]]
        keys = []
        vals = []
        for k, v in kargv.items():
            k = self.parse_col(k)
            if k.upper() in ok_keys and v is not None and not(isinstance(v, str) and len(v) == 0):
                keys.append('"'+k+'"')
                vals.append(v)
        prm = ['?']*len(vals)
        for i, v in enumerate(vals):
            if isinstance(v, MultiPolygon):
                vals[i] = vals[i].wkt
                prm[i] = 'GeomFromText(?, 4326)'
        sql = "insert into %s (%s) values (%s)" % (
            table, ', '.join(keys), ', '.join(prm))
        self.cursor.execute(sql, vals)

    def commit(self):
        self.con.commit()

    def close(self):
        self.con.commit()
        self.cursor.close()
        self.con.execute("VACUUM")
        self.con.commit()
        self.con.close()

    def select(self, sql, to_bunch=False, to_tuples=False):
        sql = sql.strip()
        if not sql.lower().startswith("select"):
            field = "*"
            if "." in sql:
                sql, field = sql.rsplit(".", 1)
            sql = "select "+field+" from "+sql
        self.cursor.execute(sql)
        r = build_result(self.cursor, to_bunch=to_bunch, to_tuples=to_tuples)
        return r

    def get_sql_table(self, table):
        sql = "SELECT sql FROM sqlite_master WHERE type='table' AND name=?"
        self.cursor.execute(sql, (table,))
        sql = self.cursor.fetchone()[0]
        return sql

    def size(self, file=None, suffix='B'):
        file = file or self.file
        num = os.path.getsize(file)
        for unit in ('', 'K', 'M', 'G', 'T', 'P', 'E', 'Z'):
            if abs(num) < 1024.0:
                return ("%3.1f%s%s" % (num, unit, suffix))
            num /= 1024.0
        return ("%.1f%s%s" % (num, 'Yi', suffix))

    def zip(self):
        zip = os.path.splitext(self.file)[0]+".7z"
        if os.path.isfile(zip):
            os.remove(zip)
        cmd = "7z a %s %s" % (zip, self.file)
        check_call(cmd.split(), stdout=DEVNULL, stderr=STDOUT)
        return self.size(zip)

    def create(self, template, *cols, **kargv):
        sql = ""
        for c in cols:
            c = self.parse_col(c)
            sql = sql + '"%s" INTEGER,\n' % c
        for c, t in kargv.items():
            c = self.parse_col(c)
            sql = '"%s" %s,\n' % (c, t)
        sql = sql.strip()
        sql = template % sql
        sql = sql.strip()
        self.cursor.execute(sql)
        self.con.commit()
        self.load_tables()

    def select_to_table(self, table, sql, to_file=None):
        table=table.upper()
        self.cursor.execute(sql)
        cols = [col[0] for col in self.cursor.description]
        columns={}
        for r in self.cursor.fetchall():
            for i, name in enumerate(cols):
                if name not in columns:
                    v =r[i]
                    if v is not None:
                        if isinstance(v, int):
                            columns[name]='INTEGER'
                        elif isinstance(v, float):
                            columns[name]='REAL'
                        elif isinstance(v, str):
                            columns[name]='TEXT'
                        elif isinstance(v, bytes):
                            columns[name]='BLOB'
            if len(columns)==len(cols):
                break
        for name in cols:
            if name not in columns:
                columns[name]='TEXT'
        sql="CREATE TABLE {} (".format(table)
        for name in cols:
            sql=sql+'\n  "'+name+'" '+columns[name]+","
        sql=sql[:-1]+"\n);\n"
        sql=sql+"INSERT INTO {} ({})\n{};".format(table, ", ".join(cols), sql)
        save(to_file, sql)




class DBshp(DBLite):
    def __init__(self, *args, **kargv):
        DBLite.__init__(
            self, *args, extensions=['/usr/lib/x86_64-linux-gnu/mod_spatialite.so'], **kargv)

    def within(self, table, lat, lon, to_bunch=False, to_tuples=False):
        table, field = table.rsplit(".", 1)
        sql = '''
            select
                *
            from
                {0}
            where
                within(GeomFromText('POINT({3} {2})'), {1})
                and rowid in (
                    SELECT pkid FROM idx_{0}_geom
                    where xmin < {3}
                    and xmax > {3}
                    and ymin < {2}
                    and ymax > {2}
                )
        '''.format(table, field, lat, lon)
        return self.select(sql, to_bunch=to_bunch, to_tuples=to_tuples)


class DBMun(DBshp):
    def __init__(self, *args, reload=False, **kargv):
        DBshp.__init__(self, "dataset/municipios.db",
                       *args, reload=reload, **kargv)
        if reload:
            self.create_database()

    def create_database(self, reload=False):
        self.execute("sql/municipios.sql")
        muns = {}
        for _shp in iglob("fuentes/fomento/shp/**/recintos*municipales*.shp"):
            print(_shp)
            with shapefile.Reader(_shp) as shp:
                for sr in shp.shapeRecords():
                    if sr.shape.points and sr.record and len(sr.record) > 4:
                        natcode = sr.record[4]
                        cod_provincia = natcode[4:6]
                        cod_municipio = natcode[6:11]
                        if cod_municipio.isdigit():
                            vals = muns.get(cod_municipio, [])
                            poli = shape(sr.shape)
                            if isinstance(poli, Polygon):
                                poli = MultiPolygon([poli])
                            vals.append((poli, sr.record[5]))
                            muns[cod_municipio] = vals
        for cod_municipio, vals in muns.items():
            nombre = set()
            poli = []
            for p, n in vals:
                poli.append(p)
                nombre.add(n)
            nombre = nombre.pop()
            if len(poli) == 1:
                poli = poli[0]
            else:
                poli = cascaded_union(poli)
            centroid = poli.centroid
            if not centroid.within(poli):
                centroid = poli.representative_point()
            self.insert("municipios", id=cod_municipio, nombre=nombre,
                        lat=centroid.y, lon=centroid.x, geom=poli)
        self.commit()


if __name__ == "__main__":
    db = DBMun(reload=True)
    for m in db.select("municipios", to_bunch=True):
        for x in db.within("municipios.geom", lat=m.lat, lon=m.lon, to_bunch=True):
            if m.ID != x.ID:
                print("")
                print(m.ID, m.nombre)
                print(x.ID, x.nombre)
                for j in db.select("select ST_Distance(a.geom, b.geom) distance from municipios a join municipios b on a.ID='%s' and b.ID='%s'" % (m.ID, x.ID), to_bunch=True):
                    print(j)
