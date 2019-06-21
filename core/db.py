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
import textwrap

re_select = re.compile(r"^\s*select\b")
re_sp = re.compile(r"\s+")
re_largefloat=re.compile("(\d+\.\d+e-\d+)")

def plain_parse_col(c):
    c = re_sp.sub(" ", c).strip()
    c = c.lower()
    c = unidecode.unidecode(c)
    c = c.replace(" ", "_")
    return c

def save(file, content):
    if file and content:
        content = textwrap.dedent(content).strip()
        with open(file, "w") as f:
            f.write(content)

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
        #self.cursor.execute('pragma foreign_keys = on')
        self.tables = None
        self.parse_col=parse_col if parse_col is not None else lambda x: x
        self.load_tables()

    def execute(self, sql, to_file=None):
        if os.path.isfile(sql):
            with open(sql, 'r') as schema:
                sql = schema.read()
        save(to_file, sql)
        self.cursor.executescript(sql)
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
            if isinstance(v, MultiPolygon) or isinstance(v, Polygon):
                vals[i] = parse_wkt(vals[i].wkt)
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
        cmd = "7z a %s ./%s" % (zip, self.file)
        check_call(cmd.split(), stdout=DEVNULL, stderr=STDOUT)
        return self.size(zip)

    def create(self, template, *cols, to_file=None, **kargv):
        sql = ""
        for c in cols:
            c = self.parse_col(c)
            sql = sql + '"%s" INTEGER,\n' % c
        for c, t in kargv.items():
            c = self.parse_col(c)
            sql = '"%s" %s,\n' % (c, t)
        sql = sql.strip()
        sql = textwrap.dedent(template) % sql
        sql = sql.strip()
        save(to_file, sql)
        self.cursor.execute(sql)
        self.con.commit()
        self.load_tables()

    def select_to_table(self, table, select_sql, to_file=None):
        select_sql = textwrap.dedent(select_sql).strip()
        table=table.upper()
        self.cursor.execute(select_sql)
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
        sql="DROP TABLE IF EXISTS {0};\n\nCREATE TABLE {0} (".format(table)
        for name in cols:
            sql=sql+'\n  "'+name+'" '+columns[name]+","
        sql=sql[:-1]+"\n);\n"
        sql=sql+'INSERT INTO {} ("{}")\n{}'.format(table, '", "'.join(cols), select_sql)
        if not sql.endswith(";"):
            sql = sql+";"
        self.execute(sql, to_file=to_file, multiple=True)


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
    def __init__(self, *args, file="dataset/municipios.db", reload=False, **kargv):
        DBshp.__init__(self, file, *args, reload=reload, **kargv)
        if reload:
            self.create_database()

    def create_database(self, reload=False):
        self.execute("sql/base.sql")
        insert_shapes(self, "provincias", "fuentes/fomento/shp/**/recintos*provinciales*.shp", i_key=(4,6))
        insert_shapes(self, "municipios", "fuentes/fomento/shp/**/recintos*municipales*.shp", i_key=(6,11))

def insert_shapes(db, table, path_glob, r_key=4, i_key=(6,11), r_data=5):
    ini, end = i_key
    dShapes = {}
    for _shp in iglob(path_glob):
        print(_shp)
        with shapefile.Reader(_shp) as shp:
            for sr in shp.shapeRecords():
                if sr.shape.points and sr.record and len(sr.record) > 4:
                    natcode = sr.record[r_key]
                    key = natcode[ini:end]
                    if key.isdigit():
                        vals = dShapes.get(key, [])
                        poli = shape(sr.shape)
                        if isinstance(poli, Polygon):
                            poli = MultiPolygon([poli])
                        vals.append((poli, sr.record[r_data]))
                        dShapes[key] = vals
    for key, vals in list(dShapes.items()):
        nombre = set()
        poli = []
        for p, n in vals:
            poli.append(p)
            nombre.add(n)
        if len(nombre)>1:
            print(key, nombre)
        nombre = nombre.pop()
        main = None
        for ps in poli:
            for p in ps:
                if main is None or main.area < p.area:
                    main = p
        if len(poli) == 1:
            poli = poli[0]
        else:
            poli = cascaded_union(poli)
        dShapes[key]=poli
        centroid = poli.centroid
        if not centroid.within(poli):
            centroid = poli.representative_point()
        db.insert(table, id=key, nombre=nombre,
                    lat=centroid.y, lon=centroid.x, geom=poli, main_geom=main)
    db.commit()
    db.execute('''
        insert into DST_{0} (A, B, km)
        select ID A, ID B, 0 from {0};
    '''.format(table))
    visto=set()
    ok=set()
    for a, aShp in dShapes.items():
        for b, bShp in dShapes.items():
            if a != b and (a, b) not in visto:
                visto.add((a, b))
                visto.add((b, a))
                if aShp.distance(bShp) == 0:
                    ok.add((a, b))
                    ok.add((b, a))
                    print(a, b)
                    db.insert("DST_"+table, a=a, b=b, km=0)
                    db.insert("DST_"+table, a=b, b=a, km=0)
    db.commit()
    sql='''
        select
          A.ID A,
          B.ID B,
          ST_Distance(A.geom, B.geom, 0)/1000 km
        from
          provincias A JOIN %s B ON A.ID>B.ID
        where
          not((A.ID=B.ID)
    '''.rstrip() % table
    for a, b in ok:
        sql=sql+" or (A.ID='%s' and B.ID='%s')" % (a, b)
    sql = sql+")"
    for r in db.select(sql, to_bunch=True):
        db.insert("DST_"+table, a=r.a, b=r.b, km=r.km)
        db.insert("DST_"+table, a=r.b, b=r.a, km=r.km)
    db.commit()


def parse_wkt(wkt):
    ori = wkt
    for n in re_largefloat.findall(wkt):
        f = float(n)
        s = ("%.025f" % f).rstrip("0")
        n=re.escape(n)
        wkt = re.sub(r"\b"+n+r"\b", s, wkt)
    return wkt

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
