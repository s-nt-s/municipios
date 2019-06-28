import os
import re
import sqlite3
import textwrap
from subprocess import DEVNULL, STDOUT, check_call

import shapefile
import unidecode
import yaml
from bunch import Bunch
from shapely.geometry import MultiPolygon, Point, Polygon, shape
from shapely.ops import cascaded_union

re_select = re.compile(r"^\s*select\b")
re_sp = re.compile(r"\s+")
re_largefloat = re.compile("(\d+\.\d+e-\d+)")


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

def zipfile(file):
    zip = os.path.splitext(file)[0]+".7z"
    if os.path.isfile(zip):
        os.remove(zip)
    cmd = "7z a %s ./%s" % (zip, file)
    check_call(cmd.split(), stdout=DEVNULL, stderr=STDOUT)
    return size(zip)

def size(file, suffix='B'):
    num = os.path.getsize(file)
    for unit in ('', 'K', 'M', 'G', 'T', 'P', 'E', 'Z'):
        if abs(num) < 1024.0:
            return ("%3.1f%s%s" % (num, unit, suffix))
        num /= 1024.0
    return ("%.1f%s%s" % (num, 'Yi', suffix))


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
    def __init__(self, file, extensions=None, reload=False, parse_col=None):
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
        self.parse_col = parse_col if parse_col is not None else lambda x: x
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
            if isinstance(v, MultiPolygon) or isinstance(v, Polygon) or isinstance(v, Point):
                vals[i] = parse_wkt(vals[i].wkt)
                prm[i] = 'GeomFromText(?, 4326)'
        sql = "insert into %s (%s) values (%s)" % (
            table, ', '.join(keys), ', '.join(prm))
        self.cursor.execute(sql, vals)

    def _build_select(self, sql):
        sql = sql.strip()
        if not sql.lower().startswith("select"):
            field = "*"
            if "." in sql:
                sql, field = sql.rsplit(".", 1)
            sql = "select "+field+" from "+sql
        return sql

    def commit(self):
        self.con.commit()

    def close(self, vacuum=True):
        self.con.commit()
        self.cursor.close()
        if vacuum:
            self.con.execute("VACUUM")
        self.con.commit()
        self.con.close()

    def select(self, sql, to_bunch=False, to_tuples=False):
        sql = self._build_select(sql)
        self.cursor.execute(sql)
        r = build_result(self.cursor, to_bunch=to_bunch, to_tuples=to_tuples)
        return r

    def get_sql_table(self, table):
        sql = "SELECT sql FROM sqlite_master WHERE type='table' AND name=?"
        self.cursor.execute(sql, (table,))
        sql = self.cursor.fetchone()[0]
        return sql

    def size(self, file=None, suffix='B'):
        return size(file or self.file)

    def zip(self):
        return zipfile(self.file)

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

    def select_to_file(self, sql, file, format=None):
        name, ext = os.path.splitext(file)
        if ext == ".7z":
            file = name+".txt"
        with open(file, "w") as f:
            for r in self.select(sql, to_tuples=True):
                r=[int(i) if isinstance(i, float) and int(i)==i else i for i in r]
                if format is None:
                    line = " ".join(['' if i is None else str(i) for i in r]) + "\n"
                else:
                    line = format.format(*r)
                f.write(line)
        if ext == ".7z":
            zipfile(file)
            os.remove(file)

    def select_to_table(self, table, select_sql, create=True, to_file=None):
        select_sql = textwrap.dedent(select_sql).strip()
        table = table.upper()
        sql=''
        if create:
            self.cursor.execute(select_sql)
            cols = [col[0] for col in self.cursor.description]
            columns = {}
            for r in self.cursor.fetchall():
                for i, name in enumerate(cols):
                    if name not in columns:
                        v = r[i]
                        if v is not None:
                            if isinstance(v, int) or isinstance(v, bool):
                                columns[name] = 'INTEGER'
                            elif isinstance(v, float):
                                columns[name] = 'REAL'
                            elif isinstance(v, str):
                                columns[name] = 'TEXT'
                            elif isinstance(v, bytes):
                                columns[name] = 'BLOB'
                if len(columns) == len(cols):
                    break
            for name in cols:
                if name not in columns:
                    columns[name] = 'TEXT'
            sql = "DROP TABLE IF EXISTS {0};\n\nCREATE TABLE {0} (".format(table)
            for name in cols:
                sql = sql+'\n  "'+name+'" '+columns[name]+","
            sql = sql[:-1]+"\n);\n"
        else:
            cols=self.tables[table]
        sql = sql+'INSERT INTO {} ("{}")\n{}'.format(table,
                                                     '", "'.join(cols), select_sql)
        if not sql.endswith(";"):
            sql = sql+";"
        self.execute(sql, to_file=to_file)

    def select_to_csv(self, file, sql):
        sql = self._build_select(sql)
        self.cursor.execute(sql+" limit 0")
        cols = tuple(col[0] for col in self.cursor.description)
        self.cursor.execute(sql)
        with open(file, "w") as f:
            f.write(",".join(cols))
            for row in self.cursor.fetchall():
                f.write("\n")
                row = ['' if r is None else str(r) for r in row]
                f.write(",".join(row))

    def dict_to_table(self, table, rows, to_file=None):
        keys={}
        for r in rows:
            for k, v in r.items():
                if v is not None:
                    k=self.parse_col(k)
                    vals = keys.get(k, set())
                    vals.add(type(v))
                    keys[k]=vals
        sql = "DROP TABLE IF EXISTS {0};\n\nCREATE TABLE {0} (".format(table)
        for name, vals in keys.items():
            tp = "TEXT"
            if bool in vals:
                tp = "INTEGER"
                vals.remove(bool)
            if int in vals:
                tp = "INTEGER"
                vals.remove(int)
            if float in vals:
                tp = "REAL"
                vals.remove(float)
            if str in vals:
                tp = "TEXT"
                vals.remove(str)
            if bytes in vals:
                tp = "BLOB"
                vals.remove(bytes)
            elif vals:
                tp = "TEXT"
            sql = sql+'\n  "'+name+'" '+tp+","
        sql = sql[:-1]+"\n);\n"
        self.execute(sql, to_file=to_file)
        for r in rows:
            self.insert(table, **r)
        self.commit()


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


def parse_wkt(wkt):
    ori = wkt
    for n in re_largefloat.findall(wkt):
        f = float(n)
        s = ("%.025f" % f).rstrip("0")
        n = re.escape(n)
        wkt = re.sub(r"\b"+n+r"\b", s, wkt)
    return wkt


if __name__ == "__main__":
    pass
