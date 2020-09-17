import csv
import json
import math
import os
import re
import sqlite3
import textwrap
from datetime import date, datetime, timedelta
from decimal import Decimal

import unidecode
from bunch import Bunch
from shapely.geometry import MultiPolygon, Point, Polygon

from .common import size, zipfile

re_select = re.compile(r"^\s*select\b")
re_sp = re.compile(r"\s+")
re_largefloat = re.compile("(\d+\.\d+e-\d+)")
re_bl = re.compile(r"\n\s*\n", re.IGNORECASE)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def bunch_factory(cursor, row):
    d = dict_factory(cursor, row)
    return Bunch(**d)


def one_factory(cursor, row):
    return row[0]


def ResultIter(cursor, size=1000):
    while True:
        results = cursor.fetchmany(size)
        if not results:
            break
        for result in results:
            yield result


def not_num(*args):
    for a in args:
        if a is None or math.isnan(a) or math.isinf(a):
            return True
    return False


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


def _get_types(object):
    if isinstance(object, list):
        for o in object:
            for r in _get_types(o):
                yield r
    elif isinstance(object, dict):
        complex = False
        for v in object.values():
            if isinstance(v, (dict, list, set, tuple)):
                complex = True
                break
        if complex:
            for o in object.values():
                for r in _get_types(o):
                    yield r
        else:
            for k, v in object.items():
                if v is None or (isinstance(v, str) and not v.strip()):
                    continue
                if isinstance(v, float) and int(v) == v:
                    v = int(v)
                yield k, type(v)


def get_cols(object):
    tps = {}
    for k, tp in _get_types(object):
        t = tps.get(k, set())
        t.add(tp)
        tps[k] = t
    for k, tp in list(tps.items()):
        n_flag, d_flag = False, False
        for sql, pyt in (
            ("INTEGER", bool),
            ("INTEGER", int),
            ("REAL", float),
        ):
            if pyt in tp:
                n_flag = True
                tps[k] = sql
                tp.remove(pyt)
        for sql, pyt in (
            ("DATE", date),
            ("DATETIME", datetime),
        ):
            if pyt in tp:
                d_flag = True
                tps[k] = sql
                tp.remove(pyt)
        if str in tp or (n_flag and d_flag):
            tps[k] = "TEXT"
            if str in tp:
                tp.remove(str)
        if len(tp) > 0:
            tps[k] = "BLOB"
    return tps


def week_ISO_8601(dt):
    if isinstance(dt, str):
        if (len(dt)) > 10:
            dt = dt[:10]
        dt = datetime.strptime(dt, '%Y-%m-%d')
    y, w, _ = dt.isocalendar()
    return round(y + (w/100), 2)


def previous_week(w):
    y = int(w)
    if w > (y+0.01):
        return round(w-0.01, 2)
    y = y - 1
    d = 32
    while True:
        d = d - 1
        dt = date(y, 12, d)
        ys, ws, _ = dt.isocalendar()
        if ys == y:
            return round(y + (ws/100), 2)


def day_of_week(w, weekday, salida=0):
    y = int(w)
    d = date(y, 1, 1)
    while d.isocalendar()[1] != 1:
        d = date(y, 1, d.day+1)
    w_of_first = d.isocalendar()[1]
    while d.weekday() != weekday:
        d = date(y, 1, d.day+1)
        w_of_first = d.isocalendar()[1]
    wk = round((w - int(w))*100)
    wk = wk - w_of_first
    if wk != 0:
        d = d + timedelta(weeks=wk)
    if salida == 1:
        d = round(d.month + (d.day/100), 2)
    if salida == 2:
        d = d.year
    return d


class CaseInsensitiveDict(dict):
    def __setitem__(self, key, value):
        dict.__setitem__(self, key.lower(), value)

    def __getitem__(self, key):
        return dict.__getitem__(self, key.lower())


def get_db(file, *extensions, readonly=False):
    if readonly:
        file = "file:"+file+"?mode=ro"
        con = sqlite3.connect(file, uri=True)
    else:
        con = sqlite3.connect(file)
    if extensions:
        con.enable_load_extension(True)
        for e in extensions:
            con.load_extension(e)
    con.create_function("week_ISO_8601", 1, week_ISO_8601)
    con.create_function("previous_week", 1, previous_week)
    con.create_function("day_of_week", 3, day_of_week)
    return con


class DBLite:
    def __init__(self, file, extensions=None, reload=False, parse_col=None, readonly=False):
        self.readonly = readonly
        self.extensions = extensions or []
        self.file = file
        self.parse_col = parse_col if parse_col is not None else lambda x: x
        if reload and os.path.isfile(self.file):
            os.remove(self.file)
        self.open()

    def open(self):
        self.con = get_db(self.file, *self.extensions, readonly=self.readonly)
        # self.cursor = self.con.cursor()
        #self.cursor.execute('pragma foreign_keys = on')
        self.tables = None
        self.srid = None
        self.inTransaction = False
        self.load_tables()

    def openTransaction(self):
        if self.inTransaction:
            self.con.execute("END TRANSACTION")
        self.con.execute("BEGIN TRANSACTION")
        self.inTransaction = True

    def closeTransaction(self):
        if self.inTransaction:
            self.con.execute("END TRANSACTION")
            self.inTransaction = False

    def read_sql_file(self, sql, *args):
        with open(sql, 'r') as schema:
            sql = schema.read()
            sql = sql.strip()
            sql = re_bl.sub("\n", sql)
            if args:
                sql = sql.format(*args)
        return sql

    def execute(self, sql, to_file=None):
        if os.path.isfile(sql):
            sql = self.read_sql_file(sql)
        if sql.strip():
            save(to_file, sql)
            self.con.executescript(sql)
            self.con.commit()
            self.load_tables()

    @property
    def indices(self):
        for i, in self.select("SELECT name FROM sqlite_master WHERE type='index' order by name"):
            yield i

    def get_cols(self, sql):
        cursor = self.con.cursor()
        cursor.execute(sql)
        cols = tuple(col[0] for col in cursor.description)
        cursor.close()
        return cols

    def load_tables(self):
        self.tables = CaseInsensitiveDict()
        for t, in list(self.select("SELECT name FROM sqlite_master WHERE type='table'")):
            self.tables[t] = self.get_cols("select * from "+t+" limit 0")

    def insert(self, table, **kargv):
        ok_keys = [k.upper() for k in self.tables[table]]
        keys = []
        vals = []
        for k, v in kargv.items():
            if v is None or (isinstance(v, str) and len(v) == 0):
                continue
            if k.upper() not in ok_keys:
                k = self.parse_col(k)
                if k.upper() not in ok_keys:
                    continue
            keys.append('"'+k+'"')
            vals.append(v)
        prm = ['?']*len(vals)
        for i, v in enumerate(vals):
            if isinstance(v, (MultiPolygon, Polygon, Point)):
                vals[i] = parse_wkt(v.wkt)
                prm[i] = 'GeomFromText(?, %s)' % self.srid
            elif isinstance(v, Decimal):
                vals[i] = float(v)
        sql = "insert into %s (%s) values (%s)" % (
            table, ', '.join(keys), ', '.join(prm))
        self.con.execute(sql, vals)

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
        if self.readonly:
            self.con.close()
            return
        self.closeTransaction()
        self.con.commit()
        if vacuum:
            self.con.execute("VACUUM")
        self.con.commit()
        self.con.close()

    def select(self, sql, row_factory=None, **kargv):
        sql = self._build_select(sql)
        self.con.row_factory = row_factory
        cursor = self.con.cursor()
        cursor.execute(sql)
        for r in ResultIter(cursor):
            yield r
        cursor.close()
        self.con.row_factory = None

    def one(self, sql):
        sql = self._build_select(sql)
        cursor = self.con.cursor()
        cursor.execute(sql)
        r = cursor.fetchone()
        cursor.close()
        if not r:
            return None
        if len(r) == 1:
            return r[0]
        return r

    def get_sql_table(self, table):
        sql = "SELECT sql FROM sqlite_master WHERE type='table' AND name=?"
        cursor = self.con.cursor()
        cursor.execute(sql, (table,))
        sql = cursor.fetchone()[0]
        cursor.close()
        return sql

    def size(self, file=None, suffix='B'):
        return size(file or self.file)

    def zip(self):
        return zipfile(self.file)

    def create(self, template, to_file=None, kSort=None, **kargv):
        sql = ""
        keys = sorted(kargv.keys()) if kSort is None else sorted(
            kargv.keys(), key=kSort)
        for c in keys:
            t = kargv[c]
            c = self.parse_col(c)
            sql = sql + '  "%s" %s,\n' % (c, t)
        sql = sql.strip()
        sql = textwrap.dedent(template) % sql
        sql = sql.strip()
        save(to_file, sql)
        self.con.execute(sql)
        self.con.commit()
        self.load_tables()

    def load_csv(self, file, table=None, separator=","):
        if table is None:
            base = os.path.basename(file)
            table, _ = os.path.splitext(base)
        with open(file, "r") as f:
            for row in csv.DictReader(f, delimiter=separator):
                self.insert(table, **row)
        self.commit()

    def save_csv(self, file, sql=None, separator=",", sorted=False, mb=None):
        name, ext = os.path.splitext(file)
        if ext == ".7z":
            file = name+".csv"
        if sql is None:
            base = os.path.basename(file)
            sql, _ = os.path.splitext(base)
        sql = self._build_select(sql)
        cols = get_cols(sql+" limit 0")
        head = separator.join(cols)
        if sorted:
            sql = sql + " order by "+", ".join(head)
        cursor = self.con.cursor()
        cursor.execute(sql)
        rows = ResultIter(cursor)
        with open(file, "w") as f:
            if head:
                f.write(head)
            for row in rows:
                f.write("\n")
                row = list(row)
                for i, v in enumerate(row):
                    if v is None:
                        v = ''
                    elif isinstance(v, float) and int(v) == v:
                        v = int(v)
                    row[i] = str(v)
                line = separator.join(row)
                line = line.rstrip(separator)
                f.write(line)
        cursor.close()
        if ext == ".7z" or mb:
            zipfile(file, only_if_bigger=(ext != ".7z"), delete=True, mb=mb)

    def save_js(self, file, sql=None, indent=None, mb=None, js_ext=".json", parse_result=None):
        separators = (',', ':') if indent is None else None
        name, ext = os.path.splitext(file)
        if ext == ".7z":
            file = name+js_ext
        if sql is None:
            base = os.path.basename(file)
            sql, _ = os.path.splitext(base)
        r = list(self.select(sql, row_factory=dict_factory))
        if parse_result is not None:
            r = parse_result(r)
        with open(file, "w") as f:
            json.dump(r, f, indent=indent, separators=separators)
        if ext == ".7z" or mb:
            zipfile(file, only_if_bigger=(ext != ".7z"), delete=True, mb=mb)

    def to_table(self, table, data, *args, **kargv):
        if isinstance(data, str):
            return self._select_to_table(table, data, *args, **kargv)
        elif isinstance(data, list):
            return self._dict_to_table(table, data, *args, **kargv)
        return None

    def _select_to_table(self, table, select_sql, create=True, sufix=None, to_file=None):
        select_sql = textwrap.dedent(select_sql).strip()
        table = table.upper()
        sql = ''
        if create:
            cursor = self.con.cursor()
            cursor.execute(select_sql)
            cols = [col[0] for col in cursor.description]
            columns = {}
            for r in cursor.fetchall():
                for i, name in enumerate(cols):
                    if name not in columns:
                        v = r[i]
                        if v is not None:
                            if isinstance(v, (int, bool)):
                                columns[name] = 'INTEGER'
                            elif isinstance(v, float):
                                columns[name] = 'REAL'
                            elif isinstance(v, str):
                                columns[name] = 'TEXT'
                            elif isinstance(v, bytes):
                                columns[name] = 'BLOB'
                if len(columns) == len(cols):
                    break
            cursor.close()
            for name in cols:
                if name not in columns:
                    columns[name] = 'TEXT'
            sql = "DROP TABLE IF EXISTS {0};\n\nCREATE TABLE {0} (".format(
                table)
            for name in cols:
                sql = sql+'\n  "'+name+'" '+columns[name]+","
            if sufix:
                sql = sql.strip() + "\n" + sufix.strip()
            else:
                sql = sql[:-1]
            sql = sql + "\n);\n"
        else:
            cols = self.tables[table]
        sql = sql+'INSERT INTO {} ("{}")\n{}'.format(table,
                                                     '", "'.join(cols), select_sql)
        if not sql.endswith(";"):
            sql = sql+";"
        self.execute(sql, to_file=to_file)

    def _dict_to_table(self, table, rows, sufix=None, to_file=None):
        kcols = get_cols(rows)
        sql = "DROP TABLE IF EXISTS {0};\n\nCREATE TABLE {0} (".format(table)
        for name, tp in kcols.items():
            sql = sql+'\n  "'+name+'" '+tp+","
        if sufix:
            sql = sql.strip() + "\n" + sufix.strip()
        else:
            sql = sql[:-1]
        sql = sql + "\n);\n"
        self.execute(sql, to_file=to_file)
        for r in rows:
            self.insert(table, **r)
        self.commit()

    def to_list(self, *args, **kargv):
        rows = []
        for r in self.select(*args, **kargv):
            if isinstance(r, tuple) and len(r) == 1:
                r = r[0]
            rows.append(r)
        return rows


class DBshp(DBLite):
    def __init__(self, *args, srid=4326, **kargv):
        DBLite.__init__(
            self, *args, extensions=['mod_spatialite.so'], **kargv)
        self.srid = srid

    def within(self, table, lat, lon, geom="geom", where=None, **kargv):
        if not_num(lat, lon):
            return None
        if not where:
            where = ''
        else:
            where = where + " and "
        if ".":
            table, field = table.rsplit(".", 1)
        else:
            field = "*"
        sql = '''
            select
                {5}
            from
                {0}
            where {4}
                within(GeomFromText('POINT({3} {2})', {5}), {1})
                and rowid in (
                    SELECT pkid FROM idx_{0}_geom
                    where xmin < {3}
                    and xmax > {3}
                    and ymin < {2}
                    and ymax > {2}
                )
        '''.format(table, geom, lat, lon, where, field, self.srid)
        return self.one(sql)

    def distance(self, table, lat, lon, geom="geom", where=None, use_ellipsoid=None, **kargv):
        if not_num(lat, lon):
            return None
        if use_ellipsoid == True:
            use_ellipsoid = ", 1"
        elif use_ellipsoid == False:
            use_ellipsoid = ", 0"
        else:
            use_ellipsoid = ''
        if where:
            where = "where " + where
        else:
            where = ''
        sql = '''
            select
                ST_Distance(GeomFromText('POINT({3} {2})', {6}), {1}{5}) dis
            from
                {0} {4}
        '''.format(table, geom, lat, lon, where, use_ellipsoid, self.srid)
        return self.one(sql)

    def nearest(self, table, lat, lon, geom="geom", where=None):
        if not_num(lat, lon):
            return None
        if where:
            where = "where " + where
        else:
            where = ''
        if ".":
            table, field = table.rsplit(".", 1)
        else:
            field = "*"
        sql = '''
            select {5} from (
                select
                    {5},
                    ST_Distance(GeomFromText('POINT({3} {2})', {6}), {1}) dis
                from
                    {0} {4}
            ) order by dis asc
        '''.format(table, geom, lat, lon, where, field, self.srid)
        return self.one(sql)


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
