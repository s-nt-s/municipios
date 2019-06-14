import csv
import json
import os
import re
import sqlite3
import zipfile
from datetime import datetime
from glob import glob, iglob
from io import BytesIO
from urllib.parse import parse_qs, urljoin, urlparse

import requests
import urllib3
import xlrd
import yaml
from bs4 import BeautifulSoup
from bunch import Bunch
from unidecode import unidecode

try:
    from .mdb_to_sqlite import mdb_to_sqlite
except:
    from mdb_to_sqlite import mdb_to_sqlite

urllib3.disable_warnings()

cYear = datetime.now().year

re_entero = re.compile(r"^\d+(\.0+)?$")
re_float = re.compile(r"^\d+\.\d+$")


def mkBunchParse(obj):
    if isinstance(obj, list):
        for i, v in enumerate(obj):
            obj[i] = mkBunchParse(v)
        return obj
    if isinstance(obj, dict):
        data = []
        # Si la clave es un año lo pasamos a entero
        flag = True
        for k in obj.keys():
            if not isinstance(k, str):
                return {k: mkBunchParse(v) for k, v in obj.items()}
            if not(k.isdigit() and len(k) == 4 and k[0] != "0"):
                flag = False
        if flag:
            return {int(k): mkBunchParse(v) for k, v in obj.items()}
        obj = Bunch(**{k: mkBunchParse(v) for k, v in obj.items()})
        return obj
    return obj


def mkBunch(file):
    if not os.path.isfile(file):
        return None
    ext = file.rsplit(".", 1)[-1]
    with open(file, "r") as f:
        if ext == "json":
            data = json.load(f)
        elif ext == "yml":
            data = list(yaml.load_all(f))
            if len(data) == 1:
                data = data[0]
    data = mkBunchParse(data)
    return data


def sqlite_to_dict(db, sql):
    sql = sql.strip()
    data = {}
    con = sqlite3.connect(db)
    c = con.cursor()
    c.execute(sql)
    for row in c.fetchall():
        k, v = row
        if k and v:
            data[k] = v
    c.close()
    con.close()
    return data


def get_xls(url):
    file = url_to_file(url, ".xls")
    if not os.path.isfile(file):
        r = requests.get(url, verify=False)
        save(file, r.content)
    book = xlrd.open_workbook(file)
    return book


def read_js_glob(path):
    data = {}
    for p in sorted(glob(path)):
        year = p[-9:-5]
        if year.isdigit():
            year = int(year)
            if year < cYear:
                data[year] = read_js(p)
    return data


def get_mes(mes):
    mes, _ = mes.split(None, 1)
    mes = mes.lower()
    mes = "enero febrero marzo abril mayo junio julio agosto septiembre octubre noviembre diciembre".split().index(mes)
    return mes+1


def unzip(target, *urls):
    if os.path.isdir(target):
        return
    os.makedirs(target, exist_ok=True)
    for url in urls:
        response = requests.get(url, verify=False)
        filehandle = BytesIO(response.content)
        with zipfile.ZipFile(filehandle, 'r') as zip:
            zip.extractall(target)
    mdbs = set()
    for e in ("mdb", "accdb"):
        mdbs = mdbs.union(set(iglob(target+"/**/*."+e)))
        mdbs = mdbs.union(set(glob(target+"/*."+e)))
    for mdb in sorted(mdbs):
        mdb_to_sqlite(mdb)


def get_yml(yml_file, **kargv):
    with open(yml_file, 'r') as f:
        for i in yaml.load_all(f):
            for k, v in kargv.items():
                if k not in i:
                    i[k] = v
            if "viejo" in i and "cod" not in i:
                i["cod"] = i["viejo"].split()[0]
            yield Bunch(i)


def parse_cell(c):
    if isinstance(c, str):
        c = c.strip()
        if len(c) == 0 or c == ".":
            return None
        if re_entero.match(c):
            return int(c.split(".")[0])
        if re_float.match(c):
            return float(c)
        return c
    if isinstance(c, float) and int(c) == c:
        return int(c)
    return c


def read_csv(file, enconde="utf-8", delimiter=","):
    rows = []
    if file and os.path.isfile(file):
        with open(file, 'r', encoding=enconde) as f:
            for row in csv.reader(f, delimiter=delimiter):
                row = [parse_cell(i) for i in row]
                flag = False
                for r in row:
                    if r is not None:
                        flag = True
                if flag:
                    rows.append(row)
        return rows
    return None


def csvBunch(file, enconde="utf-8", delimiter=","):
    arr = []
    with open(file, 'r', encoding=enconde) as f:
        for row in csv.DictReader(f, delimiter=delimiter):
            y = row.get("YEAR", None)
            if y is not None:
                row["YEAR"] = int(y)
            arr.append(Bunch(**row))
    return arr


def get_csv(url, enconde=None, delimiter=","):
    file = url_to_file(url, ".csv")
    j = read_csv(file, enconde=enconde, delimiter=delimiter)
    if j:
        return j
    r = requests.get(url, verify=False)
    save(file, r.content)
    return read_csv(file, enconde=enconde, delimiter=delimiter)


def get_cod_municipio(prov, num, *args, cambiar=None, **kargs):
    cod = _get_cod_municipio(prov, num, *args, **kargs)
    if cod is None:
        return None
    if len(cod) > 5:
        cod = cod[:5]
    if not cod.isdigit() and cod[:3].isdigit():
        cod = cod[:3]
    if len(cod) == 3 and prov is not None:
        cod = prov + cod
    if cod.endswith("000") or len(cod) != 5 or not cod.isdigit():
        return None
    if cambiar and cod in cambiar:
        return cambiar[cod]
    return cod


def _get_cod_municipio(prov, mun):
    n_cod = mun["Variable"]["Codigo"]
    if n_cod == "municipios":
        n = mun["Nombre"].split()[0]
        return n
    if n_cod == "MUN":
        n = mun["Codigo"]
        return n
    if mun["Variable"]["Nombre"] == "Municipios":
        n = mun["Codigo"]
        return n
    return None


def get_bs(url, parser='lxml'):
    r = requests.get(url, verify=False)
    soup = BeautifulSoup(r.content, parser)
    for a in soup.findAll("a"):
        href = a.attrs.get("href", None)
        if href is not None and not href.startswith("#"):
            a.attrs["href"] = urljoin(url, href)
    return soup


def read_js(file, intKey=False, maxKey=None):
    if file and os.path.isfile(file):
        with open(file, 'r') as f:
            js = json.load(f)
            if intKey:
                js = {int(k): v for k, v in js.items() if (
                    maxKey is None or int(k) < maxKey)}
            return js
    return None


def _js(url):
    r = requests.get(url, verify=False)
    j = r.json()
    if "status" in j:
        time.sleep(60)
        return _js(url)
    return r


def get_root_file(dom):
    if dom == "administracionelectronica.navarra.es":
        return "navarra"
    if dom == "opendata.euskadi.eus":
        return "euskadi"
    if dom == "sede.sepe.gob.es":
        return "sepe"
    if dom in ("www.ine.es", "servicios.ine.es"):
        return "ine"
    if dom == "datos.gob.es":
        return "datos_gob"
    return "otros"


def get_js(url):
    file = url_to_file(url, ".json")
    j = read_js(file)
    if j:
        return j
    r = _js(url)
    save(file, r.content)
    return r.json()


def url_to_file(url, ext):
    file = None
    parsed_url = urlparse(url)
    root = get_root_file(parsed_url.netloc) + "/"

    if url.startswith("https://administracionelectronica.navarra.es/GN.InstitutoEstadistica.Web/DescargaFichero.aspx"):
        query = parse_qs(parsed_url.query)
        if query and "Fichero" in query:
            file = root+query["Fichero"][0].replace("\\", "/")
    else:
        for u in (
            "https://sede.sepe.gob.es/es/",
            "http://servicios.ine.es/wstempus/js/es/",
            "http://www.ine.es/jaxiT3/files/t/es/",
            "https://datos.gob.es/",
            "http://opendata.euskadi.eus/contenidos/"
        ):
            if url.startswith(u):
                file = url[len(u):]
                file = root + file

    if not file:
        return None
    file = file.split("?", 1)[0]
    file = file.replace("//", "/")
    file = "fuentes/" + file
    if not file.endswith(ext):
        file = file + ext
    return file


def parse_td(td):
    td = td.get_text().strip()
    if len(td) == 0:
        return None
    td = td.replace(".", "")
    td = td.replace(",", ".")
    td = float(td)
    if td == int(td):
        return int(td)
    return td


def sort_col(s):
    sexo, edad = s.split(" ", 1)
    arr = [sexo] + [int(i) for i in re.findall('\d+', edad)]
    while len(arr) < 3:
        arr.append(999)
    return tuple(arr)


def wstempus(url):
    parsed_url = urlparse(url)
    qs = parse_qs(parsed_url.query)
    url = "http://servicios.ine.es/wstempus/js/es/DATOS_TABLA%s%s?tip=AM" % (
        qs["path"][0], qs["file"][0])
    return url