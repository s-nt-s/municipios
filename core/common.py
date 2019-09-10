import csv
import io
import json
import logging
import os
import re
import sqlite3
import time
import zipfile as zipfilelib
from glob import glob, iglob
from io import BytesIO
from subprocess import DEVNULL, STDOUT, check_call
from urllib.parse import parse_qs, urljoin, urlparse

import py7zlib
import requests
import urllib3
import xlrd
import yaml
from bs4 import BeautifulSoup
from bunch import Bunch
from unidecode import unidecode

from .mdb_to_sqlite import mdb_to_sqlite

urllib3.disable_warnings()


def readfile(file, *args):
    if os.path.isfile(file):
        with open(file) as f:
            txt = f.read().strip()
            if len(args):
                txt = txt.format(*args)
            return txt


re_entero = re.compile(r"^\d+(\.0+)?$")
re_float = re.compile(r"^\d+\.\d+$")
aemet_key = readfile("fuentes/aemet.key") or os.environ['AEMET_KEY']


def to_num(st, coma=False):
    s = st.strip() if st else None
    if s is None:
        return None
    try:
        if coma:
            s = s.replace(".", "")
            s = s.replace(",", ".")
        s = float(s)
        if int(s) == s:
            s = int(s)
        return s
    except:
        pass
    return st


def size(*files, suffix='B'):
    num = 0
    for file in files:
        num = num + os.path.getsize(file)
    for unit in ('', 'K', 'M', 'G', 'T', 'P', 'E', 'Z'):
        if abs(num) < 1024.0:
            return ("%3.1f%s%s" % (num, unit, suffix))
        num /= 1024.0
    return ("%.1f%s%s" % (num, 'Yi', suffix))


def get_parts(file):
    arr = []
    if os.path.isfile(file):
        arr.append(file)
    arr.extend(sorted(glob(file+".*")))
    if len(arr) == 0:
        name, ext = os.path.splitext(file)
        if ext != ".7z":
            arr = get_parts(name+".7z")
    return arr


def zipfile(file, mb=47, delete=False, only_if_bigger=False):
    if mb is None:
        mb = 47
    if only_if_bigger == True:
        only_if_bigger = mb
    if only_if_bigger and (only_if_bigger*1024*1024) > os.path.getsize(file):
        return
    zip = os.path.splitext(file)[0]+".7z"
    for z in get_parts(zip):
        os.remove(z)
    cmd = ["7z", "a", zip, "./"+file, "-v%sm" % mb]
    check_call(cmd, stdout=DEVNULL, stderr=STDOUT)
    if delete:
        os.remove(file)
    files = get_parts(zip)
    if len(files) == 1 and files[0].endswith(".7z.001"):
        dst = files[0][:-4]
        os.rename(files[0], dst)
        files = [dst]
    return size(*files)


def save(file, content):
    if file.startswith("fuentes/aemet/"):
        content = content.decode('iso-8859-1')
        content = str.encode(content)
    dir = os.path.dirname(file)
    os.makedirs(dir, exist_ok=True)
    with open(file, "wb") as f:
        f.write(content)


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
            if not(k.isdigit() and len(k) == 4 and int(k[0]) in (1, 2)):
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
            data = list(yaml.load_all(f, Loader=yaml.FullLoader))
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
    if os.path.isfile(file):
        try:
            book = xlrd.open_workbook(file)
            return book
        except Exception as e:
            logging.debug(url, exc_info=True)
    logging.info(url + " --> " + file)
    r = requests.get(url, verify=False)
    save(file, r.content)
    book = xlrd.open_workbook(file)
    return book


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
        logging.info(url + " --> " + target)
        response = requests.get(url, verify=False)
        filehandle = BytesIO(response.content)
        with zipfilelib.ZipFile(filehandle, 'r') as zip:
            zip.extractall(target)
    mdbs = set()
    for e in ("mdb", "accdb"):
        mdbs = mdbs.union(set(iglob(target+"/**/*."+e)))
        mdbs = mdbs.union(set(glob(target+"/*."+e)))
    for mdb in sorted(mdbs):
        mdb_to_sqlite(mdb)


def get_yml(yml_file, **kargv):
    with open(yml_file, 'r') as f:
        for i in yaml.load_all(f, Loader=yaml.FullLoader):
            for k, v in kargv.items():
                if k not in i:
                    i[k] = v
            if "viejo" in i and "cod" not in i:
                i["cod"] = i["viejo"].split()[0]
            yield Bunch(i)


def readcontent(file, name=None):
    if os.path.isfile(file):
        if file.endswith(".7z"):
            with open(file, "rb") as f:
                f7z = py7zlib.Archive7z(f)
                if name is None:
                    name = f7z.getnames()[0]
                txt = f7z.getmember(name)
                for l in io.StringIO(txt.read().decode()):
                    yield l
        else:
            with open(file, "r") as f:
                for l in f.readlines():
                    yield l


def readlines(file, fields=None, name=None):
    for l in readcontent(file, name=name):
        l = l.strip()
        if l and not l.startswith("#"):
            if fields:
                l = l.split(None, fields)
            yield l


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
    try:
        j = read_csv(file, enconde=enconde, delimiter=delimiter)
        if j:
            return j
    except Exception as e:
        logging.debug(file, exc_info=True)
    logging.info(url + " --> " + file)
    r = requests.get(url, verify=False)
    content = r.content
    if file.endswith("ine/csv_c/4721.csv"):
        content = content.decode("UTF-8")
        content = content.replace(", ", " ")
        content = str.encode(content)
    save(file, content)
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
    if n_cod in ("municipios", "municipio"):
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
    try:
        r = requests.get(url, verify=False)
    except:
        logging.critical(url, exc_info=true)
        raise
    soup = BeautifulSoup(r.content, parser)
    for a in soup.findAll("a"):
        href = a.attrs.get("href", None)
        if href is not None and not href.startswith("#"):
            a.attrs["href"] = urljoin(url, href)
    return soup


def read_js(file, intKey=False):
    if "*" in file:
        data = {}
        for p in sorted(glob(file)):
            year = p[-9:-5]
            if year.isdigit():
                year = int(year)
                data[year] = read_js(p)
        return data
    if file and os.path.isfile(file):
        with open(file, 'r') as f:
            js = json.load(f)
            if intKey:
                js = {int(k): v for k, v in js.items()}
            return js
    return None


def requests_js(url):
    try:
        r = requests.get(url, verify=False)
        j = r.json()
        return r, j
    except Exception as e:
        m = re.search(r"<b>description</b>\s*<u>(.+?)\s*\.?\s*</u>", r.text)
        if m:
            logging.debug(url+" : "+m.group(1))
        else:
            logging.debug(url, exc_info=True)
        time.sleep(61)
        return requests_js(url)


def _js(url):
    is_aemet = url.startswith("https://opendata.aemet.es/opendata/api/")
    if is_aemet and aemet_key not in url and url.endswith("="):
        url = url + aemet_key
    r, j = requests_js(url)
    if is_aemet and j.get("estado") == 429:
        time.sleep(61)
        return _js(url)
    if is_aemet and "datos" in j:
        r, j = requests_js(j["datos"])
    if "status" in j:
        time.sleep(61)
        return _js(url)
    return r


re_json1 = re.compile(r"^\[\s*{")
re_json2 = re.compile(r" *}\s*\]$")
re_json3 = re.compile(r"}\s*,\s*{")
re_json4 = re.compile(r"^  ", re.MULTILINE)


def obj_to_js(data):
    txt = json.dumps(data, indent=2)
    txt = re_json1.sub("[{", txt)
    txt = re_json2.sub("}]", txt)
    txt = re_json3.sub("},{", txt)
    txt = re_json4.sub("", txt)
    return txt


def save_js(file, data):
    if "*" in file:
        for year, dt in data.items():
            f = file.replace("*", str(year))
            save_js(f, dt)
    else:
        txt = obj_to_js(data)
        with open(file, "w") as f:
            f.write(txt)


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
    if dom == "opendata.aemet.es":
        return "aemet"
    return "otros"


def get_js(url):
    file = url_to_file(url, ".json")
    try:
        j = read_js(file)
        if j:
            return j
    except Exception as e:
        logging.error(file, exc_info=True)
    r = _js(url)
    logging.info(url + " --> " + file)
    # logging.info(r.encoding)
    # logging.info(r.apparent_encoding)
    # logging.info(type(r.content))
    # apple.decode('iso-8859-1').encode('utf8')
    save(file, r.content)
    return r.json()


re_clima1 = re.compile(
    r"https://opendata.aemet.es/opendata/api/valores/climatologicos/([^/]+)/datos/fechaini/(\d+)-01-01T00:00:00UTC/fechafin/(\d+)-12-31T23:59:59UTC/estacion/([^/]+)/.*")
re_clima2 = re.compile(
    r"https://opendata.aemet.es/opendata/api/valores/climatologicos/([^/]+)/datos/anioini/(\d+)/aniofin/(\d+)/estacion/([^/]+)/.*")


def url_to_file(url, ext):
    file = None
    parsed_url = urlparse(url)
    root = get_root_file(parsed_url.netloc) + "/"

    m1 = re_clima1.match(url)
    m2 = re_clima2.match(url)
    if m1:
        tp, ini, fin, id = m1.groups()
        file = root + "%s/%s/%s-%s.json" % (tp, id, ini, fin)
    elif m2:
        tp, ini, fin, id = m2.groups()
        file = root + "%s/%s/%s.json" % (tp, id, ini)
    elif url.startswith("https://administracionelectronica.navarra.es/GN.InstitutoEstadistica.Web/DescargaFichero.aspx"):
        query = parse_qs(parsed_url.query)
        if query and "Fichero" in query:
            file = root+query["Fichero"][0].replace("\\", "/")
    else:
        for u in (
            "https://opendata.aemet.es/opendata/api/",
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
    if file.endswith("/"):
        file = file[:-1]
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


def sexa_to_dec(i):
    g = i[0:2]
    m = i[2:4]
    s = i[4:6]
    o = i[-1]
    d = int(g) + (int(m) / 60) + (int(s) / 3600)
    if o in ("S", "W"):
        return -d
    return d
