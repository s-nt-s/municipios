import os
import re
from datetime import date, datetime
from functools import lru_cache

import requests
import shapefile
import urllib3
import xlrd
import yaml
from bs4 import BeautifulSoup
from bunch import Bunch
from shapely.geometry import MultiPolygon, Point, Polygon, shape
from shapely.ops import cascaded_union

from .common import *
from .db import get_cols
from .decorators import JsonCache, KmCache, ParamJsonCache
from .provincias import *

me = os.path.realpath(__file__)
dr = os.path.dirname(me)
re_nb = re.compile(r"(\d+)")
re_ft = re.compile(r"^-?\d+(,\d+)?$")

cYear = datetime.now().year


def insert_rel_mun(db, table, rows, kSort=None):
    table = table.upper()
    create = '''
        create table {} (
          MUN TEXT,
          YR INTEGER,
          %s
          PRIMARY KEY (MUN, YR),
          FOREIGN KEY(MUN) REFERENCES municipios(ID)
        )
    '''.format(table)
    db.create(create, **get_cols(rows), kSort=kSort,
              to_file="sql/%s.sql" % table)
    db.openTransaction()
    for key, row in rows.items():
        row["MUN"], row["YR"] = key
        db.insert(table, **row)
    db.closeTransaction()
    del rows

def sortColPob(s):
    if s == "total":
        sexo = "ambos"
        edad = s
    else:
        spl = s.split(" ", 1)
        if len(spl) == 1:
            sexo = "ambos"
            edad = spl[0]
        else:
            sexo, edad = spl
    if edad == "total":
        edad = "9999"
    nums = [int(i) for i in re_nb.findall(edad)]
    return (sexo, tuple(nums))


def getShp(path_glob, ini, end, r_key=4, r_data=5):
    dShapes = {}
    for _shp in iglob(path_glob):
        print(_shp)
        #_shp = os.path.realpath(_shp)
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
        if len(nombre) > 1:
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
        dShapes[key] = (poli, nombre)
    return dShapes


class Dataset():
    def __init__(self, *args, core=None, reload=False, **kargs):
        self.reload = reload
        self.file = "fuentes/fuentes.json"
        self.fuentes = mkBunch("fuentes/indice.yml")
        self.cambios = list(sorted(get_yml("fuentes/cambios_municipios.yml",
                                           remplaza=False, municipio=True, notas=""), key=lambda x: x.cod))
        self.core = core
        if self.core is None:
            self.core = mkBunch(self.file)
        if not self.core:
            self.collect()

    def getCore(self, field):
        for cod, data in sorted(self.core.items()):
            if field in data:
                yield cod, data[field]

    def unzip(self):
        unzip("fuentes/fomento/shp", self.core.todas["limites"])
        unzip("fuentes/fomento/mdb",
              self.core.todas["nomenclator"])  # , self.core.todas["nomenclator_basico"])
        for pro, dt in self.core.items():
            if "miteco" in dt:
                unzip("fuentes/miteco/mapaforestal/%s %s" %
                      (pro, dt["nombre"].split("/")[0]), dt["miteco"])

    def parseData(self, data):
        for nuevo, viejos in self.mun_remplaza.items():
            for year, dt in data.items():
                dNuevo = dt.get(nuevo, {})
                for viejo in viejos:
                    if viejo in dt:
                        dViejo = dt[viejo]
                        del dt[viejo]
                        for k, v in dViejo.items():
                            dNuevo[k] = dNuevo.get(k, 0) + v
                if dNuevo:
                    dt[nuevo] = dNuevo
        return data

    @JsonCache(file="dataset/poblacion/edades.json")
    def create_edades(self):
        data = {}
        for prov, dt in self.core.items():
            pob = dt.get("poblacion1", None)
            if pob is None:
                continue
            for year, url in pob.items():
                year = int(year)
                if year >= cYear:
                    continue
                js = get_js(url)
                if not js:
                    continue
                yData = data.get(year, {})
                for d in js:
                    if d["MetaData"][0]["Codigo"] != "ambossexos":
                        continue
                    mun = get_cod_municipio(prov, d["MetaData"][1])
                    if mun is None:
                        continue
                    mData = yData.get(mun, {})
                    edad = d["MetaData"][2]["Codigo"]
                    if edad == "total":
                        mData[-1] = int(d["Data"][0]["Valor"])
                    elif edad.isdigit():
                        mData[int(edad)] = int(d["Data"][0]["Valor"])
                    yData[mun] = mData
                data[year] = yData
        for year, yData in data.items():
            for mun, mData in list(yData.items()):
                arr = []
                for i in range(0, max(mData.keys())+1):
                    x = mData.get(i, 0)
                    arr.append(str(x) if x > 0 else "")
                yData[mun] = (",".join(arr)).rstrip(",")
        return data

    @JsonCache(file="dataset/renta/euskadi.json")
    def create_euskadi(self):
        url = self.core.todas["renta"]["euskadi"]
        rows = get_csv(url, enconde="windows-1252", delimiter=";")
        years = None
        data = {}
        for r in rows:
            c0 = r[0]
            if years is None:
                if c0 == "Código municipio":
                    years = [iy for iy in enumerate(
                        r) if isinstance(iy[1], int)]
                continue
            mun = "%05d" % c0

            for i, y in years:
                if y >= cYear:
                    continue
                v = r[i]
                v = v.replace(".", "")
                v = v.replace(",", ".")
                v = float(v)
                if v == int(v):
                    v = int(v)
                yr = data.get(y, {})
                yr[mun] = v
                data[y] = yr
        return data

    @JsonCache(file="dataset/empleo/paro_sepe_*.json")
    def create_sepe(self):
        firt_data = 8
        yrParo = {}
        for year, url in self.core.todas["paro_sepe"].items():
            year = int(year)
            if year >= cYear:
                continue
            paro = {}
            rows = get_csv(url, enconde="windows-1252", delimiter=";")
            head = rows[1][firt_data:]
            for i, v in enumerate(head):
                if v.lower() == "total paro registrado":
                    head[i] = "total"
                    continue
                if v.lower().startswith("paro "):
                    v = v[5:].strip()
                v = v.replace("25 -45", "25a45")
                v = v.replace("< 25", "<25")
                v = v.replace("edad ", "")
                head[i] = v
            for row in rows[2:]:
                if len(row) < firt_data:
                    continue
                mun = row[6]
                if mun is None or not isinstance(mun, int):
                    continue
                mun = str(mun)
                while len(mun) < 5:
                    mun = '0' + mun
                mes = get_mes(row[1])

                dMun = paro.get(mun, {})
                dt = dMun.get(mes, {})

                for i, v in enumerate(row[firt_data:]):
                    dt[head[i]] = v

                dMun[mes] = dt
                paro[mun] = dMun
            yrParo[year] = paro
        return yrParo

    @JsonCache(file="dataset/renta/aeat_*.json")
    def create_aeat(self):
        yrRenta = {}
        for year, url in self.core.todas["renta"]["aeat"].items():
            year = int(year)
            if year >= cYear:
                continue
            soup = get_bs(url)
            data = {}
            for tr in soup.select("table tr"):
                tds = tr.findAll(["th", "td"])
                if len(tds) == 8:
                    mun = tds[0].get_text().strip()
                    rent = tds[-2].get_text().strip()
                    decla = tds[2].get_text().strip()
                    rent = rent.replace(".", "")
                    decla = decla.replace(".", "")
                    cod = None
                    if not rent.isdigit():
                        continue
                    if "-" in mun:
                        pre, mun = mun.rsplit("-", 1)
                        if mun.isdigit():
                            if len(mun) == 5:
                                cod = mun
                            elif pre == "Agrupación municipios pequeños":
                                cod = "p"+mun
                    if cod is None:
                        cod = prov_to_cod(mun)
                    if cod is not None:
                        data[cod] = {
                            "media": int(rent),
                            "declaraciones": int(decla)
                        }
            yrRenta[year] = data
        return yrRenta

    @JsonCache(file="dataset/renta/navarra.json")
    def create_navarra(self):
        yrNavarra = {}
        for year, url in self.core.todas["renta"]["navarra"].items():
            year = int(year)
            if year >= cYear:
                continue
            book = get_xls(url)
            sheet = book.sheet_by_index(0)
            data = {}
            for r in range(sheet.nrows):
                row = [sheet.cell_value(r, c) for c in range(sheet.ncols)]
                c1 = row[0]
                if c1 and isinstance(c1, float) and int(c1) == c1:
                    c1 = int(c1)
                c2 = row[1]
                if c1 and isinstance(c1, int) and "(*)" in c2:
                    c1 = "31" + ("%03d" % c1)
                    data[c1] = row[3]
            yrNavarra[year] = data
        return yrNavarra

    @JsonCache(file="dataset/economia/agrario.json")
    def create_agrario(self):
        years = {}
        year = 1999
        for cod, data in self.core.items():
            censo = data.get("censo_%s" % year, None)
            if censo is not None:
                for i in get_js(censo["superficie"]):
                    mun, tenencia = i["MetaData"]
                    mun = get_cod_municipio(cod, mun)
                    if mun is not None and tenencia["Codigo"] == "todoslosregimenes":
                        valor = i["Data"][0]["Valor"]
                        if valor is not None:
                            dt = years.get(year, {})
                            yr = dt.get(mun, {})
                            yr["SAU"] = int(valor)
                            dt[mun] = yr
                            years[year] = dt
                for i in get_js(censo["unidades"]):
                    mun, tipo = i["MetaData"]
                    mun = get_cod_municipio(cod, mun)
                    if mun is None:
                        continue
                    c_tipo = tipo["Codigo"]
                    key = None
                    if c_tipo == "numerodeexplotacionestotal":
                        key = "explotaciones"
                    elif c_tipo == "unidadesganaderasug":
                        key = "unidadesganaderas"
                    elif c_tipo == "unidadesdetrabajoanouta":
                        key = "UTA"
                    if key:
                        valor = i["Data"][0]["Valor"]
                        if valor is not None:
                            dt = years.get(year, {})
                            yr = dt.get(mun, {})
                            yr[key] = int(valor)
                            dt[mun] = yr
                            years[year] = dt
        year = 2009
        with open(self.core.todas["censo_%s" % year], "r") as f:
            soup = BeautifulSoup(f, "lxml")

        for tr in soup.findAll("tr"):
            tds = tr.findAll(["th", "td"])
            if len(tds) == 20:
                cod = tds[0].get_text().strip().split()[0]
                if cod.isdigit() and len(cod) == 5:
                    datos = [parse_td(td) for td in tds[1:]]
                    dt = years.get(year, {})
                    yr = dt.get(cod, {})
                    yr["explotaciones"] = datos[0]
                    yr["SAU"] = datos[1]
                    yr["unidadesganaderas"] = datos[10]
                    yr["UTA"] = datos[11]
                    dt[cod] = yr
                    years[year] = dt
        return years

    @JsonCache(file="dataset/poblacion/edad_*.json")
    def create_edad(self):
        flag = False
        years = {}
        for cod, poblacion in self.getCore("poblacion5"):
            for year, url in sorted(poblacion.items()):
                year = int(year)
                if year >= cYear:
                    continue
                data = years.get(year, {})
                for i in get_js(url):
                    sex, mun, edad = i["MetaData"]
                    mun = get_cod_municipio(cod, mun)
                    if mun is None:
                        continue
                    valor = i["Data"][0]["Valor"]

                    c_sex = sex["Codigo"].strip()
                    c_edad = edad["Codigo"].strip()

                    if c_sex == "varones":
                        c_sex = "hombres"
                    elif c_sex == "ambossexos":
                        c_sex = ""

                    if c_edad == "59":
                        c_edad = "0509"
                    elif c_edad == "85ym s":
                        c_edad = "85ymas"
                    if c_edad == "04":
                        c_edad = "04ymenos"
                    elif len(c_edad) == 4:
                        c_edad = c_edad[0:2]+"a"+c_edad[2:4]

                    dt = data.get(mun, {})

                    key = (c_sex+" "+c_edad).strip()

                    dt[key] = int(valor) if valor is not None else None

                    data[mun] = dt
                years[year] = data
        return years

    @JsonCache(file="dataset/poblacion/sexo.json")
    def create_poblacion(self):
        years = {}
        for cod, pob in self.getCore("poblacion"):
            for i in get_js(pob):
                mun, sex, _, _ = i["MetaData"]
                mun = get_cod_municipio(None, mun)
                if mun is not None:
                    key = None
                    sex = sex["Nombre"]
                    if sex in ("Total", "ambossexos total"):
                        key = "total"
                    elif sex == "Mujeres":
                        key = "mujeres"
                    elif sex == "Hombres":
                        key = "hombres"
                    if not key:
                        continue
                    for d in i["Data"]:
                        year = d["Anyo"]
                        valor = d["Valor"]

                        year = int(year)
                        if year >= cYear:
                            continue

                        yDt = years.get(year, {})
                        mDt = yDt.get(mun, {})

                        if mDt.get(key) is None:
                            mDt[key] = int(
                                valor) if valor is not None else None
                            yDt[mun] = mDt
                            years[year] = yDt
        return years

    @JsonCache(file="dataset/economia/empresas.json")
    def create_empresas(self):
        empresas = get_csv(self.core.todas["empresas"])
        col_empresas = [
            r if r != "Total" else "Total empresas" for r in empresas[4] if r]
        years = {}
        for record in empresas:
            if len(record) < 2 or record[0] is None:
                continue
            mun = record[0].split()[0]
            if not mun.isdigit() or len(mun) != 5:
                continue
            for j, col in enumerate(col_empresas):
                for i, year in enumerate(range(2018, 2011, -1)):
                    index = (j*7)+i+1
                    e = record[index]
                    if e is not None:
                        dtY = years.get(year, {})
                        dt = dtY.get(mun, {})
                        dt[col] = e
                        dtY[mun] = dt
                        years[year] = dtY
        return years

    @KmCache(file="dataset/geografia/distancias.txt")
    def create_distancias(self):
        dist = {}
        for dShapes in (self.provincias, self.municipios):
            items = list(sorted([(k, v[0]) for k, v in dShapes.items()]))
            while items:
                a, aShp = items.pop(0)
                for b, bShp in items:
                    km = aShp.distance(bShp)
                    dist[(a, b)] = km
        return dist

    @JsonCache(file="dataset/aemet/bases.json", intKey=False, avoidReload=True)
    def create_aemet_bases(self):
        bases = get_js(self.fuentes.aemet.estaciones)
        for b in bases:
            b["latitud"] = sexa_to_dec(b["latitud"])
            b["longitud"] = sexa_to_dec(b["longitud"])
            b["provincia"] = prov_to_cod(b["provincia"])
            b["altitud"] = to_num(b.get("altitud"))
        return bases

    @ParamJsonCache(file="dataset/aemet/diarios/{}.json", intKey=False, avoidReload=True)
    def get_dia_estacion(self, id):
        del_key = ("nombre", "provincia", "indicativo", "altitud")
        items = []
        y = 1972 - 1
        while y < cYear:
            y = y + 1
            fin = min(y+4, cYear)
            a = date(y, 1, 1)
            b = date(fin, 12, 31)
            bi = (a-b).days % 365
            if bi > 1:
                fin = fin - 1
            url = self.fuentes.aemet.estacion.diario.format(
                id=id, ini=y, fin=fin)
            data = get_js(url)
            if isinstance(data, list):
                y = fin
                for d in data:
                    o = {}
                    for k, v in d.items():
                        if k not in del_key:
                            o[k] = to_num(v, coma=True)
                    l_keys = len(o.keys())
                    if l_keys == 0 or (l_keys == 1 and "fecha" in o):
                        continue
                    items.append(o)
        return items

    @ParamJsonCache(file="dataset/aemet/mensual/{}.json", intKey=False, avoidReload=True)
    def get_mes_estacion(self, id):
        del_key = ("nombre", "provincia", "indicativo", "altitud")
        items = []
        for y in range(1972, cYear):
            url = self.fuentes.aemet.estacion.mensual.format(id=id, ini=y)
            data = get_js(url)
            if isinstance(data, list):
                for d in data:
                    o = {}
                    for k, v in d.items():
                        if k not in del_key:
                            o[k] = to_num(v)
                    l_keys = len(o.keys())
                    if l_keys == 0 or (l_keys == 1 and "fecha" in o):
                        continue
                    items.append(o)
        return items

    @property
    @lru_cache(maxsize=None)
    def aemet_bases(self):
        bases = self.create_aemet_bases()
        return bases

    @lru_cache(maxsize=None)
    def distancias(self, lcod):
        distancias = self.create_distancias()
        for key, km in list(distancias.items()):
            b, a = key
            if len(a) != lcod:
                del distancias[key]
                continue
            distancias[(a, b)] = km
            distancias[(a, a)] = 0
            distancias[(b, b)] = 0
        return distancias

    @property
    @lru_cache(maxsize=None)
    def provincias(self):
        return getShp("fuentes/fomento/shp/**/recintos*provinciales*.shp", 4, 6)

    @property
    @lru_cache(maxsize=None)
    def municipios(self):
        return getShp("fuentes/fomento/shp/**/recintos*municipales*.shp", 6, 11)

    @lru_cache(maxsize=None)
    def get_dataset(self, create):
        crt = getattr(self, create)
        data = crt()
        data = self.parseData(data)
        return data

    @property
    @lru_cache(maxsize=None)
    def years_poblacion(self):
        pob = self.create_poblacion()
        return sorted(pob.keys())

    @property
    @lru_cache(maxsize=None)
    def edades(self):
        edad = self.create_edades()
        for year, yData in edad.items():
            for mun, mData in list(yData.items()):
                mData = {i: int(s)
                         for i, s in enumerate(mData.split(",")) if s}
                yData[mun] = mData
        return edad

    @property
    @lru_cache(maxsize=None)
    def meta_edades(self):
        meta = {}
        for year, yData in self.edades.items():
            meta[year] = {}
            for mun, mData in yData.items():
                e18ymas = 0
                e16ymas = 0
                e16a65 = 0
                for i in range(16, max(mData.keys())):
                    e = mData.get(i, 0)
                    if e is None or e == 0:
                        continue
                    e16ymas = e16ymas + e
                    if i >= 18:
                        e18ymas = e18ymas + e
                    if i <= 56:
                        e16a65 = e16a65 + e
                meta[year][mun] = {
                    "18ymas": e18ymas,
                    "16ymas": e16ymas,
                    "16a65": e16a65
                }
        grupos = self.create_edad()
        for year, yData in grupos.items():
            for mun, mData in yData.items():
                if year in meta and mun in meta[year]:
                    continue
                if year not in meta:
                    meta[year] = {}
                e18ymas = 0
                e16ymas = 0
                e16a65 = 0
                for k, e in mData.items():
                    if e is None or e == 0:
                        continue
                    i = None
                    if k[:3].isdigit():
                        i = int(k[:3])
                    elif k[:2].isdigit():
                        i = int(k[:2])
                    if i is None:
                        continue
                    if i >= 15:
                        e16ymas = e16ymas + 5
                        if i < 56:
                            e16a65 = e16a65 + e
                    if i >= 18:
                        e18ymas = e18ymas + e
                meta[year][mun] = {
                    "18ymas": e18ymas,
                    "16ymas": e16ymas,
                    "16a65": e16a65
                }
        return meta

    @property
    @lru_cache(maxsize=None)
    def paro(self):
        self.create_sepe()
        paro = read_js("dataset/empleo/paro_sepe_*.json")
        for year, dt in paro.items():
            for nuevo, viejos in self.mun_remplaza.items():
                dNuevo = dt.get(nuevo, {})
                dt[nuevo] = dNuevo
                for viejo in viejos:
                    if viejo in dt:
                        dViejo = dt[viejo]
                        del dt[viejo]
                        for mes, mDt in dViejo.items():
                            if mes not in dNuevo:
                                dNuevo[mes] = mDt
                                continue
                            for k, v in mDt.items():
                                dNuevo[mes][k] = dNuevo[mes].get(k, 0) + v
            if year not in self.poblacion:
                continue
            pob = self.poblacion[year]
            for viejo, nuevos in self.mun_desgaja.items():
                cNuevos = set()
                for nuevo in nuevos:
                    if nuevo not in pob and nuevo in dt:
                        cNuevos.add(nuevo)
                if len(cNuevos) == 0:
                    continue
                print("paro", year, viejo, cNuevos)
                dViejo = dt.get(viejo, {})
                dt[viejo] = dViejo
                for nuevo in cNuevos:
                    dNuevo = dt[nuevo]
                    del dt[nuevo]
                    for mes, mData in dNuevo.items():
                        if mes not in dViejo:
                            dViejo[mes] = mData
                        else:
                            for k, v in mData.items():
                                dViejo[mes][k] = dViejo[mes].get(
                                    k, 0) + mData[k]
        for year, dt in paro.items():
            if year not in self.years_poblacion:
                for mun, dMun in list(dt.items()):
                    todoCeros = True
                    for mes, dMes in dMun.items():
                        for v in dMes.values():
                            if v != 0:
                                todoCeros = False
                    if todoCeros:
                        del dt[mun]
        return paro

    @property
    @lru_cache(maxsize=None)
    def renta_euskadi(self):
        renta = self.create_euskadi()
        for nuevo, viejos in self.mun_remplaza.items():
            for year, data in renta.items():
                my = self.meta_edades.get(year, None)
                if not my:
                    continue
                dNuevo = data.get(nuevo, None)
                for viejo in viejos:
                    if viejo in data:
                        dViejo = data[viejo]
                        del data[viejo]
                        if dNuevo is None:
                            dNuevo = dViejo
                            continue
                        total = dNuevo*my[nuevo] + dViejo*my[viejo]["18ymas"]
                        dNuevo = total / \
                            (my[nuevo]["18ymas"]+my[viejo]["18ymas"])
                if dNuevo:
                    data[nuevo] = dNuevo
        for viejo, nuevos in self.mun_desgaja.items():
            for year, data in renta.items():
                my = self.meta_edades.get(year, None)
                if not my:
                    continue
                cNuevos = set(n for n in nuevos if n not in my and n in data)
                if len(cNuevos) == 0:
                    continue
                print("euskadi", year, viejo, cNuevos)
                dViejo = data[viejo]
                for n in cNuevos:
                    dNuevo = data[n]
                    del data[n]
                    if dNuevo == dViejo:
                        continue
                    total = dNuevo*my[nuevo]["18ymas"] + \
                        dViejo*my[viejo]["18ymas"]
                    dViejo = total/(my[nuevo]["18ymas"]+my[viejo]["18ymas"])
                    data[viejo] = dViejo
        return renta

    @property
    @lru_cache(maxsize=None)
    def renta_aeat(self):
        renta = self.create_aeat()
        for nuevo, viejos in self.mun_remplaza.items():
            for year, data in renta.items():
                dNuevo = data.get(nuevo, None)
                for viejo in viejos:
                    if viejo in data:
                        dViejo = data[viejo]
                        del data[viejo]
                        if dNuevo is None:
                            dNuevo = dViejo
                            continue
                        total = dNuevo["media"]*dNuevo["declaraciones"] + \
                            dViejo["media"]*dViejo["declaraciones"]
                        dNuevo["declaraciones"] = dNuevo["declaraciones"] + \
                            dViejo["declaraciones"]
                        dNuevo["media"] = total/dNuevo["declaraciones"]
                if dNuevo:
                    data[nuevo] = dNuevo
        for viejo, nuevos in self.mun_desgaja.items():
            for year, data in renta.items():
                if year not in self.years_poblacion:
                    continue
                pob = self.poblacion[year]
                cNuevos = set(n for n in nuevos if n not in pob and n in data)
                if len(cNuevos) == 0:
                    continue
                print("renta", year, viejo, cNuevos)
                dViejo = data[viejo]
                for n in cNuevos:
                    dNuevo = data[n]
                    del data[n]
                    total = dNuevo["media"]*dNuevo["declaraciones"] + \
                        dViejo["media"]*dViejo["declaraciones"]
                    dViejo["declaraciones"] = dNuevo["declaraciones"] + \
                        dViejo["declaraciones"]
                    dViejo["media"] = total/dViejo["declaraciones"]
        for year, dt in renta.items():
            if year not in self.years_poblacion:
                continue
            pob = self.poblacion[year]
            for mun, p in pob.items():
                if p.get("total", 0) == 0 and mun not in dt:
                    dt[mun] = {"media": 0, "declaraciones": 0}
            visto = [k for k in dt.keys() if len(k) == 5]
            provs = set(k[1:]
                        for k in dt.keys() if len(k) == 3 and k[0] == "p")
            for prov in provs:
                mun = set(m for m in pob.keys() if m.startswith(
                    prov) and m not in visto)
                if len(mun) == 1:
                    mun = mun.pop()
                    dt[mun] = dt["p"+prov]
                    del dt["p"+prov]
        return renta

    @property
    @lru_cache(maxsize=None)
    def mun_remplaza(self):
        data = {}
        for c in self.cambios:
            if c.remplaza:
                nuevo = c.nuevo.split()[0]
                st = data.get(nuevo, set())
                st.add(c.cod)
                data[nuevo] = st
        return data

    @property
    @lru_cache(maxsize=None)
    def mun_desgaja(self):
        data = {}
        for c in self.cambios:
            if not c.remplaza and "nuevo" in c:
                nuevo = c.nuevo.split()[0]
                st = data.get(c.cod, set())
                st.add(nuevo)
                data[c.cod] = st
        return data

    def populate_datamun(self, db, reload=False):
        aemet_mes = {}
        aemet_dia = {}
        for b in self.aemet_bases:
            b["point"] = Point(b["longitud"], b["latitud"])
            b["ID"] = b["indicativo"]
            db.insert("AEMET_BASES", **b)
            aemet_dia[b["indicativo"]] = self.get_dia_estacion(b["indicativo"])
            aemet_mes[b["indicativo"]] = self.get_mes_estacion(b["indicativo"])

        table = "AEMET_DIA"
        kcols = get_cols(aemet_dia)
        kcols["prec"] = "REAL"
        del kcols["fecha"]
        create = '''
            create table {} (
              BASE TEXT,
              FECHA DATE,
              %s
              PRIMARY KEY (BASE, FECHA),
              FOREIGN KEY(BASE) REFERENCES AEMET_BASES(ID)
            )
        '''.format(table)
        db.create(create, **kcols, to_file="sql/%s.sql" % table)

        db.openTransaction()
        for id, vals in aemet_dia.items():
            for b in vals:
                if b.get("prec") == "Ip":
                    b["prec"] = 0.09
                db.insert(table, BASE=id, **b)
        db.closeTransaction()
        del aemet_dia

        table = "AEMET_MES"
        kcols = get_cols(aemet_mes)
        del kcols["fecha"]
        create = '''
            create table {} (
              BASE TEXT,
              FECHA TEXT,
              %s
              PRIMARY KEY (BASE, FECHA),
              FOREIGN KEY(BASE) REFERENCES AEMET_BASES(ID)
            )
        '''.format(table)
        db.create(create, **kcols, to_file="sql/%s.sql" % table)

        db.openTransaction()
        for id, vals in aemet_mes.items():
            for b in vals:
                b["fecha"] = "%d-%02d" % tuple(int(i)
                                               for i in b["fecha"].split("-"))
                db.insert(table, BASE=id, **b)
        db.closeTransaction()
        del aemet_mes

        pop_rows = {}
        for year, dtY in self.parseData(self.meta_edades).items():
            for mun, dt in dtY.items():
                key = (mun, year)
                row = pop_rows.get(key, {})
                for k, v in dt.items():
                    row[k] = v
                pop_rows[key] = row

        for year, dtY in self.edad.items():
            for mun, dt in dtY.items():
                key = (mun, year)
                row = pop_rows.get(key, {})
                for k, v in dt.items():
                    row[k] = v
                pop_rows[key] = row

        for year, dtY in self.poblacion.items():
            for mun, dt in dtY.items():
                key = (mun, year)
                row = pop_rows.get(key, {})
                for k, v in dt.items():
                    if k in ("mujeres", "hombres"):
                        k = k+" total"
                    if k not in row:
                        row[k] = v
                pop_rows[key] = row

        insert_rel_mun(db, "poblacion", pop_rows, kSort=sortColPob)

        rows = {}
        for year, dtY in self.agrario.items():
            for mun, dt in dtY.items():
                key = (mun, year)
                row = rows.get(key, {})
                for k, v in dt.items():
                    row[k] = v
                rows[key] = row

        insert_rel_mun(db, "agrario", rows)

        rows = {}
        for year, dtY in self.empresas.items():
            for mun, dt in dtY.items():
                key = (mun, year)
                row = rows.get(key, {})
                for k, v in dt.items():
                    row[k] = v
                rows[key] = row

        insert_rel_mun(db, "empresas", rows)

        rows = {}
        rt1000 = {}
        visto = set()
        for year, data in self.renta_aeat.items():
            for mun, rent in data.items():
                if len(mun) == 3 and mun[0] == "p":
                    rt1000[(year, mun[1:])] = rent
                    continue
                if len(mun) != 5:
                    continue
                visto.add(mun)
                key = (mun, year)
                row = rows.get(key, {})
                row["renta"] = rent["media"]
                row["declaraciones"] = rent["declaraciones"]
                row["tipo"] = 1
                rows[key] = row

        keys_pob = [k for k in pop_rows.keys() if k[0] not in visto]
        for key, rent in rt1000.items():
            year, prov = key
            muns = set(k[0] for k in keys_pob if k[1] ==
                       year and k[0].startswith(prov))
            for mun in muns:
                key = (mun, year)
                row = rows.get(key, {})
                row["renta"] = rent["media"]
                row["declaraciones"] = rent["declaraciones"]
                row["tipo"] = 1 if len(muns) == 1 else 2
                rows[key] = row

        for year, dt in self.renta_euskadi.items():
            for mun, rent in dt.items():
                key = (mun, year)
                row = rows.get(key, {})
                row["renta"] = rent
                row["tipo"] = 3
                rows[key] = row

        for year, dt in self.renta_navarra.items():
            for mun, rent in dt.items():
                key = (mun, year)
                row = rows.get(key, {})
                row["renta"] = rent
                row["tipo"] = 4
                rows[key] = row

        insert_rel_mun(db, "renta", rows)

        db.create('''
            create table SEPE (
              MUN TEXT,
              YR INTEGER,
              MES INTEGER,
              %s
              PRIMARY KEY (MUN, YR, MES),
              FOREIGN KEY(MUN) REFERENCES municipios(ID)
            )
        ''', **get_cols(self.paro))

        db.openTransaction()
        for year, dMun in self.paro.items():
            for mun, dMes in dMun.items():
                for mes, sepe in dMes.items():
                    sepe["MUN"] = mun
                    sepe["YR"] = year
                    sepe["MES"] = mes
                    db.insert("sepe", **sepe)
        db.closeTransaction()

        fields = list(db.tables["SEPE"])
        fields.remove("YR")
        fields.remove("MUN")
        fields.remove("MES")
        view = '''
            CREATE VIEW SEPE_YEAR AS
            select
            	s1.MUN, s1.YR'''
        for f in fields:
            view = view+', "{0}"/C "{0}"'.format(f)
        view = view+'''
            from
            (
            select MUN, YR'''
        for f in fields:
            view = view+', sum("{0}")*1.0 "{0}"'.format(f)
        view = view+'''
            from sepe group by MUN, YR
            ) s1
            join
            (select MUN, YR, count(*) C from sepe group by MUN, YR) s2
            on s2.MUN = s1.MUN and s1.YR = s2.YR
        '''
        db.execute(view, to_file="sql/sepe_year.sql")
        db.execute("sql/renta_transformada.sql")

    def collect(self):
        re_trim = re.compile(
            r"\s*(:|-)?\s*(Datos municipales|Población por municipios y sexo|Población por sexo, municipios y edad \(?grupos quinquenales\)?|Población por sexo, municipio y grupo quinquenal de edad)\.?\s*$")
        self.core = get_provincias()
        self.core.todas = Bunch()
        soup = get_bs(self.fuentes.ine.poblacion.sexo)
        years = set()
        data = []
        for i in soup.select("ol.ListadoTablas li"):
            _, _id = i.attrs["id"].split("_")
            url = "http://servicios.ine.es/wstempus/js/es/DATOS_TABLA/%s?tip=AM" % _id
            js = get_js(url)
            meta = js[0]["MetaData"][0]
            self.core[meta["Codigo"]].poblacion = url
            self.core[meta["Codigo"]].poblacion5 = {}
            self.core[meta["Codigo"]].poblacion1 = {}
            data.append((meta["Codigo"], meta["Nombre"], url))

        data = {}
        for y in self.years_poblacion:
            url = self.fuentes.ine.poblacion.edad_year + str(y)
            soup = get_bs(url)
            for s in soup.select(".ocultar"):
                s.extract()
            for i in soup.select("#listadoInebase > ol.ListadoTablas > li"):
                txt = i.find("a").get_text().strip()
                if txt == "00.- Nacional":
                    continue
                txt = re_trim.sub("", txt)
                txt = txt.replace(".- ", " ")
                c, n = txt.split(" ", 1)
                a = i.find("ol").find("li").findAll("a")[-1]
                url = wstempus(a.attrs["href"])

                dt = data.get(c, {})
                dt[y] = {
                    "Nombre": normalizarProvincia(n),
                    "url": url
                }
                data[c] = dt
                self.core[c].poblacion5[y] = url

                a = i.find("ol").find(
                    "a", text="Población por sexo, municipios y edad (año a año).")
                if a:
                    url = wstempus(a.attrs["href"])
                    self.core[c].poblacion1[y] = url

        self.core.todas.empresas = self.fuentes.ine.empresas.csv
        self.core.todas.censo_2009 = self.fuentes.ine.agrario.year[2009].local

        data = {}
        soup = get_bs(self.fuentes.ine.agrario.year[1999])
        select = soup.select("select")[-1]
        for option in select.findAll("option"):
            if "value" in option.attrs:
                url = "http://www.ine.es"+option.attrs["value"]
                sp = get_bs(url)
                for s in sp.select(".ocultar"):
                    s.extract()
                for li in sp.findAll("li"):
                    if re.search(r"^\d+\.-\s+Resultados municipales", li.get_text().strip()):
                        a1 = li.find(
                            "a", text="Superficie agrícola utilizada de las explotaciones según regimen de tenencia (Ha.)")
                        a2 = li.find(
                            "a", text="Explotaciones, parcelas, unidades ganaderas (UG) y unidades trabajo-año (UTA)")
                        a1 = a1.attrs["href"]
                        a2 = a2.attrs["href"]
                        _a1 = wstempus(a1)  # if a1 else None
                        _a2 = wstempus(a2)  # if a2 else None
                        js = get_js(_a1)
                        js = get_js(_a2)
                        cod = js[0]["MetaData"][0]["Codigo"][0:2]
                        data[cod] = (url, _a1, _a2, a1, a2)
        for cod, dt in sorted(data.items()):
            url, _a1, _a2, a1, a2 = dt
            self.core[cod].censo_1999 = {
                "superficie": _a1,
                "unidades": _a2
            }

        self.core.todas.nomenclator_basico = self.fuentes.fomento.nomenclator.basico
        self.core.todas.nomenclator = self.fuentes.fomento.nomenclator.municipios
        self.core.todas.limites = self.fuentes.fomento.mapa

        self.core.todas.paro_sepe = {}
        js = get_js(self.fuentes.sepe.json)
        data = {}
        for i in js["result"]["items"][0]["distribution"]:
            url = i["accessURL"]
            if url.endswith(".csv"):
                _, year, _ = url.rsplit("_", 2)
                year = int(year)
                data[year] = url
                self.core.todas.paro_sepe[year] = url

        self.core.todas.renta = {}
        soup = get_bs(self.fuentes.renta.aeat.root)
        for n in soup.select("div.contenido li strong"):
            year = n.get_text().split()[-1]
            year = int(year)
            li = n.find_parent("li")
            a = li.find("a").attrs["href"]
            sp = get_bs(a)
            a = sp.find(
                "a", text="Detalle de los municipios con más de 1.000 habitantes")
            a = a.attrs["href"]
            sp = get_bs(a)
            a = sp.find(
                "a", text="Posicionamiento de los municipios mayores de 1.000 habitantes por Renta bruta media")
            a = a.attrs["href"]
            aeat = self.core.todas.renta.get("aeat", {})
            aeat[year] = a
            self.core.todas.renta["aeat"] = aeat

        self.core.todas.renta["euskadi"] = self.fuentes.renta.euskadi.csv

        self.core.todas.renta["navarra"] = {}
        soup = get_bs(self.fuentes.renta.navarra)
        for li in soup.select("#cuerpo li > ul > li"):
            y = li.get_text().strip()[:4]
            if y.isdigit():
                y = int(y)
                a = li.find("a", attrs={"title": "Municipios"})
                if a:
                    self.core.todas.renta["navarra"][y] = a.attrs["href"]

        provs = []
        soup = get_bs(self.fuentes.miteco.root)
        for a in soup.select("map area"):
            url = a.attrs.get("href", None)
            if url and url.startswith("http"):
                s = get_bs(url)
                for tr in s.select("div.panel tr"):
                    tds = tr.findAll("td")
                    if len(tds) == 2:
                        a = tds[1].find("a")
                        if a:
                            p = tds[0].get_text().strip()
                            z = a.attrs["href"]
                            cod = a.get_text().strip().split("_")[
                                1].split(".")[0]
                            provs.append((cod.strip(), p, z))
        for cod, p, z in sorted(provs):
            nombre = self.core[cod].nombre
            self.core[cod].miteco = z

        save_js(self.file, self.core)


for name in dir(Dataset):
    func = getattr(Dataset, name)
    if callable(func) and name.startswith("create_"):
        id = name.split("_")[-1]
        if id in ("euskadi", "navarra"):
            id = "renta_"+id
        if not hasattr(Dataset, id):
            f = eval("lambda slf: slf.get_dataset('%s')" % name)
            setattr(Dataset, id, property(f))


if __name__ == "__main__":
    d = Dataset(reload=["dataset/poblacion/edades.json"])
    d.create_edades()
