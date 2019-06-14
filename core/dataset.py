import json
import os
import re
from functools import lru_cache

import requests
import urllib3
import xlrd
import yaml
from bs4 import BeautifulSoup
from bunch import Bunch

try:
    from .common import *
except:
    from common import *

try:
    from .provincias import *
except:
    from provincias import *

me = os.path.realpath(__file__)
dr = os.path.dirname(me)


class Dataset():
    def __init__(self, *args, core=None, **kargs):
        self.file = "fuentes/fuentes.json"
        self.fuentes = mkBunch("fuentes/indice.yml")
        self.cambios = list(sorted(get_yml("fuentes/cambios_municipios.yml",
                                           remplaza=False, municipio=True, notas=""), key=lambda x: x.cod))
        self.core = core
        if self.core is None:
            self.core = mkBunch(self.file)
        if not self.core:
            self.collect()

    def create_mayores(self, reload=False):
        file = "dataset/poblacion/mayores.json"
        if not reload and os.path.isfile(file):
            return False
        mayores = {}
        for prov, dt in self.core.items():
            pob = dt.get("poblacion1", None)
            if pob is None:
                continue
            for year, url in pob.items():
                year = int(year)
                js = get_js(url)
                if not js:
                    continue
                aux_mayores = {}
                for d in js:
                    if d["MetaData"][0]["Codigo"] != "ambossexos":
                        continue
                    mun = get_cod_municipio(prov, d["MetaData"][1])
                    if mun is None:
                        continue
                    total, menores = aux_mayores.get(mun, (0, 0))
                    edad = d["MetaData"][2]["Codigo"]
                    if edad == "total":
                        total = int(d["Data"][0]["Valor"])
                    elif edad.isdigit() and int(edad) < 18:
                        menores = menores + int(d["Data"][0]["Valor"])
                    aux_mayores[mun] = (total, menores)
                if aux_mayores:
                    ydata = mayores.get(year, {})
                    for mun, dt in aux_mayores.items():
                        total, menores = dt
                        ydata[mun] = {
                            "mayores": total - menores,
                            "total": total
                        }
                    mayores[year] = ydata
        self.save(file, mayores)
        return True

    def create_euskadi(self, reload=False):
        file = "dataset/renta/euskadi.json"
        if not reload and os.path.isfile(file):
            return False
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
                v = r[i]
                v = v.replace(".", "")
                v = v.replace(",", ".")
                v = float(v)
                if v == int(v):
                    v = int(v)
                yr = data.get(y, {})
                yr[mun] = v
                data[y] = yr
        self.save(file, data)
        return True

    def create_aeat(self, reload=False):
        flag = False
        for year, url in self.core.todas["renta"]["aeat"].items():
            year = int(year)
            file = "dataset/renta/aeat_%s.json" % year
            if not reload and os.path.isfile(file):
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
            self.save(file, data)
            flag = True
        return flag

    def create(self, reload=False):
        self.create_mayores(reload=reload)
        self.create_aeat(reload=reload)
        self.create_euskadi(reload=reload)

    def unzip(self):
        unzip("fuentes/fomento/shp", self.core.todas["limites"])
        unzip("fuentes/fomento/mdb",
              self.core.todas["nomenclator"], self.core.todas["nomenclator_basico"])
        for pro, dt in self.core.items():
            if "miteco" in dt:
                unzip("fuentes/miteco/mapaforestal/%s %s" %
                      (pro, dt["nombre"]), dt["miteco"])

    def save(self, file=None, data=None):
        if data is None:
            data = self.core
        if file is None:
            file = self.file
        if file and data:
            with open(file, "w") as f:
                json.dump(data, f, indent=4)

    def get_paro(self, year, cod, avoid=None):
        if cod not in self.paro or year not in self.paro[cod]:
            return None
        paro = 0
        vals = self.paro[cod][year].values()
        for m in vals:
            paro = paro + m["total Paro Registrado"]
        return paro/len(vals)

    @property
    @lru_cache(maxsize=None)
    def renta_aeat(self):
        self.create_aeat()
        renta = read_js_glob("dataset/renta/aeat_*.json")
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
                data[nuevo] = dNuevo
        for viejo, nuevos in self.mun_desgaja.items():
            for year, data in renta.items():
                if year not in self.mayores:
                    continue
                my = self.mayores[year]
                cNuevos = set(n for n in nuevos if n not in my and n in data)
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
            if year not in self.mayores:
                continue
            my = self.mayores[year]
            for mun, p in my.items():
                if p["total"] == 0 and mun not in dt:
                    dt[mun] = {"media": 0, "declaraciones": 0}
            visto = [k for k in dt.keys() if len(k) == 5]
            provs = set(k[1:]
                        for k in dt.keys() if len(k) == 3 and k[0] == "p")
            for prov in provs:
                mun = set(m for m in my.keys() if m.startswith(
                    prov) and m not in visto)
                if len(mun) == 1:
                    mun = mun.pop()
                    dt[mun] = dt["p"+prov]
                    del dt["p"+prov]
        for year, dt in renta.items():
            for mun, rt in dt.items():
                for y in (year, year-1, year+1):
                    if y in self.mayores and mun in self.mayores[y]:
                        rt["mayores"] = self.mayores[y][mun]["mayores"]
                        break
        return renta

    def create_navarra(self, reload=False):
        flag = False
        for year, url in self.core.todas["renta"]["navarra"].items():
            year = int(year)
            file = "dataset/renta/navarra_%s.json" % year
            if not reload and os.path.isfile(file):
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
            self.save(file, data)
            flag = True
        return flag

    @property
    @lru_cache(maxsize=None)
    def renta_euskadi(self):
        self.create_euskadi()
        return read_js("dataset/renta/euskadi.json", intKey=True)

    @property
    @lru_cache(maxsize=None)
    def renta_navarra(self):
        self.create_navarra()
        return read_js_glob("dataset/renta/navarra_*.json")

    @property
    @lru_cache(maxsize=None)
    def years_poblacion(self):
        years = set()
        for dt in self.core.values():
            if "poblacion" not in dt:
                continue
            js = get_js(dt["poblacion"])
            for d in js:
                for y in d["Data"]:
                    years.add(int(y["Anyo"]))
        return sorted(years)

    @property
    @lru_cache(maxsize=None)
    def empresas(self):
        return get_csv(self.core.todas["empresas"])

    @property
    @lru_cache(maxsize=None)
    def paro(self):
        firt_data = 8
        paro = {}
        for year, url in self.core.todas["paro_sepe"].items():
            year = int(year)
            if year < cYear:
                rows = get_csv(url, enconde="windows-1252", delimiter=";")
                head = rows[1][firt_data:]
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
                    dYear = dMun.get(year, {})
                    dMes = dYear.get(mes, {})

                    for i, v in enumerate(row[firt_data:]):
                        dMes[head[i]] = v

                    dYear[mes] = dMes
                    dMun[year] = dYear
                    paro[mun] = dMun
        for nuevo, viejos in self.mun_remplaza.items():
            dNuevo = paro.get(nuevo, {})
            paro[nuevo] = dNuevo
            for viejo in viejos:
                if viejo in paro:
                    dViejo = paro[viejo]
                    del paro[viejo]
                    for year in sorted(dViejo.keys()):
                        if year not in dNuevo:
                            dNuevo[year] = dViejo[year]
                        else:
                            yViejo = dViejo[year]
                            yNuevo = dNuevo[year]
                            for mes in sorted(yViejo.keys()):
                                if mes not in yNuevo:
                                    yNuevo[mes] = yViejo[mes]
                                else:
                                    mViejo = yViejo[mes]
                                    mNuevo = yNuevo[mes]
                                    for k, v in mViejo.items():
                                        mNuevo[k] = mNuevo.get(
                                            k, 0) + mViejo[k]

        for viejo, nuevos in self.mun_desgaja.items():
            if viejo not in paro:
                continue
            for year, yData in sorted(paro[viejo].items()):
                if year not in self.mayores:
                    continue
                my = self.mayores[year]
                cNuevos = set()
                for nuevo in nuevos:
                    if nuevo not in my and nuevo in paro and year in paro[nuevo]:
                        cNuevos.add(nuevo)
                if len(cNuevos) == 0:
                    continue
                print("paro", year, viejo, cNuevos)
                for nuevo in cNuevos:
                    dNuevo = paro[nuevo][year]
                    for mes, mData in dNuevo.items():
                        if mes not in yData:
                            yData[mes] = mData
                        else:
                            for k, v in mData.items():
                                yData[mes][k] = yData[mes].get(k, 0) + mData[k]
        return paro

    @property
    @lru_cache(maxsize=None)
    def mayores(self):
        self.create_mayores()
        mayores = read_js("dataset/poblacion/mayores.json",
                          intKey=True, maxKey=cYear)
        for nuevo, viejos in self.mun_remplaza.items():
            for year, dt in mayores.items():
                dNuevo = dt.get(nuevo, {})
                dt[nuevo] = dNuevo
                for viejo in viejos:
                    if viejo in dt:
                        dViejo = dt[viejo]
                        del dt[viejo]
                        for k, v in dViejo.items():
                            dNuevo[k] = dNuevo.get(k, 0) + v
        return mayores

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

    @property
    @lru_cache(maxsize=None)
    def renta_menos1000(self):
        self.create_renta_menos1000()
        return read_js("dataset/renta/aeat_menos_mil.json", intKey=True, maxKey=cYear)

    def create_renta_menos1000(self, reload=False):
        file = "dataset/renta/aeat_menos_mil.json"
        if not reload and os.path.isfile(file):
            return False
        rData = {}
        for year, data in sorted(self.renta_aeat.items()):
            if year not in self.mayores:
                continue
            yData = {}
            rData[year] = yData
            my = self.mayores[year]
            visto = set(k for k in data.keys() if len(k) == 5)
            provs = set(k[1:]
                        for k in data.keys() if len(k) == 3 and k[0] == "p")
            for prov in sorted(provs):
                falta = set()
                tengo = set()
                paro = {}
                for k in my.keys():
                    if k.startswith(prov):
                        if k in visto:
                            tengo.add(k)
                        p = self.get_paro(year, k)
                        if p is not None:
                            paro[k] = p
                            if k not in visto:
                                falta.add(k)
                if len(falta) in (0, 1):
                    print(len(falta), "commorl?")
                    continue
                yData[prov] = data[prov]
                yData[prov]["mayores"] = sum(v["mayores"]
                                             for k, v in my.items() if k in tengo)
                yData[prov]["renta"] = yData[prov]["media"] * \
                    yData[prov]["declaraciones"]
                yData[prov]["paro"] = int(sum(paro.values()))
                yData[prov]["ocupados"] = yData[prov]["mayores"] - \
                    yData[prov]["paro"]
                yData[prov]["renta_m18"] = yData[prov]["renta"] / \
                    yData[prov]["mayores"]
                p = data["p"+prov]
                p["mayores"] = sum(v["mayores"]
                                   for k, v in my.items() if k in falta)
                p["paro"] = int(sum(v for k, v in paro.items() if k in falta))
                p["renta"] = p["media"]*p["declaraciones"]
                p["ocupados"] = p["mayores"] - p["paro"]
                p["renta_m18"] = p["renta"]/p["mayores"]
                yData["p"+prov] = p
                for f in sorted(falta):
                    yData[f] = {
                        "paro": paro[f],
                        "mayores": my[f]["mayores"],
                    }
                    yData[f]["ocupados"] = yData[f]["mayores"] - \
                        yData[f]["paro"]
                    yData[f]["renta"] = yData["p"+prov]["renta"] * \
                        yData[f]["ocupados"] / yData["p"+prov]["ocupados"]
                    yData[f]["renta_m18"] = yData[f]["renta"] / \
                        yData[f]["mayores"]
        self.save(file, rData)
        return True

    @property
    @lru_cache(maxsize=None)
    def mun_super(self):
        return sqlite_to_dict("fuentes/fomento/mdb/Nomenclator_Municipios_EntidadesDePoblacion.sqlite", '''
            select substr(COD_INE, 1,5), SUPERFICIE/100 from MUNICIPIOS
        ''')

    def populate_datamun(self, db, reload=False):
        cambios_municipios = {c.cod: c.nuevo.split(
        )[0] for c in self.cambios if c.remplaza}
        cols = set()
        municipio = {}

        col_empresas = [
            r if r != "Total" else "Total empresas" for r in self.empresas[4] if r]
        for record in self.empresas:
            if len(record) < 2 or record[0] is None:
                continue
            mun = record[0].split()[0]
            if not mun.isdigit() or len(mun) != 5:
                continue
            if mun in cambios_municipios:
                mun = cambios_municipios[mun]
            for j, col in enumerate(col_empresas):
                for i, year in enumerate(range(2018, 2011, -1)):
                    index = (j*7)+i+1
                    e = record[index]
                    if e is not None:
                        dt = municipio.get(mun, {})
                        yr = dt.get(year, {})

                        yr[col] = e

                        dt[year] = yr
                        municipio[mun] = dt

        sepe_municipio = {}
        cols_sepe = []
        for mun, dYear in self.paro.items():
            sepe_dt = sepe_municipio.get(mun, {})
            dt = municipio.get(mun, {})
            for year, dMes in dYear.items():
                yr = dt.get(year, {})
                divisor = {}
                for mes, sepe in dMes.items():
                    sepe_dt[(year, mes)] = sepe
                    for k, v in sepe.items():
                        divisor[k] = divisor.get(k, 0) + 1
                        yr[k] = yr.get(k, 0) + v
                        if k not in cols_sepe:
                            cols_sepe.append(k)
                for k, v in divisor.items():
                    yr[k] = yr[k] / v
                dt[year] = yr
            sepe_municipio[mun] = sepe_dt
            municipio[mun] = dt

        for year, data in self.renta_aeat.items():
            for mun, rent in data.items():
                if len(mun) != 5:
                    continue
                dt = municipio.get(mun, {})
                yr = dt.get(year, {})
                yr["renta"] = (rent["media"] *
                               rent["declaraciones"]) / rent["mayores"]

                dt[year] = yr
                municipio[mun] = dt

        for year, dt in sorted(self.renta_euskadi.items()):
            for mun, rent in dt.items():
                dt = municipio.get(mun, {})
                yr = dt.get(year, {})

                yr["renta"] = rent

                dt[year] = yr
                municipio[mun] = dt

        for cod, data in sorted(self.core.items()):
            if not cod.isdigit():
                continue
            poblacion = data["poblacion5"]
            for year, url in sorted(poblacion.items()):
                year = int(year)
                for i in get_js(url):
                    sex, mun, edad = i["MetaData"]
                    mun = get_cod_municipio(
                        cod, mun, cambiar=cambios_municipios)
                    if mun is None:
                        continue

                    c_sex = sex["Codigo"].strip()
                    c_edad = edad["Codigo"].strip()

                    if c_sex == "varones":
                        c_sex = "hombres"

                    if c_edad == "59":
                        c_edad = "0509"
                    elif c_edad == "85ym s":
                        c_edad = "85ymas"
                    if c_edad == "04":
                        c_edad = "04ymenos"
                    elif len(c_edad) == 4:
                        c_edad = c_edad[0:2]+"a"+c_edad[2:4]

                    valor = i["Data"][0]["Valor"]
                    dt = municipio.get(mun, {})
                    yr = dt.get(year, {})

                    key = c_sex+" "+c_edad
                    yr[key] = int(valor) if valor is not None else None

                    dt[year] = yr
                    municipio[mun] = dt

                    cols.add(key)

            for i in get_js(data["poblacion"]):
                mun, sex, _, _ = i["MetaData"]
                mun = get_cod_municipio(None, mun, cambiar=cambios_municipios)
                if mun is not None:
                    key = None
                    sex = sex["Nombre"]
                    if sex == "Total":
                        key = "ambossexos total"
                    elif sex == "Mujeres":
                        key = "mujeres total"
                    elif sex == "Hombres":
                        key = "hombres total"
                    if not key:
                        continue
                    for d in i["Data"]:
                        year = d["Anyo"]
                        valor = d["Valor"]

                        dt = municipio.get(mun, {})
                        yr = dt.get(year, {})

                        if yr.get(key, None) is None:
                            yr[key] = int(valor) if valor is not None else None
                            dt[year] = yr
                            municipio[mun] = dt

            year = 1999
            censo = data.get("censo_%s" % year, None)
            if censo is not None:
                for i in get_js(censo["superficie"]):
                    mun, tenencia = i["MetaData"]
                    mun = get_cod_municipio(
                        cod, mun, cambiar=cambios_municipios)
                    if mun is not None and tenencia["Codigo"] == "todoslosregimenes":
                        valor = i["Data"][0]["Valor"]
                        if valor is not None:
                            dt = municipio.get(mun, {})
                            yr = dt.get(year, {})

                            yr["SAU"] = int(valor)

                            dt[year] = yr
                            municipio[mun] = dt
                for i in get_js(censo["unidades"]):
                    mun, tipo = i["MetaData"]
                    mun = get_cod_municipio(
                        cod, mun, cambiar=cambios_municipios)
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
                            dt = municipio.get(mun, {})
                            yr = dt.get(year, {})

                            yr[key] = int(valor)

                            dt[year] = yr
                            municipio[mun] = dt
        year = 2009
        with open(self.core.todas["censo_%s" % year], "r") as f:
            soup = BeautifulSoup(f, "lxml")

        for tr in soup.findAll("tr"):
            tds = tr.findAll(["th", "td"])
            if len(tds) == 20:
                cod = tds[0].get_text().strip().split()[0]
                if cod.isdigit() and len(cod) == 5:

                    if cod in cambios_municipios:
                        cod = cambios_municipios[cod]

                    datos = [parse_td(td) for td in tds[1:]]
                    dt = municipio.get(cod, {})
                    yr = dt.get(year, {})

                    yr["explotaciones"] = datos[0]
                    yr["SAU"] = datos[1]
                    yr["unidadesganaderas"] = datos[10]
                    yr["UTA"] = datos[11]

                    dt[year] = yr
                    municipio[cod] = dt

        cols = list(sorted(cols, key=sort_col)) + col_empresas + cols_sepe + \
            ["SAU", "explotaciones", "unidadesganaderas", "UTA", "renta"]

        db.create('''
            create table socioeconomico (
              MUN TEXT REFERENCES municipios(ID),
              YEAR INTEGER,
              %s
              PRIMARY KEY (MUN, YEAR)
            )
        ''', *[unidecode(c) for c in cols])

        for cod, mun in sorted(municipio.items()):
            for year, dt in sorted(mun.items()):
                row = {
                    "MUN": cod,
                    "YEAR": year
                }
                for col in cols:
                    val = dt.get(col, None)
                    if val is None:
                        continue
                    if col in cols_sepe and isinstance(val, float):
                        val = int(val)
                    if col == "renta" and val == "":
                        val = self.renta_menos1000.get(year, {}).get(
                            cod, {}).get("renta_m18", "")
                        row["tipo_renta"] = 1
                    row[unidecode(col)] = val
                db.insert("socioeconomico", **row)

        db.create('''
            create table sepemes (
              MUN TEXT REFERENCES municipios(ID),
              YEAR INTEGER,
              MES INTEGER,
              %s
              PRIMARY KEY (MUN, YEAR, MES)
            )
        ''', *[unidecode(c) for c in cols_sepe])

        for cod, mun in sorted(sepe_municipio.items()):
            for ym, dt in sorted(mun.items()):
                row = {
                    "MUN": cod[0],
                    "YEAR": cod[1],
                    "MES": ym,
                }
                for col in cols_sepe:
                    val = dt.get(col)
                    if val is None:
                        continue
                    row[unidecode(col)] = val
                db.insert("sepemes", **row)

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
            url = "http://www.ine.es/dynt3/inebase/es/index.htm?type=pcaxis&file=pcaxis&path=%2Ft20%2Fe245%2Fp05%2F%2Fa" + \
                str(y)
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

        self.save()


if __name__ == "__main__":
    d = Dataset()
    # d.create_aeat(reload=True)
    d.create_datamun(reload=True)
    #d.create_datamunarea(20, reload=True)
