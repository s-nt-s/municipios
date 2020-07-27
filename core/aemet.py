import json
import logging
import re
import sys
import time
from datetime import datetime
from statistics import mean, stdev
import os

import bs4
import requests
from bunch import Bunch

from .common import save_js


level = int(os.environ.get("DEBUG_LEVEL", "1"))
levels = [logging.WARNING, logging.INFO, logging.DEBUG]
level = levels[min(len(levels)-1, level)]

logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')

def get_txt(soup, slc):
    n = soup.select_one(slc)
    if n is None:
        return None
    n = n.get_text().strip()
    if n == "":
        return None
    return n


def save_js(file, *datas, indent=2, **kargv):
    separators = (',', ':') if indent is None else None
    with open(file, "w") as f:
        for data in datas:
            json.dump(data, f, indent=indent, separators=separators)
        for k, v in kargv.items():
            f.write("var "+k+"=")
            json.dump(v, f, indent=indent, separators=separators)
            f.write(";\n")


class Aemet:
    def __init__(self, key=None):
        if key is None:
            logging.warning(
                "No se ha facilitado api key, por lo tanto solo estaran disponibles los endpoints xml")
        self.key = key
        self.now = datetime.now()
        self.requests_verify = not(os.environ.get("AVOID_REQUEST_VERIFY") == "true")
        logging.info("requests_verify = " + str(self.requests_verify))

    def _safe_int(self, s, label=None):
        if s is None or s == "":
            return None
        try:
            s = float(s)
        except Exception as e:
            if label:
                logging.critical(label+" = "+str(v) +
                                  " no es un float", exc_info=True)
            return None
        if s == int(s):
            return int(s)
        return s

    def _meanDict(self, keys, arr, desviacion=None):
        d = {}
        vls = {}
        for k in keys:
            values = tuple(self._safe_int(i[k], label=k) for i in arr)
            values = tuple(i for i in values if i is not None)
            vls[k] = values
            if len(values) == 0:
                d[k] = None
                continue
            if k.endswith("min"):
                d[k] = min(values)
            elif k.endswith("max"):
                d[k] = max(values)
            else:
                d[k] = mean(values)
        if desviacion:
            for k, new_k in desviacion.items():
                values = vls.get(k)
                if values is None or len(values) < 2:
                    d[new_k] = None
                else:
                    d[new_k] = stdev(values)
        return d

    def addkey(self, url):
        if self.key in url:
            return url
        url = url.replace("api_key=", "api_key="+self.key)
        if self.key in url:
            return url
        if "?" in url:
            return url+"&api_key="+self.key
        return url+"?api_key="+self.key

    def _get(self, url, url_debug=None, intentos=0):
        try:
            return requests.get(url, verify=self.requests_verify)
        except Exception as e:
            if intentos < 4:
                time.sleep(61)
                return self._get(url, url_debug=url_debug, intentos=intentos+1)
            logging.critical("GET "+(url_debug or url) +
                              " > "+str(e), exc_info=True)
            return None

    def get_json(self, url):
        r = self._get(self.addkey(url), url_debug=url)
        if r is None:
            return None
        try:
            j = r.json()
        except Exception as e:
            logging.critical("GET "+url+" > "+str(r.text) +
                              " > "+str(e), exc_info=True)
            return None
        url_datos = j.get('datos')
        if url_datos is None:
            estado = j.get("estado")
            if estado == 429:
                time.sleep(61)
                return self.get_json(url)
            logging.critical("GET "+url+" > "+str(j), exc_info=True)
            return None
        try:
            r = requests.get(url_datos, verify=self.requests_verify)
        except Exception as e:
            logging.critical("GET "+url_datos+" > "+str(e), exc_info=True)
            return None
        try:
            j = r.json()
        except Exception as e:
            logging.critical("GET "+url_datos+" > " +
                              str(r.text)+" > "+str(e), exc_info=True)
            return None
        return j

    def get_xml(self, url):
        r = self._get(url)
        if r is None:
            return None
        try:
            soup = bs4.BeautifulSoup(r.text, 'lxml')
            name = url.split("/")[-1]
            if name.startswith("localidad_"):
                name = name[:12]+"/"+name[12:]
            name = self.now.strftime("%Y.%m.%d_%H.%M")+"/"+name
            name = "fuentes/aemet/prevision/"+name
            dir = os.path.dirname(name)
            os.makedirs(dir, exist_ok=True)
            with open(name, "w") as f:
                f.write(r.text)
        except Exception as e:
            logging.critical("GET "+url+" > "+str(r.text) +
                              " > "+str(e), exc_info=True)
            return None
        return soup

    def get_municipios(self, provincia):
        r = self._get(
            "https://opendata.aemet.es/centrodedescargas/xml/municipios/loc%02d.xml" % int(provincia))
        if r is None:
            return None
        provs = re.findall(r"<ID>\s*id(.+?)\s*</ID>",
                           r.text, flags=re.IGNORECASE)
        if len(provs) == 0:
            logging.critical("GET "+url+" > "+str(r.text))
        return sorted(set(provs))

    def get_prediccion_semanal(self, *provincias, key_total=None):
        if len(provincias) == 0:
            j = self.get_xml(
                "https://opendata.aemet.es/centrodedescargas/xml/provincias.xml")
            provincias = sorted(set(i.get_text().strip()
                                    for i in j.select("provincia id")))
        keys = (
            "prec_medi",
            "vien_velm",
            "vien_rach",
            "temp_maxi",
            "temp_mini",
        )
        desviacion = {"temp_mini": "tmin_vari"}
        prediccion = {}
        for provincia in provincias:
            dt_prov = []
            for mun in self.get_municipios(provincia):
                url = "http://www.aemet.es/xml/municipios/localidad_" + \
                    str(mun) + ".xml"
                j = self.get_xml(url)
                if j is None:
                    continue
                dt_mun = []
                for dia in j.select("prediccion > dia")[:7]:
                    d = Bunch(
                        prec_medi=get_txt(dia, "prob_precipitacion"),
                        vien_velm=get_txt(dia, "viento velocidad"),
                        vien_rach=get_txt(dia, "racha_max"),
                        temp_maxi=get_txt(dia, "temperatura maxima"),
                        temp_mini=get_txt(dia, "temperatura minima"),
                        hume_maxi=get_txt(dia, "humedad_relativa minima"),
                        hume_mini=get_txt(dia, "humedad_relativa minima"),
                        nieve=get_txt(dia, "cota_nieve_prov"),
                        cielo=get_txt(dia, "estado_cielo"),
                        stmax=get_txt(dia, "sens_termica minima"),
                        stmin=get_txt(dia, "sens_termica minima"),
                        uvmax=get_txt(dia, "uv_max"),
                    )
                    d = {k: v for k, v in dict(d).items() if k in keys}
                    dt_mun.append(d)
                if len(dt_mun) == 0:
                    logging.critical("GET "+url+" > "+str(j))
                    continue
                dt_prov.append(self._meanDict(
                    keys, dt_mun, desviacion=desviacion))
            if len(dt_prov) == 0:
                logging.debug("get_prediccion_provincia : len(dt_prov)==0")
                continue
            if len(dt_prov) == 1:
                data = dt_prov[0]
            else:
                data = self._meanDict(keys, dt_prov, desviacion=desviacion)
            prediccion[provincia] = data
        if key_total:
            data = self._meanDict(keys, prediccion.values())
            prediccion[key_total] = data
        prediccion["__timestamp__"] = time.time()
        return prediccion


if __name__ == "__main__":
    a = Aemet()
    pre = a.get_prediccion_semanal()
    save_js("dataset/aemet/prediccion_semanal.json", pre)
