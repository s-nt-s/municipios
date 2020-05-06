#!/usr/bin/env python3

import argparse
import json
import logging
import os

from core.create_db import create_db
from core.db import DBshp, plain_parse_col

parser = argparse.ArgumentParser("Crea base de datos de municipios")
parser.add_argument('--verbose', '-v', action='count',
                    help="Nivel de depuración", default=int(os.environ.get("DEBUG_LEVEL", 0)))
parser.add_argument('--solojs', action='store_true',
                    help="Solo generar los js")
parser.add_argument('datos', nargs='?',
                    help='Nombre de la base de datos de consulta', default="dataset/municipios.db")
args = parser.parse_args()

levels = [logging.WARNING, logging.INFO, logging.DEBUG]
level = levels[min(len(levels)-1, args.verbose)]

logging.basicConfig(
    level=level, format='%(asctime)s - %(levelname)s - %(message)s')


def parse_result(r):
    geojson = {'type': 'FeatureCollection', 'features': []}
    for p in r:
        p["nombre"] = p["nombre"].split("/")[-1]
        js = json.loads(p["geom"])
        js["properties"] = {"n": p["nombre"], "i": p["ID"]}
        geojson['features'].append(js)
    return geojson


def create_script(db, t):
    file = "dataset/geo/"+t+".js"
    db.save_js(file, sql="select ID, nombre, AsGeoJSON(geom, 4) geom from " +
               t+" order by nombre, id", indent=None, parse_result=parse_result)

os.makedirs("dataset/geo", exist_ok=True)
db = DBshp(args.datos, parse_col=plain_parse_col)
create_script(db, "provincias")
create_script(db, "municipios")
db.close()
