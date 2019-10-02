#!/usr/bin/env python3

import argparse
import logging
import os

from core.create_db import create_db
from core.db import DBshp, plain_parse_col

parser = argparse.ArgumentParser("Crea base de datos de municipios")
parser.add_argument('--verbose', '-v', action='count',
                    help="Nivel de depuración", default=int(os.environ.get("DEBUG_LEVEL", 0)))
parser.add_argument('salida', nargs='?',
                    help='Nombre de la base de datos de salida', default="dataset/municipios.db")
args = parser.parse_args()

levels = [logging.WARNING, logging.INFO, logging.DEBUG]
level = levels[min(len(levels)-1, args.verbose)]

logging.basicConfig(
    level=level, format='%(asctime)s - %(levelname)s - %(message)s')

create_db(args.salida)
