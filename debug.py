#!/usr/bin/env python3

from core.dataset import Dataset
from core.db import DBMun, plain_parse_col
import sys

db = DBMun(reload=True, parse_col=plain_parse_col, file="debug.db")
db.execute("sql/distancias.sql")
db.close()
sys.exit()
dataset = Dataset()
dataset.unzip()
dataset.populate_datamun(db)
db.close()
