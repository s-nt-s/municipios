#!/usr/bin/env python3

from core.dataset import Dataset
from core.db import DBMun, plain_parse_col

dataset = Dataset(reload=["dataset/poblacion/edad_*.json"])
dataset.unzip()
db = DBMun(file="dataset/municipios_tmp.db", reload=True, parse_col=plain_parse_col)
#dataset.reload=True
dataset.populate_datamun(db)
db.close()
db.zip()
