#!/usr/bin/env python3

from core.dataset import Dataset
from core.db import DBMun, plain_parse_col

dataset = Dataset()
dataset.unzip()
db = DBMun(reload=False, parse_col=plain_parse_col)#file="dataset/municipios.db")
dataset.populate_datamun(db)
db.close()
db.zip()
