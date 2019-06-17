#!/usr/bin/env python3

from core.dataset import Dataset
from core.db import DBMun

dataset = Dataset()
dataset.unzip()
db = DBMun(reload=True)
#dataset.reload=True
dataset.populate_datamun(db)
db.close()
db.zip()
