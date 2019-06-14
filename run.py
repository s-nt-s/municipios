#!/usr/bin/env python3

from core.dataset import Dataset
from core.db import DBMun

dataset = Dataset()
db = DBMun(reload=True)
dataset.populate_datamun(db)
