import functools
import json
import math
import os
import re
import sqlite3
import textwrap

try:
    from .common import *
except:
    from common import *

class JsonCache:
    def __init__(self, file):
        self.file = file
        self.data = {}
        self.func = None

    def callCache(self, slf, *args, **kargs):
        if not self.isReload(slf):
            data = read_js(self.file, intKey=True)
            if data:
                return data
        data = self.func(slf, *args, **kargs)
        save_js(self.file, data)
        return data

    def isReload(self, slf):
        reload = getattr(slf, "reload", None)
        if reload is None or reload == True:
            return True
        if (isinstance(reload, list) or isinstance(reload, tuple)) and self.file in reload:
            return True
        return False

    def __call__(self, func):
        functools.update_wrapper(self, func)
        self.func = func
        return lambda *args, **kargs: self.callCache(*args, **kargs)
