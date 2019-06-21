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

class Cache:
    def __init__(self, file):
        self.file = file
        self.data = {}
        self.func = None

    def read(self):
        pass

    def save(self):
        pass

    def callCache(self, slf, *args, **kargs):
        if not self.isReload(slf):
            data = self.read()
            if data:
                return data
        data = self.func(slf, *args, **kargs)
        self.save(data)
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

class JsonCache(Cache):
    def __init__(self, *args, **kargv):
        Cache.__init__(self, *args, **kargv)

    def read(self):
        return read_js(self.file, intKey=True)

    def save(self, data):
        save_js(self.file, data)


class KmCache(Cache):
    def __init__(self, *args, **kargv):
        Cache.__init__(self, *args, **kargv)

    def read(self):
        data={}
        for a, b, km in readlines(self.file, fields=3):
            data[(a, b)]=float(km)
        return data

    def save(self, data):
        with open(self.file, "w") as f:
            for key, val in sorted(data.items()):
                if int(km)==km:
                    km=int(km)
                a, b = key
                f.write("%s %s %s\n" % (a, b, km))
