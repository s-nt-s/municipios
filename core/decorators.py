import functools

from .common import *


class Cache:
    def __init__(self, file, avoidReload=False):
        self.file = file
        self.data = {}
        self.func = None
        self.avoidReload = avoidReload

    def read(self):
        pass

    def save(self):
        pass

    def callCache(self, slf, *args, **kargs):
        if not self.isReload(slf):
            data = self.read(*args, **kargs)
            if data:
                return data
        data = self.func(slf, *args, **kargs)
        self.save(data, *args, **kargs)
        return data

    def isReload(self, slf):
        if self.avoidReload:
            return False
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
    def __init__(self, *args, intKey=True, **kargv):
        Cache.__init__(self, *args, **kargv)
        self.intKey = intKey

    def read(self, *args, **kargs):
        return read_js(self.file, intKey=self.intKey)

    def save(self, data, *args, **kargs):
        save_js(self.file, data)


class ParamJsonCache(JsonCache):
    def __init__(self, *args, **kargv):
        JsonCache.__init__(self, *args, **kargv)

    def read(self, *args, **kargs):
        f = self.file.format(*args, **kargs)
        return read_js(f, intKey=self.intKey)

    def save(self, data, *args, **kargs):
        f = self.file.format(*args, *kargs)
        save_js(f, data)


class KmCache(Cache):
    def __init__(self, *args, **kargv):
        Cache.__init__(self, *args, **kargv)

    def read(self, *args, **kargs):
        data = {}
        for a, b, km in readlines(self.file, fields=3):
            data[(a, b)] = float(km)
        return data

    def save(self, data, *args, **kargs):
        with open(self.file, "w") as f:
            for key, val in sorted(data.items()):
                if int(km) == km:
                    km = int(km)
                a, b = key
                f.write("%s %s %s\n" % (a, b, km))
