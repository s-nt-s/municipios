import functools
import logging

from .common import *


class Cache:
    def __init__(self, file, *args, avoidReload=False, **kargv):
        self._file = file
        self.data = {}
        self.func = None
        self.avoidReload = avoidReload

    def file_path(self):
        return self._file

    def read(self):
        pass

    def save(self):
        pass

    def callCache(self, slf, *args, **kargs):
        reload = self.isReload(slf, *args, **kargs)
        data = None
        if not reload:
            data = self.read(*args, **kargs)
        ln1 = len(data) if data else -1
        data = self.func(slf, *args, old_data=data, **kargs)
        ln2 = len(data) if data else -1
        if reload or (ln2 != ln1):
            self.save(data, *args, **kargs)
        return data

    def isReload(self, slf, *args, **kargs):
        fl = self.file_path(*args, **kargs)
        if "*" not in fl:
            if os.path.isfile(fl):
                logging.debug("EXITE: " + fl)
            else:
                logging.debug("NO EXITE: " + fl)
        if self.avoidReload:
            return False
        reload = getattr(slf, "reload", None)
        if reload is not None and reload == True:
            logging.debug("RECARGA por orden de la clase")
            return True
        if (isinstance(reload, list) or isinstance(reload, tuple)) and self._file in reload:
            logging.debug("RECARGA por orden de la clase")
            return True
        return False

    def __call__(self, func):
        functools.update_wrapper(self, func)
        self.func = func
        return lambda *args, **kargs: self.callCache(*args, **kargs)


class JsonCache(Cache):
    def __init__(self, *args, intKey=True, sort_keys=False, **kargv):
        Cache.__init__(self, *args, **kargv)
        self.intKey = intKey
        self.sort_keys = sort_keys

    def read(self, *args, **kargs):
        fl = self.file_path(*args, *kargs)
        return read_js(fl, intKey=self.intKey)

    def save(self, data, *args, **kargs):
        fl = self.file_path(*args, *kargs)
        save_js(fl, data, sort_keys=self.sort_keys)


class ParamJsonCache(JsonCache):
    def __init__(self, *args, **kargv):
        JsonCache.__init__(self, *args, **kargv)

    def read(self, *args, **kargs):
        f = self.file_path(*args, *kargs)
        return read_js(f, intKey=self.intKey)

    def save(self, data, *args, **kargs):
        f = self.file_path(*args, *kargs)
        save_js(f, data)

    def file_path(self, *args, **kargs):
        return self._file.format(*args, *kargs)


class KmCache(Cache):
    def __init__(self, *args, **kargv):
        Cache.__init__(self, *args, **kargv)

    def read(self, *args, **kargs):
        fl = self.file_path(*args, **kargs)
        data = {}
        for a, b, km in readlines(fl, fields=3):
            data[(a, b)] = float(km)
        return data

    def save(self, data, *args, **kargs):
        fl = self.file_path(*args, **kargs)
        with open(fl, "w") as f:
            for key, val in sorted(data.items()):
                if int(km) == km:
                    km = int(km)
                a, b = key
                f.write("%s %s %s\n" % (a, b, km))
