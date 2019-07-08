import io
import ntpath
import os
import re

import py7zlib

from .common import get_parts

re_chomp = re.compile(r"[\n\r]+$")


class JoinFileOpener:
    def __init__(self, *files):
        if len(files) == 1:
            self._file = open(files[0], "rb")
        else:
            self._file = io.BytesIO()
            for fl in files:
                with open(fl, "rb") as f:
                    self._file.write(f.read())
            self._file.seek(0)

    def __enter__(self, *args, **kargs):
        return self._file

    def __exit__(self, *args, **kargs):
        self._file.close()


class jFile:
    def __init__(self, file):
        self.fullname = file
        self.files = get_parts(file)
        self.path = os.path.dirname(file)
        self.file = ntpath.basename(file)
        self.type = file.rsplit(".", 1)[-1].lower()
        self.main = self.type
        if len(self.files) and (self.files[0].endswith(".7z") or self.files[0].endswith(".7z.001")):
            self.main = "7z"

    def content(self):
        if self.files:
            if self.main == "7z":
                with JoinFileOpener(*self.files) as f:
                    f7z = py7zlib.Archive7z(f)
                    name = f7z.getnames()[0]
                    self.file = ntpath.basename(name)
                    self.type = name.rsplit(".", 1)[-1].lower()
                    txt = f7z.getmember(name)
                    for l in io.StringIO(txt.read().decode()):
                        l = re_chomp.sub("", l)
                        yield l
            else:
                for file in self.files:
                    with open(file, "r") as f:
                        for l in f.readlines():
                            l = re_chomp.sub("", l)
                            yield l

    def lines(self):
        for l in self.content():
            l = l.strip()
            if l and not l.startswith("#"):
                yield l

    def tuples(self, cast=None, head=False, separator=None):
        gen = self.lines()
        length = len(cast) if cast else -1
        if head:
            head = next(gen)
            tp = head.split(separator, length)
            yield tp
        for l in gen:
            tp = l.split(separator, length)
            if cast:
                tp = tuple((c(i) for c, i in zip(cast, tp)))
            yield tp

    def items(self, *args, separator=None, **kargv):
        gen = self.tuples(head=True, cast=args, separator=separator)
        head = next(gen)
        for tp in gen:
            item = {k: v for k, v in zip(head, tp)}
            for k, v in kargv.items():
                if k in item:
                    item[k] = v(item[k])
            yield item

    @property
    def empty(self):
        if not self.files:
            return True
        gen = self.lines()
        l = next(gen, None)
        if l is None:
            return True
        l = next(gen, None)
        if l is None and self.type == "csv":
            return True
        return False
