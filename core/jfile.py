import os
from io import BytesIO
import py7zlib
import io
from glob import glob
import re
import tempfile
import os
import ntpath
from .common import get_parts

re_chomp=re.compile(r"[\n\r]+$")

class jFile:
    def __init__(self, file, auto_close=True):
        self.fullname = file
        self.files=get_parts(file)
        self.path = os.path.dirname(file)
        self.file = ntpath.basename(file)
        self.type = file.rsplit(".", 1)[-1].lower()
        self.main = self.type
        self.tmp = None
        self.auto_close = auto_close

    def open(self):
        if len(self.files)>1 and (self.tmp is None or not os.path.isfile(self.tmp)):
            f1 = self.files[0]
            if f.endswith(".7z.001") or f.endswith(".7z"):
                self.main = "7z"
                with tempfile.NamedTemporaryFile(suffix=".7z", delete=False) as tmp:
                    for file in self.files:
                        with open(file, "rb") as f:
                            tmp.write(f.read())
                    self.tmp = tmp.name

    def close(self):
        if self.tmp and os.path.isfile(self.tmp):
            os.remove(self.tmp)

    def content(self):
        if self.files:
            self.open()
            if self.main == "7z":
                file = self.tmp or self.files[0]
                with open(file, "rb") as f:
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
            if self.auto_close:
                self.close()

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
                tp = tuple((c(i) for c,i in zip(cast, tp)))
            yield tp

    def items(self, *args, separator=None, **kargv):
        gen = self.tuples(head=True, cast=args, separator=separator)
        head = next(gen)
        for tp in gen:
            item = {k:v for k,v in zip(head, tp)}
            for k, v in kargv.items():
                if k in item:
                    item[k]=v(item[k])
            yield item
