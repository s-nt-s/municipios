import re

re_rtrim = re.compile(r"^\s*\n")

class MDWriter:
    def __init__(self, file):
        self.file = file
        self.f = open(file, "w")
        self.last_line=""

    def close(self):
        self.f.close()

    def write(self, s, *args, end="\n", **kargv):
        if args or kargv:
            s = s.format(*args, **kargv)
        if s.startswith("#"):
            if len(self.last_line) > 0:
                s = "\n" + s
            s = s + "\n"
        if len(self.last_line) == 0:
            s = re_rtrim.sub("", s)
        self.last_line = s.split("\n")[-1]
        self.f.write(s+end)
