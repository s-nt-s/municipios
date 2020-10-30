from subprocess import DEVNULL, STDOUT, check_call
import os
import tempfile
from PIL import Image
from urllib.request import urlretrieve
from textwrap import dedent

class SchemasPy:
    def __init__(self, home=None):
        self.driver="https://github.com/xerial/sqlite-jdbc/releases/download/3.32.3.2/sqlite-jdbc-3.32.3.2.jar"
        self.jar="https://github.com/schemaspy/schemaspy/releases/download/v6.1.0/schemaspy-6.1.0.jar"
        self._driver = os.path.basename(self.driver)
        self._jar = os.path.basename(self.jar)
        self.home=home
        if self.home is None and os.path.isdir("schemaspy"):
            self.home="schemaspy"
        self.root = os.path.realpath(self.home)+"/"

    def dwn(self, url):
        name = os.path.basename(url)
        if os.path.isfile(self.root+name):
            return False
        print("wget", url)
        urlretrieve(url, self.root+name)
        return True

    def write(self, file, txt, overwrite=False):
        if not overwrite and os.path.isfile(file):
            return False
        with open(file, "w") as f:
            f.write(dedent(txt).strip())
        return True

    def report(self, file, out=None, **kargv):
        # https://github.com/schemaspy/schemaspy/issues/524#issuecomment-496010502
        if self.home is None:
            self.home = tempfile.mkdtemp()
        if not os.path.isdir(self.home):
            os.makedirs(self.home, exist_ok=True)
        if out is None:
            out = tempfile.mkdtemp()

        r1 = self.dwn(self.driver)
        r2 = self.dwn(self.jar)
        reload = r1 or r2

        self.write(self.root+"sqlite.properties", '''
            driver=org.sqlite.JDBC
            description=SQLite
            driverPath={driver}
            connectionSpec=jdbc:sqlite:<db>
        '''.format(driver=self._driver), overwrite=reload)

        self.write(self.root+"schemaspy.properties", '''
            schemaspy.t=sqlite
            schemaspy.sso=true
        ''', overwrite=reload)

        self.write(self.root+"rename.sh", '''
            #!/bin/bash
            grep "$1" -l -r $2 | xargs sed -i -e "s|${1}||g"
        ''', overwrite=True)

        name = os.path.basename(file)
        name = name.rsplit(".", 1)[0]
        db=os.path.realpath(file)
        out=os.path.realpath(out)
        cmd = "java -jar {root}{schemaspy} -dp {root} -db {db} -o {out} -cat {name} -s {name} -u {name}".format(
            schemaspy=self._jar,
            root=self.root,
            db=db,
            out=out,
            name=name,
        )
        for k, v in kargv.items():
            if len(k)==1:
                k = "-"+k
            else:
                k = "--"+k
            if isinstance(v, str):
                v = '"'+v+'"'
            else:
                v = str(v)
            cmd = cmd + " {k} {v}".format(k=k, v=v)
        current_dir = os.getcwd()
        os.chdir(self.root)
        print(cmd)
        self.run(cmd)
        self.run("bash", "rename.sh", os.path.dirname(db)+"/", out)
        os.chdir(current_dir)
        print(out+"/index.html")
        return out

    def run(self, *args):
        if len(args)==1 and " " in args[0]:
            args = args[0].split()
        check_call(args, stdout=DEVNULL, stderr=STDOUT)

    def save_diagram(self, db, img, size="compact", **kargv):
        out = self.report(db, **kargv)
        im = Image.open(out+"/diagrams/summary/relationships.real.{}.png".format(size))
        box = im.getbbox()
        box = list(box)
        box[3] = box[3] - 45
        gr = im.crop(tuple(box))
        gr.save(img)
        gr.close()
        im.close()

if __name__ == "__main__":
    s = SchemasPy()
    s.save_diagram(
        "dataset/municipios.db",
        "dataset/municipios.png",
        size="large",
        I=".*(spatial|geometry|CAMBIOS|CRS_KMS|AREA_INFLUENCIA|idx_|SpatialIndex|sql_statements_log|ElementaryGeometries).*",
    )
    out = "docs/informe/"
    if os.path.isdir(out):
        import shutil
        shutil.rmtree(out)
    s.report("dataset/municipios.db", out=out)
