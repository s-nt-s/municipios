El objetivo de este proyecto es generar una base de datos con datos abiertos
de municipios españoles, principalmente datos geográficos y socioeconómicos.

# Requerimientos

```console
$ sudo apt-get install p7zip-full
$ sudo apt-get install spatialite-bin
$ sudo apt-get install libsqlite3-mod-spatialite
$ sudo apt-get install mdbtools
$ sudo pip install -r requirements.txt
```

# Aviso

Aunque los `json` generados son fieles a las fuentes, en la base de datos
se han hecho las siguientes transformaciones para facilitar la comparación
de los datos a lo largo del tiempo:

* Si varios municipios se han fusionado en uno solo, se reconstruye la
fusión retroactivamente (agregando los datos de los municipios originales)
de manera que en la base de datos solo figura el municipio resultando
de la fusión
* Si un municipio se desgaja en varios y aparecen datos de los nuevos
municipios en alguna fuente antes que en el propio ine, se reconstruye,
para esas fuentes, el municipio original (antes de desgajarse) para que no
aparezca hasta que también salga en el ine.

Con el mismo animo de facilitar la comparación, solo se cargan datos
hasta el último año completo, de manera de que si estamos a mediados de
2019, la base de datos solo tendrá datos hasta final de 2018.

Además solo se cargan datos hasta el último año en el que INE haya publicado
las estadísticas de población.
