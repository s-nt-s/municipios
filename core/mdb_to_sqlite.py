import os
import sqlite3
import subprocess
import sys


def get_config():
    if len(sys.argv) != 2:
        return None
    database = sys.argv[1]
    if not os.path.isfile(database) or not database.endswith(".mdb"):
        return None
    return database


def save(sql_script, out, save_sql=False):
    sqlite = out+"sqlite"
    if os.path.isfile(sqlite):
        os.remove(sqlite)
    con = sqlite3.connect(sqlite)
    c = con.cursor()
    c.executescript(sql_script)
    con.commit()
    c.close()
    if save_sql:
        with open(out+"sql", "w") as f:
            f.write(sql_script)


def run_cmd(*args):
    output = subprocess.check_output(args)
    output = output.decode('utf-8')
    return output


def mdb_to_sqlite(DATABASE, save_sql=False):
    # Dump the schema for the DB
    SQL_SCRIPT = run_cmd("mdb-schema", DATABASE, "mysql")

    # Get the list of table names with "mdb-tables"
    table_names = subprocess.Popen(["mdb-tables", "-1", DATABASE],
                                   stdout=subprocess.PIPE).communicate()
    table_names = table_names[0]
    tables = table_names.splitlines()

    # start a transaction, speeds things up when importing
    SQL_SCRIPT = SQL_SCRIPT + '\nBEGIN;'

    # Dump each table as a CSV file using "mdb-export",
    # converting " " in table names to "_" for the CSV filenames.
    for table in tables:
        if len(table) > 0:
            output = run_cmd("mdb-export", "-I", "mysql", DATABASE, table)
            SQL_SCRIPT = SQL_SCRIPT + '\n' + output

    SQL_SCRIPT = SQL_SCRIPT + "\nCOMMIT;"  # end the transaction

    NAME = DATABASE[:-3]
    save(SQL_SCRIPT, NAME, save_sql=save_sql)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Ha de pasar como parametro la ruta de un fichero")
    f = sys.argv[1]
    if not os.path.isfile(f):
        sys.exit(f+" no existe")
    mdb_to_sqlite(f)
