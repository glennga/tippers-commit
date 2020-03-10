""" Courtesy of https://stackoverflow.com/a/25080347 """

import psycopg2
import json

with open('config/postgres.json') as postgres_config_file:
    conn = psycopg2.connect(**json.load(postgres_config_file))
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    curs = conn.cursor()
    curs.execute("SELECT gid FROM pg_prepared_xacts WHERE database = current_database()")
    for (gid,) in curs.fetchall():
        curs.execute("ROLLBACK PREPARED %s", (gid,))
