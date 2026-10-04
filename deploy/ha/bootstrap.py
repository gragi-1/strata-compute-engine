"""Create the dedicated application role after Patroni initializes a new cluster."""

import json
import sys
from pathlib import Path

import psycopg
from psycopg import sql


def main():
    password = json.loads(Path("/run/strata/secrets.json").read_text())["application"]
    with psycopg.connect(sys.argv[1], autocommit=True) as connection:
        connection.execute(
            sql.SQL(
                "CREATE ROLE strata LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                "NOREPLICATION PASSWORD {}"
            ).format(sql.Literal(password))
        )
        connection.execute("CREATE DATABASE strata OWNER strata")


if __name__ == "__main__":
    main()
