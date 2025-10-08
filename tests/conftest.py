import logging
import sys
import os
import re
import csv
from typing import Callable, List
from alembic import config as alembic_config_module
import pycds
import testing.postgresql
import sqlalchemy as sa

from sqlalchemy.schema import CreateSchema
from sqlalchemy.orm import Session
from pycds import Network, Station, History

def alembic_config():
    """
    In a test config, none of the existing environments are appropriate, we want to
    use a blank local database, so we generate a new config here.
    """
    alembic_config = alembic_config_module.Config()
    alembic_config.set_main_option("script_location", "pycds/alembic")
    return alembic_config

def alembic_engine():
    """
    While testing we want to use a "fresh" database based around postgres. We leverage
    the `testing.postgresql` package to create a temporary database cluster for each test.
    """
    with testing.postgresql.Postgresql() as pg:
        uri = pg.url()
        engine = sa.create_engine(uri)
        yield engine
        engine.dispose()

def init_db(postgresql: testing.postgresql.Postgresql):
    engine = sa.create_engine(postgresql.url())
    db_setup(engine)

def pytest_runtest_setup():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    # logging.getLogger("tests").setLevel(logging.DEBUG)
    # logging.getLogger("alembic").setLevel(logging.DEBUG)
    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    # logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)

def db_setup(engine: sa.Engine):
    schema_name: str = pycds.get_schema_name()
    test_user = "testuser"
    with engine.begin() as conn:
        for role, _ in get_standard_table_privileges():
            conn.execute(sa.text(f"CREATE ROLE {role};"))
        conn.execute(
            sa.text(f"CREATE ROLE {pycds.get_su_role_name()} WITH SUPERUSER NOINHERIT;")
        )
        conn.execute(sa.text(f"CREATE USER {test_user} WITH SUPERUSER NOINHERIT;;"))

        logging.basicConfig()
        logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)

        # print(f"### initial user {conn.execute('SELECT current_user').scalar()}")

        conn.execute(sa.text("CREATE EXTENSION postgis;"))
        conn.execute(sa.text("CREATE EXTENSION plpython3u;"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS citext;"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS hstore;"))

        # We need this function available, and it does not come pre-installed.
        conn.execute(
            sa.text(
                "CREATE OR REPLACE FUNCTION public.moddatetime() "
                "RETURNS trigger "
                "LANGUAGE 'c' "
                "COST 1 "
                "VOLATILE NOT LEAKPROOF "
                "AS '$libdir/moddatetime', 'moddatetime';"
            )
        )

        conn.execute(CreateSchema(schema_name))
        # schemas = conn.execute("select schema_name from information_schema.schemata").fetchall()
        # print(f"### schemas: {[x[0] for x in schemas]}")

        conn.execute(
            sa.text(f"GRANT ALL PRIVILEGES ON SCHEMA {schema_name} TO {test_user};")
        )

        privs = [
            f"GRANT ALL PRIVILEGES ON ALL {objects} IN SCHEMA {schema_name} TO {test_user};"
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema_name} GRANT ALL PRIVILEGES ON TABLES TO {test_user};"
            for objects in ("TABLES", "SEQUENCES", "FUNCTIONS")
        ]
        conn.execute(sa.text("".join(privs)))

        # One of the following *should* set the current user to `test_user`.
        # But it's hard to tell if it does, because `SELECT current_user`
        # *always* returns `postgres`, except when it is executed in the same
        # `conn.execute` operation as the `SET ROLE/AUTH` statement.
        # Subsequent `SELECT current_user` queries then return `postgres` again,
        # so it's very hard to tell what is actually happening.

        # conn.execute(f"SET ROLE '{test_user}';")
        conn.execute(sa.text(f"SET SESSION AUTHORIZATION '{test_user}';"))

        # result = conn.execute(f"SELECT current_user").scalar()
        #   --> "postgres"
        # result = conn.execute(f"SET SESSION AUTHORIZATION '{test_user}'; SELECT current_user").scalar()
        #   --> "testuser"
        # print(f'### final user {result}')

def seed_history_records(engine: sa.Engine):
    """
    Seed the database with Network, Station, and History records for all history_ids
    and joint_stations found in the test data CSV file.
    """
    csv_path = os.path.join(
        os.path.dirname(__file__),
        "data/composite_station_info/ppt_composite_station_file.csv"
    )
    
    history_ids = set()
    
    # Read all history_ids and joint_stations from the CSV
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Add the main history_id
            if row['history_id']:
                history_ids.add(int(row['history_id']))
            
            # Add all joint_stations from all three periods
            for period in ['1971', '1981', '1991']:
                for i in range(1, 4):  # joint_stations_X_1, joint_stations_X_2, joint_stations_X_3
                    col_name = f'joint_stations_{period}_{i}'
                    if col_name in row and row[col_name]:
                        history_ids.add(int(row[col_name]))
    
    # Create the records using raw SQL to set explicit history_id values
    with engine.begin() as conn:
        # Create a test network
        result = conn.execute(
            sa.text(
                "INSERT INTO crmp.meta_network (network_name) "
                "VALUES ('Test Network') RETURNING network_id"
            )
        )
        network_id = result.scalar()
        
        # Create a test station
        result = conn.execute(
            sa.text(
                "INSERT INTO crmp.meta_station (native_id, network_id) "
                "VALUES ('TEST001', :network_id) RETURNING station_id"
            ),
            {"network_id": network_id}
        )
        station_id = result.scalar()
        
        # Create history records for all unique history_ids
        for history_id in sorted(history_ids):
            conn.execute(
                sa.text(
                    "INSERT INTO crmp.meta_history (history_id, station_id, freq) "
                    "VALUES (:history_id, :station_id, 'daily')"
                ),
                {"history_id": history_id, "station_id": station_id}
            )

def split_on(sep:str) -> Callable[[str], List[str]]:
    def f(s:str) -> List[str]:
        if s == "":
            return []
        return re.split(rf"\s*{sep}\s*", s)

    return f

def parse_standard_table_privileges(stp:str) -> List[tuple[str, List[str]]]:
    """A standard table privilege spec is a string of the form
        role: priv, priv, priv, ...; role: priv, priv, priv, ...; ...
    The named role is granted the privileges listed after the colon following it.
    Privileges are separated by commas. A sequence of such role-priv items is separated
    by semicolons.
    """
    split_items = split_on(";")
    split_item = split_on(":")

    def split_privs(rp: List[str]) -> tuple[str, List[str]]:
        # Ensure rp has exactly two elements: [role, privs]
        if len(rp) != 2:
            raise ValueError(f"Invalid role-privilege pair: {rp}")
        return rp[0], split_on(",")(rp[1])

    return [split_privs(split_item(item)) for item in split_items(stp)]

def get_standard_table_privileges(
    default_privs: str="inspector: SELECT; viewer: SELECT; steward: ALL",
):
    """Get and parse an environment variable defining the standard privileges to be
    applied to new table-like) objects (tables, views, matviews.
    """
    env = os.environ.get("PYCDS_STANDARD_TABLE_PRIVS", default_privs)
    return parse_standard_table_privileges(env)


def get_schema_name():
    return os.environ.get("PYCDS_SCHEMA_NAME", "crmp")


def get_su_role_name():
    return os.environ.get("PYCDS_SU_ROLE_NAME", "pcicdba")
