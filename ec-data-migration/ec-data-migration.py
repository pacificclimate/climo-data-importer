crmp_url = "postgresql+psycopg2://crmp@/crmp?host=pg01.pcic.uvic.ca,pg02.pcic.uvic.ca&port=5432,5432&target_session_attrs=read-write&passfile=/workspaces/climo-data-importer/.pgpass"
# NOTE: metnorth_ro is read-only; update to a write-capable user before running the migration
metnorth_url = "postgresql+psycopg2://metnorth@/metnorth?host=pg01.pcic.uvic.ca,pg02.pcic.uvic.ca&port=5432,5432&target_session_attrs=read-write&passfile=/workspaces/climo-data-importer/.pgpass"

import logging
import io
import time
import pandas as pd
import sqlalchemy as sa
from pycds import Obs

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

crmp_engine = sa.create_engine(crmp_url, echo=False)
metnorth_engine = sa.create_engine(metnorth_url, echo=False)

CRMP_EC_RAW_NETWORK_ID = 19
NORTHERN_PROVINCES = ['NT', 'YT', 'NU']
BATCH_SIZE = 50000


def _copy_chunk(metnorth_conn, chunk: pd.DataFrame):
    """Bulk-load a chunk into obs_raw using the PostgreSQL COPY protocol."""
    buf = io.StringIO()
    chunk[["obs_time", "datum", "vars_id", "history_id"]].to_csv(
        buf, index=False, header=False, na_rep="\\N"
    )
    buf.seek(0)
    # Reach through SQLAlchemy's pool proxy to get the raw psycopg2 connection
    raw_conn = metnorth_conn.connection.driver_connection
    with raw_conn.cursor() as cur:
        cur.copy_expert(
            "COPY obs_raw (obs_time, datum, vars_id, history_id) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
            buf,
        )


def migrate_network(metnorth_conn) -> int:
    """Insert the EC_raw network into metnorth and return the new network_id."""
    with crmp_engine.connect() as crmp_conn:
        row = crmp_conn.execute(sa.text("""
            SELECT network_name, description, virtual, publish, col_hex, network_display_name
            FROM meta_network
            WHERE network_id = :nid
        """), {"nid": CRMP_EC_RAW_NETWORK_ID}).mappings().fetchone()

    if row is None:
        raise ValueError(f"Network {CRMP_EC_RAW_NETWORK_ID} not found in crmp")

    result = metnorth_conn.execute(sa.text("""
        INSERT INTO meta_network (network_name, description, virtual, publish, col_hex, network_display_name)
        VALUES (:network_name, :description, :virtual, :publish, :col_hex, :network_display_name)
        RETURNING network_id
    """), {**row})

    network_id = result.fetchone()[0]
    logger.info(f"Inserted EC_raw network → metnorth network_id={network_id}")
    return network_id


def migrate_vars(metnorth_conn, metnorth_network_id: int) -> dict:
    """Insert meta_vars for EC_raw into metnorth. Returns crmp vars_id -> metnorth vars_id map."""
    vars_df = pd.read_sql(sa.text("""
        SELECT vars_id, unit, precision, standard_name, cell_method,
               long_description, net_var_name, display_name, short_name
        FROM meta_vars
        WHERE network_id = :nid
    """), crmp_engine, params={"nid": CRMP_EC_RAW_NETWORK_ID})

    vars_id_map = {}
    for _, row in vars_df.iterrows():
        # Handle null items as None instead of NaN or NaT for SQL insertion
        record = {k: None if pd.isna(v) else v for k, v in row.items()}
        crmp_vars_id = int(record.pop("vars_id"))
        record["network_id"] = metnorth_network_id
        result = metnorth_conn.execute(sa.text("""
            INSERT INTO meta_vars (network_id, unit, precision, standard_name, cell_method,
                                   long_description, net_var_name, display_name, short_name)
            VALUES (:network_id, :unit, :precision, :standard_name, :cell_method,
                    :long_description, :net_var_name, :display_name, :short_name)
            RETURNING vars_id
        """), record)
        vars_id_map[crmp_vars_id] = result.fetchone()[0]

    logger.info(f"Inserted {len(vars_id_map)} variable(s)")
    return vars_id_map


def migrate_stations(metnorth_conn, metnorth_network_id: int) -> dict:
    """Insert meta_station rows for NT/YT/NU stations into metnorth.
    Returns crmp station_id -> metnorth station_id map.
    """
    stations_df = pd.read_sql(sa.text("""
        SELECT DISTINCT s.station_id, s.native_id, s.min_obs_time, s.max_obs_time, s.publish
        FROM meta_station s
        JOIN meta_history h ON s.station_id = h.station_id
        WHERE s.network_id = :nid
          AND h.province = ANY(:provinces)
    """), crmp_engine, params={"nid": CRMP_EC_RAW_NETWORK_ID, "provinces": NORTHERN_PROVINCES})

    station_id_map = {}
    for _, row in stations_df.iterrows():
        # Handle null items as None instead of NaN or NaT for SQL insertion
        record = {k: None if pd.isna(v) else v for k, v in row.items()}
        crmp_station_id = int(record.pop("station_id"))
        record["network_id"] = metnorth_network_id
        result = metnorth_conn.execute(sa.text("""
            INSERT INTO meta_station (network_id, native_id, min_obs_time, max_obs_time, publish)
            VALUES (:network_id, :native_id, :min_obs_time, :max_obs_time, :publish)
            RETURNING station_id
        """), record)
        station_id_map[crmp_station_id] = result.fetchone()[0]

    logger.info(f"Inserted {len(station_id_map)} station(s)")
    return station_id_map


def migrate_histories(metnorth_conn, station_id_map: dict) -> dict:
    """Insert meta_history rows for the migrated stations into metnorth.
    Returns crmp history_id -> metnorth history_id map.

    Notes:
    - the_geom is read as EWKT and re-inserted via ST_GeomFromEWKT.
    - freq is a custom timescale type; cast explicitly on insert.
    - sensor_id references meta_sensor which may differ between databases; set to NULL.
    """
    crmp_station_ids = list(station_id_map.keys())
    histories_df = pd.read_sql(sa.text("""
        SELECT history_id, station_id, station_name, lon, lat, elev,
               sdate, edate, tz_offset, province, country, comments,
               ST_AsEWKT(the_geom) AS the_geom, freq
        FROM meta_history
        WHERE station_id = ANY(:station_ids)
          AND province = ANY(:provinces)
    """), crmp_engine, params={"station_ids": crmp_station_ids, "provinces": NORTHERN_PROVINCES})

    history_id_map = {}
    for _, row in histories_df.iterrows():
        # Handle null items as None instead of NaN or NaT for SQL insertion
        record = {k: None if pd.isna(v) else v for k, v in row.items()}
        crmp_history_id = int(record.pop("history_id"))
        record["station_id"] = station_id_map[int(record["station_id"])]
        result = metnorth_conn.execute(sa.text("""
            INSERT INTO meta_history (station_id, station_name, lon, lat, elev,
                                      sdate, edate, tz_offset, province, country, comments,
                                      the_geom, freq)
            VALUES (:station_id, :station_name, :lon, :lat, :elev,
                    :sdate, :edate, :tz_offset, :province, :country, :comments,
                    ST_GeomFromEWKT(:the_geom), :freq)
            RETURNING history_id
        """), record)
        history_id_map[crmp_history_id] = result.fetchone()[0]

    logger.info(f"Inserted {len(history_id_map)} history record(s)")
    return history_id_map


def migrate_obs_raw(metnorth_conn, vars_id_map: dict, history_id_map: dict):
    """Stream obs_raw from crmp per history_id and insert into metnorth."""
    crmp_vars_ids = list(vars_id_map.keys())
    total_inserted = 0
    migration_start = time.monotonic()

    query = sa.text("""
        SELECT obs_time, datum, vars_id, history_id
        FROM obs_raw
        WHERE history_id = :history_id
          AND vars_id = ANY(:vars_ids)
    """)

    count_query = sa.text("""
        SELECT COUNT(*)
        FROM obs_raw
        WHERE history_id = :history_id
          AND vars_id = ANY(:vars_ids)
    """)

    with crmp_engine.connect() as crmp_conn:
        for i, (crmp_history_id, metnorth_history_id) in enumerate(history_id_map.items(), start=1):
            total_rows = crmp_conn.execute(
                count_query.bindparams(history_id=crmp_history_id, vars_ids=crmp_vars_ids)
            ).scalar()
            logger.info(
                f"History {i}/{len(history_id_map)} (crmp={crmp_history_id}): "
                f"{total_rows} rows to migrate"
            )

            result = crmp_conn.execution_options(
                stream_results=True, max_row_buffer=BATCH_SIZE
            ).execute(query.bindparams(history_id=crmp_history_id, vars_ids=crmp_vars_ids))

            history_inserted = 0
            chunk_num = 0
            history_start = time.monotonic()
            try:
                while True:
                    rows = result.fetchmany(BATCH_SIZE)
                    if not rows:
                        break
                    chunk_num += 1
                    chunk_start = time.monotonic()
                    chunk = pd.DataFrame(rows, columns=list(result.keys()))
                    chunk["history_id"] = metnorth_history_id
                    chunk["vars_id"] = chunk["vars_id"].map(vars_id_map).astype(int)

                    _copy_chunk(metnorth_conn, chunk[["obs_time", "datum", "vars_id", "history_id"]])
                    history_inserted += len(chunk)
                    chunk_elapsed = time.monotonic() - chunk_start
                    logger.info(
                        f"  → {history_inserted}/{total_rows} rows inserted for crmp history {crmp_history_id}"
                        f" ({len(chunk) / chunk_elapsed:,.0f} rows/s this chunk)"
                    )
            finally:
                result.close()

            total_inserted += history_inserted
            history_elapsed = time.monotonic() - history_start
            rate = history_inserted / history_elapsed if history_elapsed > 0 else 0
            logger.info(
                f"History {i}/{len(history_id_map)} (crmp={crmp_history_id}): "
                f"{history_inserted} rows in {history_elapsed:.1f}s ({rate:,.0f} rows/s)"
                f" — running total: {total_inserted}"
            )

    total_elapsed = time.monotonic() - migration_start
    overall_rate = total_inserted / total_elapsed if total_elapsed > 0 else 0
    logger.info(
        f"obs_raw migration complete — {total_inserted} rows in {total_elapsed:.1f}s "
        f"({overall_rate:,.0f} rows/s overall)"
    )


def migrate():
    """Run the full migration: network → vars → stations → histories → obs_raw."""
    with metnorth_engine.connect() as metnorth_conn:
        try:
            metnorth_network_id = migrate_network(metnorth_conn)
            vars_id_map = migrate_vars(metnorth_conn, metnorth_network_id)
            station_id_map = migrate_stations(metnorth_conn, metnorth_network_id)
            history_id_map = migrate_histories(metnorth_conn, station_id_map)
            migrate_obs_raw(metnorth_conn, vars_id_map, history_id_map)
            metnorth_conn.commit()
            logger.info("Migration completed successfully")
        except Exception:
            metnorth_conn.rollback()
            logger.exception("Migration failed — all changes rolled back")
            raise


if __name__ == "__main__":
    migrate()

