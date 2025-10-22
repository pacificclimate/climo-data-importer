import csv
import logging
import os
from pycds import ClimatologicalPeriod, ClimatologicalStation, ClimatologicalStationXHistory, ClimatologicalValue, ClimatologicalVariable # type: ignore
import sqlalchemy as sa
# start by reading files
from typing import List, Dict, Optional


from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)



# csv file targets, configurable via environment variable
basedir = os.getenv("CLIMO_DATA_DIR", "/data/")
composite_station_info_dir = f"{basedir}composite_station_info/"

ppt_fill = "ppt"
tmax_fill = "tmax"
tmin_fill = "tmin"

var_map = {
    ppt_fill: "Precip_Climatology",
    tmax_fill: "Tx_Climatology",
    tmin_fill: "Tn_Climatology"
}

# station info files
# {0} = variable (ppt, tmax, tmin)
station_info_template = f"{composite_station_info_dir}{{0}}_composite_station_file.csv"

# data files
data_dir = f"{basedir}csv/"

clim_1971_2000 = "1971_2000"
clim_1981_2010 = "1981_2010"
clim_1991_2020 = "1991_2020"

# {0} = variable (ppt, tmax, tmin)
# {1} = climatology period (1971_2000, 1981_2010, 1991_2020)
# {2} = station identifier (station_id)
data_location_template = f"{data_dir}{{0}}/{{1}}/{{2}}_{{0}}_{{1}}.csv"

ppt_data = data_location_template.format(ppt_fill, clim_1971_2000, "station_id")

# Deserialize line based on:
# history_id	lat	lon	elev	basin	monthlyyears_1971_1	monthlyyears_1971_2	monthlyyears_1971_3	monthlyyears_1971_4	monthlyyears_1971_5	monthlyyears_1971_6	monthlyyears_1971_7	monthlyyears_1971_8	monthlyyears_1971_9	monthlyyears_1971_10	monthlyyears_1971_11	monthlyyears_1971_12	joint_stations_1971_1	joint_stations_1971_2	joint_stations_1971_3	monthlyyears_1981_1	monthlyyears_1981_2	monthlyyears_1981_3	monthlyyears_1981_4	monthlyyears_1981_5	monthlyyears_1981_6	monthlyyears_1981_7	monthlyyears_1981_8	monthlyyears_1981_9	monthlyyears_1981_10	monthlyyears_1981_11	monthlyyears_1981_12	joint_stations_1981_1	joint_stations_1981_2	joint_stations_1981_3	monthlyyears_1991_1	monthlyyears_1991_2	monthlyyears_1991_3	monthlyyears_1991_4	monthlyyears_1991_5	monthlyyears_1991_6	monthlyyears_1991_7	monthlyyears_1991_8	monthlyyears_1991_9	monthlyyears_1991_10	monthlyyears_1991_11	monthlyyears_1991_12	joint_stations_1991_1	joint_stations_1991_2	joint_stations_1991_3

class HistoryLine():
    """ Represents a line from the composite station info file. """
    def __init__(self, line: Dict[str, str]):
        self.history_id: int = int(line['history_id'])
        self.lat: float = float(line['lat'])
        self.lon: float = float(line['lon'])
        self.elev: float = float(line['elev'])
        # basin isn't always available, it'll an int id or "NaN"
        basin_val = line['basin']
        self.basin: int | None = None if basin_val == "NaN" or basin_val == "" else int(basin_val)
        # some of these are empty strings, but we should have a complete set or nothing
        self.monthlyyears_1971: list[int | None] = [int(line[f'monthlyyears_1971_{i}']) if line[f'monthlyyears_1971_{i}'] not in ["NaN", ""] else None for i in range(1, 13)]
        self.joint_stations_1971: list[int | None] = [int(line[f'joint_stations_1971_{i}']) if line[f'joint_stations_1971_{i}'] not in ["NaN", ""] else None for i in range(1, 4)]
        self.monthlyyears_1981: list[int | None] = [int(line[f'monthlyyears_1981_{i}']) if line[f'monthlyyears_1981_{i}'] not in ["NaN", ""] else None for i in range(1, 13)]
        self.joint_stations_1981: list[int | None] = [int(line[f'joint_stations_1981_{i}']) if line[f'joint_stations_1981_{i}'] not in ["NaN", ""] else None for i in range(1, 4)]
        self.monthlyyears_1991: list[int | None] = [int(line[f'monthlyyears_1991_{i}']) if line[f'monthlyyears_1991_{i}'] not in ["NaN", ""] else None for i in range(1, 13)]
        self.joint_stations_1991: list[int | None] = [int(line[f'joint_stations_1991_{i}']) if line[f'joint_stations_1991_{i}'] not in ["NaN", ""] else None for i in range(1, 4)]

        # we can do some quick existence checks based on if we have data available for each period
        self.has_1971_data = all(self.monthlyyears_1971)
        self.has_1981_data = all(self.monthlyyears_1981)
        self.has_1991_data = all(self.monthlyyears_1991)

    # helper to print a line when printing
    def __repr__(self):
        return f"HistoryLine(history_id={self.history_id}, lat={self.lat}, lon={self.lon}, elev={self.elev}, basin={self.basin}, has_1971_data={self.has_1971_data}, has_1981_data={self.has_1981_data}, has_1991_data={self.has_1991_data})"
        

# Deserialize line based on:
# obs_time	datum
class StationDataLine():
    """ Represents a line from the station data file. """
    def __init__(self, line: Dict[str, str]):
        self.obs_time = line['obs_time']
        self.datum = float(line['datum'])

    def __repr__(self):
        return f"StationDataLine(obs_time={self.obs_time}, datum={self.datum})"


## Utility functions to read files

def read_station_info_file(variable: str) -> List[HistoryLine]:
    """ Read the station info file for a given variable (ppt, tmax, tmin) 
    and return a list of HistoryLine objects.
    """
    station_file = station_info_template.format(variable)
    logger.info(f"Reading station info file for variable '{variable}': {station_file}")
    
    stations: List[HistoryLine] = []
    try:
        with open(station_file, 'r') as f:
            reader = csv.DictReader(f)
            row_count = 0
            for row in reader:
                stations.append(HistoryLine(row))
                row_count += 1
        
        logger.info(f"Successfully read {row_count} station records for variable '{variable}'")
        return stations
    except FileNotFoundError:
        logger.error(f"Station info file not found: {station_file}")
        raise
    except Exception as e:
        logger.error(f"Error reading station info file {station_file}: {e}")
        raise

def read_data_file(variable: str, climatology_period: str, station_id: str) -> List[StationDataLine]:
    """ Read the data file for a given variable (ppt, tmax, tmin), climatology period (1971_2000, 1981_2010, 1991_2020)
    and return a list of StationDataLine objects.
    """
    data_file = data_location_template.format(variable, climatology_period, station_id)
    logger.debug(f"Reading data file for station {station_id}, variable '{variable}', period '{climatology_period}': {data_file}")
    
    data: List[StationDataLine] = []
    try:
        with open(data_file, 'r') as f:
            reader = csv.DictReader(f)
            row_count = 0
            for row in reader:
                data.append(StationDataLine(row))
                row_count += 1
        
        logger.debug(f"Successfully read {row_count} data points for station {station_id} ({variable}, {climatology_period})")
        return data
    except FileNotFoundError:
        logger.warning(f"Data file not found: {data_file} (station {station_id}, {variable}, {climatology_period})")
        raise
    except Exception as e:
        logger.error(f"Error reading data file {data_file}: {e}")
        raise

# Now we can start filling in the database

# tables will need to be filled in this order due to foreign key constraints
# ClimatologicalPeriod: Taken from our 3 periods, 1971_2000, 1981_2010, 1991_2020
# ClimatologicalVariable: Taken from our 3 variables, ppt, tmax, tmin
# ClimatologicalStation: One per unique history line, combined with each period
# ClimatologicalStationXHistory: For each station above, this will record the joint stations. 
#   Each station will have up to 3 joint stations, histories will have to pre-exist in the database
# ClimatologicalValue: The actual data values, linked to station, variable

def generate_climatological_periods(session: Session) -> None:
    """ Generate the climatological periods in the database. """
    logger.info("Creating climatological periods in database...")
    
    periods = [
        ClimatologicalPeriod(start_date="1971-01-01", end_date="2000-12-31"),
        ClimatologicalPeriod(start_date="1981-01-01", end_date="2010-12-31"),
        ClimatologicalPeriod(start_date="1991-01-01", end_date="2020-12-31"),
    ]
    
    session.add_all(periods)
    session.flush()  # Flush to ensure IDs are available for foreign keys
    
    logger.info(f"Successfully created {len(periods)} climatological periods")

def generate_climatological_variables(session: Session) -> None:
    """ Generate the climatological variables in database. """
    logger.info("Creating climatological variables in database...")
    
    variables = [
        ClimatologicalVariable(
            duration="monthly",
            unit="mm",
            standard_name="lwe_thickness_of_precipitation_amount",
            display_name="Precipitation Climatology",
            short_name="lwe_thickness_of_precipitation_amount t: sum within months t: mean over years",
            cell_methods="t: sum within months t: mean over years",
            net_var_name="Precip_Climatology"
        ),
        ClimatologicalVariable(
            duration="monthly",
            unit="celsius",
            standard_name="air_temperature",
            display_name="Temperature Climatology (Max.)",
            short_name="air_temperature t: maximum within days t: mean within months t: mean over years",
            cell_methods="t: maximum within days t: mean within months t: mean over years",
            net_var_name="Tx_Climatology"
        ),
        ClimatologicalVariable(
            duration="monthly",
            unit="celsius",
            standard_name="air_temperature",
            display_name="Temperature Climatology (Min.)",
            short_name="air_temperature t: minimum within days t: mean within months t: mean over years",
            cell_methods="t: minimum within days t: mean within months t: mean over years",
            net_var_name="Tn_Climatology"
        ),

        # This isn't used for our imported data, but exists as part of the existing PCIC climatology
        # values in obs_raw. Create it so it makes our lives easier later.
        ClimatologicalVariable(
            duration="monthly",
            unit="celsius",
            standard_name="air_temperature",
            display_name="Temperature Climatology (Mean)",
            short_name="air_temperature t: mean within days t: mean within months t: mean over years",
            cell_methods="t: mean within days t: mean within months t: mean over years",
            net_var_name="T_mean_Climatology"
        ),
    ]
    
    session.add_all(variables)
    session.flush()  # Flush to ensure IDs are available for foreign keys
    
    logger.info(f"Successfully created {len(variables)} climatological variables: {[v.net_var_name for v in variables]}")

def get_joint_stations_for_period(session: Session, history_line: HistoryLine, climo_period_id: int):
    """Get the appropriate joint stations list for the given climatological period."""
    # Get the period dates to determine which joint stations to use
    period = session.query(ClimatologicalPeriod).filter_by(id=climo_period_id).first()
    if period is None:
        raise ValueError(f"Period ID {climo_period_id} not found")
    
    # Map period dates to the appropriate joint stations list
    start_date_str = str(period.start_date)
    end_date_str = str(period.end_date)
    
    if start_date_str == "1971-01-01" and end_date_str == "2000-12-31":
        return history_line.joint_stations_1971
    elif start_date_str == "1981-01-01" and end_date_str == "2010-12-31":
        return history_line.joint_stations_1981
    elif start_date_str == "1991-01-01" and end_date_str == "2020-12-31":
        return history_line.joint_stations_1991
    else:
        raise ValueError(f"Unknown period: {start_date_str} to {end_date_str}")

def generate_station(session: Session, history_line: HistoryLine, climo_period_id: int):
    logger.debug(f"Creating climatological station for history_id {history_line.history_id}, period_id {climo_period_id}")
    
    # Get the joint stations for this specific period
    joint_stations = get_joint_stations_for_period(session, history_line, climo_period_id)
    
    # Composite if we use any joint stations for this specific period
    station = ClimatologicalStation(
        type= "composite" if any(joint_stations) else "long-record", 
        basin_id=history_line.basin,
        comments="",
        climo_period_id=climo_period_id
    )
    session.add(station)
    session.flush()  # Flush to get the ID without committing

    logger.debug(f"Created climatological station with ID {station.id} for history_id {history_line.history_id}")
    return station

def generate_base_station_history(session: Session, station_id: int, history_id: int):
    logger.debug(f"Creating base station history link: station_id {station_id} -> history_id {history_id}")
    
    base_station = ClimatologicalStationXHistory(
        climo_station_id=station_id,
        history_id=history_id,
        role="base"  # Composite stations reference their base history
    )
    session.add(base_station)

def generate_station_histories(session: Session, station_id: int, joint_stations: List[int | None]):
    joint_count = 0
    for joint_id in joint_stations:
        # Skip None values - some joint_stations columns may be empty
        if joint_id is None:
            continue
            
        logger.debug(f"Creating joint station history link: station_id {station_id} -> joint_history_id {joint_id}")
        joint_station = ClimatologicalStationXHistory(
            climo_station_id=station_id,
            history_id=joint_id,
            role="joint"  # Composite stations reference joint stations
        )
        session.add(joint_station)
        joint_count += 1
    
    if joint_count > 0:
        logger.debug(f"Created {joint_count} joint station history links for station_id {station_id}")

def generate_value_data(session: Session, variable: str, period: str, station_id: int, history_id: str, monthlyyears: list[int | None]):
    """Generate climatological value data for a station from CSV files.
    
    Args:
        session: Database session
        variable: Variable name (ppt, tmax, tmin)
        period: Period string (e.g., "1971_2000")
        station_id: Climatological station ID
        history_id: History ID for reading the data file
        monthlyyears: List of 12 values indicating contributing years for each month
    """
    logger.debug(f"Processing value data for station_id {station_id}, variable '{variable}', period '{period}', history_id {history_id}")
    
    # Get the variable ID
    climo_var = session.query(ClimatologicalVariable).filter_by(
        net_var_name=var_map[variable]
    ).first()
    if climo_var is None:
        logger.error(f"Variable {variable} not found in database")
        raise ValueError(f"Variable {variable} not found")
    
    # Read data lines from CSV (should be 12 monthly values)
    try:
        data_lines = read_data_file(variable, period, history_id)
        logger.debug(f"Read {len(data_lines)} data lines for station {history_id}")
    except Exception as e:
        logger.error(f"Failed to read data file for station {history_id}, variable {variable}, period {period}: {e}")
        raise
    
    # Each data line corresponds to a month, match with monthlyyears
    values_added = 0
    for idx, data_line in enumerate(data_lines):
        # Get the number of contributing years for this month
        num_years = monthlyyears[idx] if idx < len(monthlyyears) and monthlyyears[idx] is not None else 0
        
        value = ClimatologicalValue(
            climo_station_id=station_id,
            climo_variable_id=climo_var.id,
            value_time=data_line.obs_time,
            value=data_line.datum,
            num_contributing_years=num_years
        )
        session.add(value)
        values_added += 1
    
    logger.debug(f"Successfully added {values_added} climatological values for station_id {station_id} ({variable}, {period})")


def get_period_id_by_dates(session: Session, start_date: str, end_date: str):
    """Get the period ID for a given date range."""
    period = session.query(ClimatologicalPeriod).filter_by(
        start_date=start_date,
        end_date=end_date
    ).first()
    if period is None:
        raise ValueError(f"Period {start_date} to {end_date} not found")
    return period.id

def generate_climatological_stations(session: Session, variable: str) -> None:
    """ Generate the climatological stations in the database for a given variable. """
    logger.info(f"Starting climatological station generation for variable '{variable}'")
    
    # Get period IDs
    period_1971_id = get_period_id_by_dates(session, "1971-01-01", "2000-12-31")
    period_1981_id = get_period_id_by_dates(session, "1981-01-01", "2010-12-31")
    period_1991_id = get_period_id_by_dates(session, "1991-01-01", "2020-12-31")
    
    logger.info(f"Period IDs: 1971-2000={period_1971_id}, 1981-2010={period_1981_id}, 1991-2020={period_1991_id}")
    
    history_lines = read_station_info_file(variable)
    
    # Track statistics
    stations_1971 = 0
    stations_1981 = 0
    stations_1991 = 0
    total_processed = 0
    
    logger.info(f"Processing {len(history_lines)} history lines for variable '{variable}'")
    
    for idx, line in enumerate(history_lines, 1):
        logger.debug(f"Processing history line {idx}/{len(history_lines)}: history_id {line.history_id}")
        
        # create a station for each period we have data for
        if line.has_1971_data:
            logger.debug(f"Creating 1971-2000 station for history_id {line.history_id}")
            station = generate_station(session, line, period_1971_id)
            generate_base_station_history(session, station.id, line.history_id)
            generate_station_histories(session, station.id, line.joint_stations_1971)
            generate_value_data(session, variable, "1971_2000", station.id, str(line.history_id), line.monthlyyears_1971)
            stations_1971 += 1

        if line.has_1981_data:
            logger.debug(f"Creating 1981-2010 station for history_id {line.history_id}")
            station = generate_station(session, line, period_1981_id)
            generate_base_station_history(session, station.id, line.history_id)
            generate_station_histories(session, station.id, line.joint_stations_1981)
            generate_value_data(session, variable, "1981_2010", station.id, str(line.history_id), line.monthlyyears_1981)
            stations_1981 += 1
        
        if line.has_1991_data:
            logger.debug(f"Creating 1991-2020 station for history_id {line.history_id}")
            station = generate_station(session, line, period_1991_id)
            generate_base_station_history(session, station.id, line.history_id)
            generate_station_histories(session, station.id, line.joint_stations_1991)
            generate_value_data(session, variable, "1991_2020", station.id, str(line.history_id), line.monthlyyears_1991)
            stations_1991 += 1
            
        total_processed += 1

        # Log progress every 100 stations
        if idx % 100 == 0:
            logger.info(f"Processed {idx}/{len(history_lines)} history lines for variable '{variable}'")
    
    logger.info(f"Completed climatological station generation for variable '{variable}': "
                f"{stations_1971} stations (1971-2000), {stations_1981} stations (1981-2010), "
                f"{stations_1991} stations (1991-2020), {total_processed} total history lines processed")

def main(session: Optional[Session] = None) -> None:
    # Use provided session - it must be provided
    if session is None:
        raise ValueError("A database session must be provided")
    
    logger.info("=" * 60)
    logger.info("Starting climatological data import process")
    logger.info("=" * 60)
    
    # generate periods and variables
    logger.info("Phase 1/2: Setting up database structure...")
    generate_climatological_periods(session)
    generate_climatological_variables(session)
    logger.info("Phase 1/2: Database structure setup completed")

    # generate stations and data for each variable
    variables = [ppt_fill, tmax_fill, tmin_fill]
    logger.info(f"Phase 2/2: Processing data for {len(variables)} variables: {variables}")
    
    for idx, variable in enumerate(variables, 1):
        logger.info(f"Processing variable {idx}/{len(variables)}: '{variable}'")
        try:
            generate_climatological_stations(session, variable)
            logger.info(f"Successfully completed processing for variable '{variable}'")
        except Exception as e:
            logger.error(f"Failed to process variable '{variable}': {e}")
            raise
    
    # Commit all changes in one transaction
    session.commit()
    logger.info("All changes committed successfully")
    
    logger.info("=" * 60)
    logger.info("Climatological data import process completed successfully")
    logger.info("=" * 60)

if __name__ == "__main__":
    logger.info("Initializing database connection...")
    engine = sa.create_engine("postgresql://crmp@dbtest04.pcic.uvic.ca/crmp", echo=False)
    session = Session(engine)
    logger.info("Database connection established")
    
    try:
        main(session=session)
    except Exception as e:
        logger.error(f"Import process failed: {e}")
        raise
    finally:
        session.close()
        logger.info("Database session closed")