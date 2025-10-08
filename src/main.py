import csv
import os
from pycds import ClimatologicalPeriod, ClimatologicalStation, ClimatologicalStationXHistory, ClimatologicalValue, ClimatologicalVariable # type: ignore
import sqlalchemy as sa
# start by reading files
from typing import List, Dict, Optional


from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine

engine = sa.create_engine("sqlite:///data/stations.db", echo=False)
session = Session(engine)


# csv file targets, configurable via environment variable
basedir = os.getenv("CLIMO_DATA_DIR", "/data/")
composite_station_info_dir = f"{basedir}composite_station_info/"

ppt_fill = "ppt"
tmax_fill = "tmax"
tmin_fill = "tmin"

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
    stations: List[HistoryLine] = []
    with open(station_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            stations.append(HistoryLine(row))
    return stations

def read_data_file(variable: str, climatology_period: str, station_id: str) -> List[StationDataLine]:
    """ Read the data file for a given variable (ppt, tmax, tmin), climatology period (1971_2000, 1981_2010, 1991_2020)
    and return a list of StationDataLine objects.
    """
    data_file = data_location_template.format(variable, climatology_period, station_id)
    data: List[StationDataLine] = []
    with open(data_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(StationDataLine(row))
    return data

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
    periods = [
        ClimatologicalPeriod(start_date="1971-01-01", end_date="2000-12-31"),
        ClimatologicalPeriod(start_date="1981-01-01", end_date="2010-12-31"),
        ClimatologicalPeriod(start_date="1991-01-01", end_date="2020-12-31"),
    ]
    session.add_all(periods)
    session.commit()

def generate_climatological_variables(session: Session) -> None:
    """ Generate the climatological variables in the database. """
    variables = [
        ClimatologicalVariable(
            duration="monthly",
            unit="mm",
            standard_name="ppt",
            display_name="Precipitation",
            short_name="ppt",
            cell_methods="time: sum",
            net_var_name="ppt"
        ),
        ClimatologicalVariable(
            duration="monthly",
            unit="degC",
            standard_name="tmax",
            display_name="Maximum Temperature",
            short_name="tmax",
            cell_methods="time: maximum",
            net_var_name="tmax"
        ),
        ClimatologicalVariable(
            duration="monthly",
            unit="degC",
            standard_name="tmin",
            display_name="Minimum Temperature",
            short_name="tmin",
            cell_methods="time: minimum",
            net_var_name="tmin"
        ),
    ]
    session.add_all(variables)
    session.commit()

def generate_station(session: Session, history_line: HistoryLine, climo_period_id: int):
    station = ClimatologicalStation(
        type="composite", # One of long-record, composite, prism 
        basin_id=history_line.basin,
        comments="",
        climo_period_id=climo_period_id
    )
    session.add(station)
    session.commit()

    return station

def generate_base_station_history(session: Session, station_id: int, history_id: int):
    base_station = ClimatologicalStationXHistory(
        climo_station_id=station_id,
        history_id=history_id,
        role="base"  # Composite stations reference their base history
    )
    session.add(base_station)
    session.commit()

def generate_station_histories(session: Session, station_id: int, joint_stations: List[int | None]):
    
    for joint_id in joint_stations:
        # Skip None values - some joint_stations columns may be empty
        if joint_id is None:
            continue
            
        joint_station = ClimatologicalStationXHistory(
            climo_station_id=station_id,
            history_id=joint_id,
            role="joint"  # Composite stations reference joint stations
        )
        session.add(joint_station)
    
    session.commit()

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
    # Get the variable ID
    climo_var = session.query(ClimatologicalVariable).filter_by(
        net_var_name=variable
    ).first()
    if climo_var is None:
        raise ValueError(f"Variable {variable} not found")
    
    # Read data lines from CSV (should be 12 monthly values)
    data_lines = read_data_file(variable, period, history_id)
    
    # Each data line corresponds to a month, match with monthlyyears
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
    session.commit()


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
    # Get period IDs
    period_1971_id = get_period_id_by_dates(session, "1971-01-01", "2000-12-31")
    period_1981_id = get_period_id_by_dates(session, "1981-01-01", "2010-12-31")
    period_1991_id = get_period_id_by_dates(session, "1991-01-01", "2020-12-31")
    
    history_lines = read_station_info_file(variable)
    for line in history_lines:
        # create a station for each period we have data for
        if line.has_1971_data:
            station = generate_station(session, line, period_1971_id)
            generate_base_station_history(session, station.id, line.history_id)
            generate_station_histories(session, station.id, line.joint_stations_1971)
            generate_value_data(session, variable, "1971_2000", station.id, str(line.history_id), line.monthlyyears_1971)

        if line.has_1981_data:
            station = generate_station(session, line, period_1981_id)
            generate_base_station_history(session, station.id, line.history_id)
            generate_station_histories(session, station.id, line.joint_stations_1981)
            generate_value_data(session, variable, "1981_2010", station.id, str(line.history_id), line.monthlyyears_1981)
        
        if line.has_1991_data:
            station = generate_station(session, line, period_1991_id)
            generate_base_station_history(session, station.id, line.history_id)
            generate_station_histories(session, station.id, line.joint_stations_1991)
            generate_value_data(session, variable, "1991_2020", station.id, str(line.history_id), line.monthlyyears_1991)

def main(custom_session: Optional[Session] = None, custom_engine: Optional[Engine] = None) -> None:
    # Use provided session/engine or fall back to module-level defaults
    sess = custom_session if custom_session is not None else session
    
    # generate periods and variables
    generate_climatological_periods(sess)
    generate_climatological_variables(sess)

    # generate stations and data for each variable
    for variable in [ppt_fill, tmax_fill, tmin_fill]:
        generate_climatological_stations(sess, variable)