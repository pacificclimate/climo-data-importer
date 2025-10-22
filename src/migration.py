# Migration script for the climatological database. There are a number of climatologies in obs raw 
# that we want to centralize on the new structure, this script helps us do that.


# tables will need to be filled in this order due to foreign key constraints (as with the excel insertions)
# ClimatologicalPeriod: Taken from our 3 periods, 1971_2000, 1981_2010, 1991_2020
# ClimatologicalVariable: Taken from our 3 variables, ppt, tmax, tmin
# ClimatologicalStation: One per unique history line, combined with each period
# ClimatologicalStationXHistory: For each station above, this will record the joint stations. 
#   Each station will have up to 3 joint stations, histories will have to pre-exist in the database
# ClimatologicalValue: The actual data values, linked to station, variable
    
# ClimatologicalPeriod: Existing climatologies are only available for the 1971-2000 period.
# This script will be run after the initial insert from the CSV files, so should be populated. Start by grabbing its value.

from datetime import datetime
import sqlalchemy as sa
from sqlalchemy.orm import Session
from pycds import ClimatologicalPeriod, ClimatologicalStation, ClimoObsCount, Obs, Variable
from main import get_period_id_by_dates
    
engine = sa.create_engine("postgresql://crmp@dbtest04.pcic.uvic.ca/crmp", echo=False)
session = Session(engine)

climatology_period_1971_2000 = get_period_id_by_dates(session, "1971-01-01", "2000-12-31")
# We can use the ClimoObsCount to find all histories that have climo data

histories = session.query(ClimoObsCount).all()

# looping through each history we can grab its relevant variables

def generate_base_station(session: Session, history_id: int, period_id: int):
    station = ClimatologicalStation(
        type="prism", # One of long-record, composite, prism 
        basin_id=history_line.basin,
        comments="",
        climo_period_id=climo_period_id
    )
    session.add(station)

def generate_pcic_climo_stations(session: Session, history_id: int, period_id: int):
    generate_base_station(session, history_id, period_id)

for history in histories:
    generate_pcic_climo_stations(session, history.history_id, climatology_period_1971_2000)