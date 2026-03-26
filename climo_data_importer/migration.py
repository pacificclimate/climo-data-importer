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
import logging
import sqlalchemy as sa
from sqlalchemy.orm import Session
from pycds import ClimatologicalPeriod, ClimatologicalStation, ClimatologicalStationXHistory, ClimatologicalValue, ClimatologicalVariable, ClimoObsCount, Obs, Variable
from src.main import get_period_id_by_dates
    
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def run_migration(session: Session | None = None):
    """Run migration with optional session for testing"""
    if session is None:
        engine = sa.create_engine("postgresql://crmp@dbtest04.pcic.uvic.ca/crmp", echo=False)
        session = Session(engine)
        should_close_session = True
    else:
        should_close_session = False
    
    try:
        climatology_period_1971_2000 = get_period_id_by_dates(session, "1971-01-01", "2000-12-31")
        # We can use the ClimoObsCount to find all histories that have climo data
        histories = session.query(ClimoObsCount).all()
        
        logger.info(f"Found {len(histories)} histories with climatological data")
        
        # looping through each history we can grab its relevant variables
        for history in histories:
            # history is an instance, so we can access attributes directly
            history_id = history.history_id  # type: ignore # SQLAlchemy instance attribute
            logger.debug(f"Processing history_id: {history_id}")
            
            station = generate_base_station(session, history_id, climatology_period_1971_2000)  # type: ignore
            # prism stations will only have a base history
            generate_base_station_history(session, station.id, history_id)  # type: ignore # SQLAlchemy instance attribute
            generate_value_data(session, station.id, history_id)  # type: ignore # SQLAlchemy instance attribute

        # Commit all changes
        session.commit()
        logger.info("Migration completed successfully")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        session.rollback()
        raise
    finally:
        if should_close_session:
            session.close()


def main():
    """Main migration function for command line execution"""
    run_migration()

def generate_base_station(session: Session, history_id: int, period_id: int):
    station = ClimatologicalStation(
        type="prism", # our migrated records will all be prism
        basin_id=None, # migrated records won't have basin info
        comments="",
        climo_period_id=period_id  # Use the passed period_id parameter
    )
    session.add(station)
    session.flush()  # To get the station ID assigned

    return station


def generate_base_station_history(session, station_id, history_id):
    history_link = ClimatologicalStationXHistory(
        climo_station_id=station_id,
        history_id=history_id,
        role="base"  # Migration records use base role
    )
    session.add(history_link)
    session.flush()  # To get the link ID assigned

    return history_link

cache_filled = False
cache = {}

def climo_variable_cache(session: Session):
    global cache_filled
    if not cache_filled:
        variables = session.query(ClimatologicalVariable).all()
        for var in variables:
            cache[var.net_var_name] = var.id
        cache_filled = True  # Mark cache as filled
        return cache
    else:
        return cache

def generate_value_data(session: Session, station_id: int, history_id: int):
    # Find all variables associated with this history
    variables_cache = climo_variable_cache(session)

    # Get all variables from the database (not from cache keys)
    climo_variables = session.query(ClimatologicalVariable).all()
    
    # For each climatological variable, find matching observations
    for climo_var in climo_variables:
        # Find the corresponding Variable record by name mapping
        var_name_mapping = {
            "Precip_Climatology": "Precipitation", 
            "Tx_Climatology": "Air Temperature (max)",
            "Tn_Climatology": "Air Temperature (min)",
            "T_mean_Climatology": "Air Temperature (mean)"
        }
        
        # This is a simplified approach - you may need to adjust based on your actual Variable table structure
        net_var_name = climo_var.net_var_name  # type: ignore # SQLAlchemy instance attribute  
        search_name = var_name_mapping.get(net_var_name, net_var_name)  # type: ignore
        variable = session.query(Variable).filter(
            Variable.display_name.ilike(f"%{search_name}%")
        ).first()
        
        if variable is None:
            logger.warning(f"No matching Variable found for {climo_var.net_var_name}")
            continue

        # get the observations for this variable and history
        observations = session.query(Obs).filter(
            Obs.history_id == history_id,
            Obs.variable_id == variable.id
        ).all()

        # Now we can create the ClimatologicalValue records
        for obs in observations:
            value = ClimatologicalValue(
                value_time=obs.observation_time,
                value=obs.value,
                num_contributing_years=1,  # Default to 1 for migrated data (unknown contributing years)
                climo_variable_id=climo_var.id,
                climo_station_id=station_id
            )
            session.add(value)
        
    session.flush()

if __name__ == "__main__":
    main()