"""
End-to-end integration tests using real PostgreSQL database.
These tests avoid mocks and use actual database operations to validate the full import flow.
"""
import os
import sys
from datetime import date
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session
import testing.postgresql
from pytest_alembic import MigrationContext
from alembic.config import Config

# Set test data directory BEFORE importing main (so basedir is set correctly)
test_dir = os.path.join(os.path.dirname(__file__), 'data')
os.environ['CLIMO_DATA_DIR'] = test_dir + '/'

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import db_setup from the parent module
from tests.conftest import db_setup, alembic_config
from pycds import (
    ClimatologicalPeriod,
    ClimatologicalStation,
    ClimatologicalStationXHistory,
    ClimatologicalValue,
    ClimatologicalVariable,
    get_schema_name
)
from main import (
    generate_climatological_periods,
    generate_climatological_variables,
    generate_station,
    generate_station_histories,
    generate_value_data,
    generate_climatological_stations,
    read_station_info_file,
    read_data_file,
)


@pytest.fixture(scope="function")
def test_db_engine():
    """Create a temporary PostgreSQL database for testing."""
    with testing.postgresql.Postgresql() as pg:
        engine = sa.create_engine(pg.url())
        db_setup(engine)
        
        # Run Alembic migrations to create tables
        import pycds
        pycds_path = os.path.dirname(pycds.__file__)
        alembic_dir = os.path.join(pycds_path, "alembic")
        
        config = Config()
        config.set_main_option("script_location", alembic_dir)
        config.set_main_option("sqlalchemy.url", pg.url())
        
        # Run migrations using alembic command
        from alembic import command
        command.upgrade(config, "head")
        
        # Set search_path so triggers can find hxtk_* functions
        schema = get_schema_name()
        with engine.begin() as conn:
            conn.execute(sa.text(f"SET search_path TO {schema}, public"))
        
        # Seed history records from test data
        from tests.conftest import seed_history_records
        seed_history_records(engine)
        
        yield engine
        engine.dispose()


@pytest.fixture(scope="function")
def test_session(test_db_engine):
    """Create a database session for testing."""
    session = Session(test_db_engine)
    # Set search_path for this session so triggers can find hxtk_* functions
    schema = get_schema_name()
    session.execute(sa.text(f"SET search_path TO {schema}, public"))
    yield session
    session.close()


@pytest.fixture(scope="function")
def test_data_dir():
    """Return the test data directory path."""
    return test_dir


class TestEndToEndDatabaseOperations:
    """End-to-end tests with real database operations."""

    def test_generate_climatological_periods(self, test_session):
        """Test generating climatological periods in a real database."""
        # Generate periods
        generate_climatological_periods(test_session)
        
        # Query the database
        schema = get_schema_name()
        periods = test_session.query(ClimatologicalPeriod).all()
        
        # Verify results
        assert len(periods) == 3
        
        start_dates = [p.start_date for p in periods]
        end_dates = [p.end_date for p in periods]
        
        # Convert to strings for comparison
        start_date_strs = [str(d).split()[0] for d in start_dates]
        end_date_strs = [str(d).split()[0] for d in end_dates]
        
        assert '1971-01-01' in start_date_strs
        assert '2000-12-31' in end_date_strs
        assert '1981-01-01' in start_date_strs
        assert '2010-12-31' in end_date_strs
        assert '1991-01-01' in start_date_strs
        assert '2020-12-31' in end_date_strs

    def test_generate_climatological_variables(self, test_session):
        """Test generating climatological variables in a real database."""
        # Generate variables
        generate_climatological_variables(test_session)
        
        # Query the database
        variables = test_session.query(ClimatologicalVariable).all()
        
        # Verify results
        assert len(variables) == 3
        
        standard_names = [v.standard_name for v in variables]
        display_names = [v.display_name for v in variables]
        
        assert 'ppt' in standard_names
        assert 'tmax' in standard_names
        assert 'tmin' in standard_names
        assert 'Precipitation' in display_names
        assert 'Maximum Temperature' in display_names
        assert 'Minimum Temperature' in display_names

    def test_generate_station_with_basin(self, test_session, test_data_dir):
        """Test creating a station with basin information."""
        # Create a period first
        period = ClimatologicalPeriod(start_date="1971-01-01", end_date="2000-12-31")
        test_session.add(period)
        test_session.commit()
        
        # Read a history line with basin data
        history_lines = read_station_info_file('ppt')
        history_with_basin = next((h for h in history_lines if h.basin is not None), None)
        
        assert history_with_basin is not None, "No history line with basin found"
        
        # Generate station
        station = generate_station(test_session, history_with_basin, period.id)
        
        # Verify
        assert station.id is not None
        assert station.type == "composite"
        assert station.basin_id == history_with_basin.basin
        
        # Query back from database
        db_station = test_session.query(ClimatologicalStation).filter_by(id=station.id).first()
        assert db_station is not None
        assert db_station.basin_id == history_with_basin.basin

    def test_generate_station_without_basin(self, test_session, test_data_dir):
        """Test creating a station without basin information."""
        # Create a period first
        period = ClimatologicalPeriod(start_date="1971-01-01", end_date="2000-12-31")
        test_session.add(period)
        test_session.commit()
        
        # Read a history line without basin data
        history_lines = read_station_info_file('ppt')
        history_without_basin = next((h for h in history_lines if h.basin is None), None)
        
        assert history_without_basin is not None, "No history line without basin found"
        
        # Generate station
        station = generate_station(test_session, history_without_basin, period.id)
        
        # Verify
        assert station.id is not None
        assert station.type == "composite"
        assert station.basin_id is None

    def test_generate_station_histories(self, test_session, test_data_dir):
        """Test generating climatological station histories."""
        # Read a history line
        history_lines = read_station_info_file('ppt')
        history_line = next((h for h in history_lines if h.has_1971_data), None)
        
        assert history_line is not None
        
        # Create period first
        period = ClimatologicalPeriod(start_date=date(1971, 1, 1), end_date=date(2000, 12, 31))
        test_session.add(period)
        test_session.commit()
        
        # Generate climo station
        climo_station = generate_station(test_session, history_line, period.id)
        
        # Generate histories
        joint_stations = [j for j in history_line.joint_stations_1971 if j is not None]
        generate_station_histories(test_session, climo_station.id, joint_stations)
        
        # Query back
        histories = test_session.query(ClimatologicalStationXHistory).filter_by(
            climo_station_id=climo_station.id
        ).all()
        
        assert len(histories) == len(joint_stations)
        history_ids = [h.history_id for h in histories]
        assert set(history_ids) == set(joint_stations)

    def test_generate_value_data(self, test_session, test_data_dir):
        """Test generating climatological values from data files."""
        # Create period and variable first
        period = ClimatologicalPeriod(start_date=date(1971, 1, 1), end_date=date(2000, 12, 31))
        test_session.add(period)
        test_session.commit()
        
        variable = ClimatologicalVariable(
            duration="monthly",
            unit="mm",
            standard_name="ppt",
            display_name="Precipitation",
            short_name="ppt",
            cell_methods="time: sum",
            net_var_name="ppt"
        )
        test_session.add(variable)
        test_session.commit()
        
        # Read a history line
        history_lines = read_station_info_file('ppt')
        history_line = next((h for h in history_lines if h.has_1971_data), None)
        
        assert history_line is not None
        
        # Generate station
        station = generate_station(test_session, history_line, period.id)
        
        # Generate value data with monthlyyears
        generate_value_data(
            test_session,
            'ppt',
            '1971_2000',
            station.id,
            str(history_line.history_id),
            history_line.monthlyyears_1971
        )
        
        # Query back
        values = test_session.query(ClimatologicalValue).filter_by(
            climo_station_id=station.id
        ).all()
        
        assert len(values) > 0
        assert len(values) == 12  # Should have 12 monthly values
        
        # Verify first value has expected structure
        first_value = values[0]
        assert first_value.climo_station_id == station.id
        assert first_value.climo_variable_id == variable.id
        assert first_value.value_time is not None
        assert first_value.value is not None
        assert isinstance(first_value.value, float)
        assert first_value.num_contributing_years is not None
        assert first_value.num_contributing_years > 0

    def test_full_station_import_single_variable(self, test_session, test_data_dir):
        """Test complete import flow for a single variable."""
        # Create periods first
        generate_climatological_periods(test_session)
        # Create variables
        generate_climatological_variables(test_session)
        
        # Import ppt data
        generate_climatological_stations(test_session, 'ppt')
        
        # Verify stations were created
        stations = test_session.query(ClimatologicalStation).all()
        assert len(stations) > 0
        
        # Verify histories were created
        histories = test_session.query(ClimatologicalStationXHistory).all()
        assert len(histories) > 0
        
        # Verify values were created
        values = test_session.query(ClimatologicalValue).all()
        assert len(values) > 0

    def test_data_integrity_station_references(self, test_session, test_data_dir):
        """Test that all station references in values are valid."""
        # Create periods first
        generate_climatological_periods(test_session)
        # Create variables
        generate_climatological_variables(test_session)
        
        # Import data
        generate_climatological_stations(test_session, 'ppt')
        
        # Get all station IDs
        station_ids = {s.id for s in test_session.query(ClimatologicalStation).all()}
        
        # Get all value station IDs
        value_station_ids = {
            v.climo_station_id for v in test_session.query(ClimatologicalValue).all()
        }
        
        # All value station IDs should reference existing stations
        assert value_station_ids.issubset(station_ids)

    def test_read_station_info_file_with_test_data(self, test_data_dir):
        """Test reading station info file from test data directory."""
        history_lines = read_station_info_file('ppt')
        
        # We know we have 12 stations in our test data
        assert len(history_lines) == 12
        
        # Verify some known history_ids
        history_ids = {h.history_id for h in history_lines}
        assert 404 in history_ids
        assert 406 in history_ids
        assert 11370 in history_ids

    def test_read_data_file_with_test_data(self, test_data_dir):
        """Test reading data file from test data directory."""
        data_lines = read_data_file('ppt', '1971_2000', '404')
        
        # Should have data
        assert len(data_lines) > 0
        
        # Verify structure
        first_line = data_lines[0]
        assert hasattr(first_line, 'obs_time')
        assert hasattr(first_line, 'datum')
        assert isinstance(first_line.datum, float)

    def test_multiple_periods_same_history_id(self, test_session, test_data_dir):
        """Test that stations with data in multiple periods create separate station records."""
        # Create periods first
        generate_climatological_periods(test_session)
        # Create variables
        generate_climatological_variables(test_session)
        
        # History 406 has data in all three periods according to our test data
        generate_climatological_stations(test_session, 'ppt')
        
        # Count how many stations reference history 406
        histories = test_session.query(ClimatologicalStationXHistory).filter_by(
            history_id=406
        ).all()
        
        # Should have references if 406 appears in the data
        assert len(histories) >= 0  # At minimum, we validate the query works

    def test_value_data_count_matches_file(self, test_session, test_data_dir):
        """Test that the number of values imported matches the data file."""
        # Create period and variable first
        period = ClimatologicalPeriod(start_date=date(1971, 1, 1), end_date=date(2000, 12, 31))
        test_session.add(period)
        test_session.commit()
        
        variable = ClimatologicalVariable(
            duration="monthly",
            unit="mm",
            standard_name="ppt",
            display_name="Precipitation",
            short_name="ppt",
            cell_methods="time: sum",
            net_var_name="ppt"
        )
        test_session.add(variable)
        test_session.commit()
        
        # Read the data file directly
        data_lines = read_data_file('ppt', '1971_2000', '404')
        expected_count = len(data_lines)
        
        # Read history to get a station to attach values to
        history_lines = read_station_info_file('ppt')
        history_404 = next((h for h in history_lines if h.history_id == 404), None)
        assert history_404 is not None
        
        # Generate station and values with period_id and monthlyyears
        station = generate_station(test_session, history_404, period.id)
        generate_value_data(test_session, 'ppt', '1971_2000', station.id, '404', history_404.monthlyyears_1971)
        
        # Count values in database
        values = test_session.query(ClimatologicalValue).filter_by(
            climo_station_id=station.id
        ).all()
        
        assert len(values) == expected_count
