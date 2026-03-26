"""
End-to-end tests for migration.py script.
These tests validate the migration of existing climatological data from obs_raw to the new structure.
"""
import os
import sys
from datetime import date
import pytest
from unittest.mock import patch, MagicMock
import sqlalchemy as sa
from sqlalchemy.orm import Session
import testing.postgresql
from pytest_alembic import MigrationContext
from alembic.config import Config

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
    ClimoObsCount,
    History,
    Obs,
    Variable,
    get_schema_name
)
from migration import (
    generate_base_station,
    generate_base_station_history,
    generate_value_data,
    climo_variable_cache,
    run_migration
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
def mock_climo_obs_count():
    """Create mock ClimoObsCount objects for testing."""
    def create_mock_count(history_id, count=3):
        mock_count = MagicMock()
        mock_count.history_id = history_id
        mock_count.count = count
        return mock_count
    return create_mock_count


@pytest.fixture(scope="function")
def seed_migration_data(test_session):
    """Seed the database with test data for migration testing."""
    # Create climatological periods (these should exist before migration)
    periods = [
        ClimatologicalPeriod(start_date="1971-01-01", end_date="2000-12-31"),
        ClimatologicalPeriod(start_date="1981-01-01", end_date="2010-12-31"),
        ClimatologicalPeriod(start_date="1991-01-01", end_date="2020-12-31"),
    ]
    test_session.add_all(periods)
    test_session.flush()
    
    # Create climatological variables (these should exist before migration)
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
    ]
    test_session.add_all(variables)
    test_session.flush()
    
    # Create test variables in the Variable table (represents existing obs_raw variables)
    obs_variables = [
        Variable(
            name="Precipitation_test",
            display_name="Precipitation",
            standard_name="lwe_thickness_of_precipitation_amount",
            unit="mm",
            cell_method="sum"  # Required field
        ),
        Variable(
            name="Temp_max_test", 
            display_name="Air Temperature (max)",
            standard_name="air_temperature",
            unit="celsius",
            cell_method="max"  # Required field
        )
    ]
    test_session.add_all(obs_variables)
    test_session.flush()
    
    # Create test histories (represents existing stations)
    from tests.conftest import seed_history_records
    seed_history_records(test_session.bind)
    
    # Get the first history for testing
    history = test_session.query(History).first()
    
    # Create some test observations (represents existing climatological obs data)
    test_obs = [
        Obs(
            history_id=history.id,
            variable_id=obs_variables[0].id,  # Precipitation
            observation_time="1971-01-15",  # January average
            value=45.2
        ),
        Obs(
            history_id=history.id,
            variable_id=obs_variables[0].id,  # Precipitation
            observation_time="1971-02-15",  # February average
            value=38.7
        ),
        Obs(
            history_id=history.id,
            variable_id=obs_variables[1].id,  # Temperature max
            observation_time="1971-01-15",  # January average
            value=2.5
        ),
    ]
    test_session.add_all(test_obs)
    test_session.flush()
    
    # Note: ClimoObsCount is likely a view, so we can't insert directly
    # Instead, we'll mock the query in our tests or create the underlying data
    # that would make the view return results
    
    test_session.commit()
    
    return {
        'periods': periods,
        'variables': variables,
        'obs_variables': obs_variables,
        'history': history,
        'observations': test_obs
    }


class TestMigrationFunctions:
    """Test individual migration functions."""

    def test_generate_base_station(self, test_session, seed_migration_data):
        """Test generating a base climatological station."""
        period_id = seed_migration_data['periods'][0].id
        history_id = seed_migration_data['history'].id
        
        station = generate_base_station(test_session, history_id, period_id)
        
        # Verify station was created correctly
        assert station.climo_station_id is not None
        # Use getattr to avoid SQLAlchemy comparison issues
        assert getattr(station, 'type') == "prism"
        assert getattr(station, 'basin_id') is None  # Migration records don't have basin info
        assert getattr(station, 'comments') == ""
        assert getattr(station, 'climo_period_id') == period_id
        
        # Verify it was persisted to database
        db_station = test_session.query(ClimatologicalStation).filter_by(climo_station_id=station.climo_station_id).first()
        assert db_station is not None
        assert db_station.type == "prism"

    def test_generate_base_station_history(self, test_session, seed_migration_data):
        """Test generating station history linkage."""
        period_id = seed_migration_data['periods'][0].id
        history_id = seed_migration_data['history'].id
        
        # Create a station first
        station = generate_base_station(test_session, history_id, period_id)
        
        # Generate the history linkage
        generate_base_station_history(test_session, station.id, history_id)
        
        # Verify the linkage was created
        history_link = test_session.query(ClimatologicalStationXHistory).filter_by(
            climo_station_id=station.id
        ).first()
        
        assert history_link is not None
        assert history_link.history_id == history_id
        assert history_link.climo_station_id == station.id

    def test_generate_value_data(self, test_session, seed_migration_data):
        """Test generating climatological values from existing observations."""
        period_id = seed_migration_data['periods'][0].id
        history_id = seed_migration_data['history'].id
        
        # Create a station first
        station = generate_base_station(test_session, history_id, period_id)
        
        # Generate value data
        generate_value_data(test_session, station.id, history_id)  # type: ignore
        
        # Verify values were created
        values = test_session.query(ClimatologicalValue).filter_by(
            climo_station_id=station.id
        ).all()
        
        assert len(values) > 0
        
        # Verify value structure
        first_value = values[0]
        assert first_value.climo_station_id == station.id
        assert first_value.climo_variable_id is not None
        assert first_value.value_time is not None
        assert first_value.value is not None
        assert isinstance(first_value.value, float)
        # num_contributing_years should be 1 for migrated data (default when unknown)
        assert first_value.num_contributing_years == 1

    def test_climo_variable_cache(self, test_session, seed_migration_data):
        """Test the climatological variable caching mechanism."""
        # First call should populate cache
        cache1 = climo_variable_cache(test_session)
        
        # Verify cache contains expected variables
        assert "Precip_Climatology" in cache1
        assert "Tx_Climatology" in cache1
        
        # Second call should return cached results
        cache2 = climo_variable_cache(test_session)
        
        # Should be the same cache object
        assert cache1 is cache2
        
        # Verify cache values are variable IDs
        precip_var = test_session.query(ClimatologicalVariable).filter_by(
            net_var_name="Precip_Climatology"
        ).first()
        assert cache1["Precip_Climatology"] == precip_var.id


class TestMigrationEndToEnd:
    """End-to-end tests for the complete migration process."""

    def test_full_migration_process(self, test_session, seed_migration_data, mock_climo_obs_count):
        """Test the complete migration process end-to-end."""
        # Count initial state
        initial_stations = test_session.query(ClimatologicalStation).count()
        initial_histories = test_session.query(ClimatologicalStationXHistory).count()
        initial_values = test_session.query(ClimatologicalValue).count()
        
        # Create mock ClimoObsCount data
        mock_history = seed_migration_data['history']
        mock_counts = [mock_climo_obs_count(mock_history.id)]
        
        # Mock the ClimoObsCount query in the migration
        with patch.object(test_session, 'query') as mock_query:
            # Set up the mock to return our test data for ClimoObsCount
            def query_side_effect(model):
                if model == ClimoObsCount:
                    mock_query_result = MagicMock()
                    mock_query_result.all.return_value = mock_counts
                    return mock_query_result
                else:
                    # For other queries, use the real session
                    return test_session.query(model)
            
            mock_query.side_effect = query_side_effect
            
            # Run migration with our test session
            run_migration(test_session)
        
        # Verify migration results
        final_stations = test_session.query(ClimatologicalStation).count()
        final_histories = test_session.query(ClimatologicalStationXHistory).count()
        final_values = test_session.query(ClimatologicalValue).count()
        
        assert final_stations > initial_stations
        assert final_histories > initial_histories
        assert final_values > initial_values

    def test_migration_with_multiple_histories(self, test_session, seed_migration_data):
        """Test migration with multiple history records."""
        period_id = seed_migration_data['periods'][0].id
        
        # Create additional test histories
        from tests.conftest import seed_history_records
        seed_history_records(test_session.bind)
        
        histories = test_session.query(History).limit(3).all()
        
        stations_created = []
        
        for history in histories:
            # Simulate what the migration loop does
            station = generate_base_station(test_session, history.id, period_id)
            generate_base_station_history(test_session, station.id, history.id)
            stations_created.append(station)
        
        test_session.commit()
        
        # Verify all stations were created
        assert len(stations_created) == 3
        
        # Verify all have unique IDs
        station_ids = [s.id for s in stations_created]
        assert len(set(station_ids)) == 3
        
        # Verify all have history linkages
        for station in stations_created:
            history_link = test_session.query(ClimatologicalStationXHistory).filter_by(
                climo_station_id=station.id
            ).first()
            assert history_link is not None

    def test_migration_data_integrity(self, test_session, seed_migration_data):
        """Test that migration maintains data integrity."""
        period_id = seed_migration_data['periods'][0].id
        history_id = seed_migration_data['history'].id
        
        # Run migration for one station
        station = generate_base_station(test_session, history_id, period_id)
        generate_base_station_history(test_session, station.id, history_id)  # type: ignore
        generate_value_data(test_session, station.id, history_id)  # type: ignore
        
        test_session.commit()
        
        # Verify foreign key relationships
        db_station = test_session.query(ClimatologicalStation).filter_by(id=station.id).first()
        assert db_station.climo_period_id == period_id
        
        # Verify station-history relationship
        history_link = test_session.query(ClimatologicalStationXHistory).filter_by(
            climo_station_id=station.id
        ).first()
        assert history_link.history_id == history_id
        
        # Verify climatological values reference valid variables and stations
        values = test_session.query(ClimatologicalValue).filter_by(
            climo_station_id=station.id
        ).all()
        
        for value in values:
            # Check station reference
            assert value.climo_station_id == station.id
            
            # Check variable reference exists
            var = test_session.query(ClimatologicalVariable).filter_by(
                id=value.climo_variable_id
            ).first()
            assert var is not None
            
            # Check value is reasonable
            assert value.value is not None
            assert isinstance(value.value, (int, float))

    def test_migration_handles_missing_variables(self, test_session, seed_migration_data):
        """Test migration gracefully handles missing variable mappings."""
        period_id = seed_migration_data['periods'][0].id
        history_id = seed_migration_data['history'].id
        
        # Create a station
        station = generate_base_station(test_session, history_id, period_id)
        
        # Remove one of the climatological variables to simulate missing mapping
        test_session.query(ClimatologicalVariable).filter_by(
            net_var_name="Tx_Climatology"
        ).delete()
        test_session.flush()
        
        # Migration should handle this gracefully (log warning, continue)
        generate_value_data(test_session, station.id, history_id)  # type: ignore
        
        # Should still create values for the remaining variables
        values = test_session.query(ClimatologicalValue).filter_by(
            climo_station_id=station.id
        ).all()
        
        # Should have values for precipitation but not temperature
        precip_var = test_session.query(ClimatologicalVariable).filter_by(
            net_var_name="Precip_Climatology"
        ).first()
        
        precip_values = [v for v in values if v.climo_variable_id == precip_var.id]
        assert len(precip_values) > 0


class TestMigrationErrorHandling:
    """Test error handling in migration functions."""

    def test_migration_with_invalid_period_id(self, test_session, seed_migration_data):
        """Test migration handles invalid period ID."""
        history_id = seed_migration_data['history'].id
        invalid_period_id = 99999
        
        with pytest.raises(Exception):  # Should raise foreign key constraint error
            station = generate_base_station(test_session, history_id, invalid_period_id)
            test_session.commit()

    def test_migration_with_invalid_history_id(self, test_session, seed_migration_data):
        """Test migration handles invalid history ID."""
        period_id = seed_migration_data['periods'][0].id
        invalid_history_id = 99999
        
        # Should create station but fail on history linkage
        station = generate_base_station(test_session, invalid_history_id, period_id)
        
        with pytest.raises(Exception):  # Should raise foreign key constraint error
            generate_base_station_history(test_session, station.id, invalid_history_id)
            test_session.commit()

    def test_empty_climo_obs_count(self, test_session, seed_migration_data):
        """Test migration handles empty ClimoObsCount gracefully."""
        # Migration loop should not create any stations with empty ClimoObsCount
        initial_count = test_session.query(ClimatologicalStation).count()
        
        # Mock empty ClimoObsCount query
        with patch.object(test_session, 'query') as mock_query:
            def query_side_effect(model):
                if model == ClimoObsCount:
                    mock_query_result = MagicMock()
                    mock_query_result.all.return_value = []  # Empty result
                    return mock_query_result
                else:
                    return test_session.query(model)
            
            mock_query.side_effect = query_side_effect
            
            # Run migration - should complete without error
            run_migration(test_session)
        
        final_count = test_session.query(ClimatologicalStation).count()
        assert final_count == initial_count