"""
Shared fixtures for main.py tests.
"""
import pytest
from unittest.mock import MagicMock


# ============================================================================
# Test Data Fixtures
# ============================================================================

@pytest.fixture
def sample_history_dict_complete():
    """Complete history line with all data periods."""
    return {
        'history_id': '12345',
        'lat': '49.2827',
        'lon': '-123.1207',
        'elev': '70.0',
        'basin': '5',
        **{f'monthlyyears_1971_{i}': str(20 + i) for i in range(1, 13)},
        **{f'joint_stations_1971_{i}': str(100 + i) for i in range(1, 4)},
        **{f'monthlyyears_1981_{i}': str(25 + i) for i in range(1, 13)},
        **{f'joint_stations_1981_{i}': str(200 + i) for i in range(1, 4)},
        **{f'monthlyyears_1991_{i}': str(30 + i) for i in range(1, 13)},
        **{f'joint_stations_1991_{i}': str(300 + i) for i in range(1, 4)},
    }


@pytest.fixture
def sample_history_dict_partial():
    """History line with only 1981 data."""
    return {
        'history_id': '54321',
        'lat': '48.4284',
        'lon': '-123.3656',
        'elev': '15.0',
        'basin': 'NaN',
        **{f'monthlyyears_1971_{i}': 'NaN' for i in range(1, 13)},
        **{f'joint_stations_1971_{i}': 'NaN' for i in range(1, 4)},
        **{f'monthlyyears_1981_{i}': str(25 + i) for i in range(1, 13)},
        **{f'joint_stations_1981_{i}': str(200 + i) for i in range(1, 4)},
        **{f'monthlyyears_1991_{i}': '' for i in range(1, 13)},
        **{f'joint_stations_1991_{i}': '' for i in range(1, 4)},
    }


@pytest.fixture
def sample_station_data_dicts():
    """Sample station data lines."""
    return [
        {'obs_time': '1971-01-01', 'datum': '10.5'},
        {'obs_time': '1971-02-01', 'datum': '15.3'},
        {'obs_time': '1971-03-01', 'datum': '20.1'},
    ]


@pytest.fixture
def mock_session():
    """Create a mock SQLAlchemy session."""
    session = MagicMock()
    session.add = MagicMock()
    session.add_all = MagicMock()
    session.commit = MagicMock()
    return session
