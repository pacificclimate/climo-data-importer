"""
Tests for generate_station function.
"""
import pytest
from unittest.mock import patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from main import HistoryLine, generate_station


class TestGenerateStation:
    """Test cases for generate_station function."""

    @pytest.mark.parametrize("basin_value", [5, 123, None])
    def test_station_creation(self, basin_value, mock_session, sample_history_dict_complete):
        """Test station creation with different basin values."""
        sample_history_dict_complete['basin'] = str(basin_value) if basin_value else 'NaN'
        history_line = HistoryLine(sample_history_dict_complete)
        
        # Mock the period query to return a 1971-2000 period
        mock_period = MagicMock()
        mock_period.start_date = "1971-01-01"
        mock_period.end_date = "2000-12-31"
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_period
        
        with patch('main.ClimatologicalStation') as mock_station_class:
            mock_station = MagicMock()
            mock_station.id = 42
            mock_station_class.return_value = mock_station
            
            result = generate_station(mock_session, history_line, climo_period_id=1)
            
            mock_station_class.assert_called_once_with(
                type="composite",
                basin_id=basin_value,
                comments="",
                climo_period_id=1
            )
            mock_session.add.assert_called_once()
            mock_session.flush.assert_called_once()  # We now flush instead of commit
            assert result == mock_station

    def test_station_type_is_composite(self, mock_session, sample_history_dict_complete):
        """Test that generated stations are of type 'composite'."""
        history_line = HistoryLine(sample_history_dict_complete)
        
        # Mock the period query to return a 1971-2000 period
        mock_period = MagicMock()
        mock_period.start_date = "1971-01-01"
        mock_period.end_date = "2000-12-31"
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_period
        
        with patch('main.ClimatologicalStation') as mock_station_class:
            mock_station = MagicMock()
            mock_station_class.return_value = mock_station
            
            generate_station(mock_session, history_line, climo_period_id=1)
            
            assert mock_station_class.call_args[1]['type'] == "composite"

    def test_station_type_varies_by_period(self, mock_session):
        """Test that station type can be different for different periods."""
        # Create a history line with joint stations only for 1971 period
        history_dict = {
            'history_id': '12345',
            'lat': '49.2827',
            'lon': '-123.1207',
            'elev': '70.0',
            'basin': '5',
            **{f'monthlyyears_1971_{i}': str(20 + i) for i in range(1, 13)},
            **{f'joint_stations_1971_{i}': str(100 + i) for i in range(1, 4)},  # Has joint stations for 1971
            **{f'monthlyyears_1981_{i}': str(25 + i) for i in range(1, 13)},
            **{f'joint_stations_1981_{i}': 'NaN' for i in range(1, 4)},  # No joint stations for 1981
            **{f'monthlyyears_1991_{i}': str(30 + i) for i in range(1, 13)},
            **{f'joint_stations_1991_{i}': 'NaN' for i in range(1, 4)},  # No joint stations for 1991
        }
        history_line = HistoryLine(history_dict)
        
        # Test 1971 period (should be composite due to joint stations)
        mock_period_1971 = MagicMock()
        mock_period_1971.start_date = "1971-01-01"
        mock_period_1971.end_date = "2000-12-31"
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_period_1971
        
        with patch('main.ClimatologicalStation') as mock_station_class:
            mock_station = MagicMock()
            mock_station_class.return_value = mock_station
            
            generate_station(mock_session, history_line, climo_period_id=1)
            
            assert mock_station_class.call_args[1]['type'] == "composite"
        
        # Reset mock for next test
        mock_station_class.reset_mock()
        
        # Test 1981 period (should be long-record due to no joint stations)
        mock_period_1981 = MagicMock()
        mock_period_1981.start_date = "1981-01-01"
        mock_period_1981.end_date = "2010-12-31"
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_period_1981
        
        with patch('main.ClimatologicalStation') as mock_station_class:
            mock_station = MagicMock()
            mock_station_class.return_value = mock_station
            
            generate_station(mock_session, history_line, climo_period_id=2)
            
            assert mock_station_class.call_args[1]['type'] == "long-record"
