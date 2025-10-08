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
            mock_session.commit.assert_called_once()
            assert result == mock_station

    def test_station_type_is_composite(self, mock_session, sample_history_dict_complete):
        """Test that generated stations are of type 'composite'."""
        history_line = HistoryLine(sample_history_dict_complete)
        
        with patch('main.ClimatologicalStation') as mock_station_class:
            mock_station = MagicMock()
            mock_station_class.return_value = mock_station
            
            generate_station(mock_session, history_line, climo_period_id=1)
            
            assert mock_station_class.call_args[1]['type'] == "composite"
