"""
Tests for generate_climatological_stations function.
"""
import pytest
from unittest.mock import patch, mock_open, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from main import generate_climatological_stations


class TestGenerateClimatologicalStations:
    """Test cases for generate_climatological_stations function."""

    def test_processes_all_history_lines(self, mock_session, sample_history_dict_complete):
        """Test that all history lines are processed."""
        csv_content = "history_id,lat,lon,elev,basin," + \
                     ",".join([f"monthlyyears_1971_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1971_{i}" for i in range(1, 4)]) + "," + \
                     ",".join([f"monthlyyears_1981_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1981_{i}" for i in range(1, 4)]) + "," + \
                     ",".join([f"monthlyyears_1991_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1991_{i}" for i in range(1, 4)]) + "\n"
        
        csv_content += ",".join([sample_history_dict_complete[k] for k in sample_history_dict_complete.keys()]) + "\n"
        
        with patch("builtins.open", mock_open(read_data=csv_content)):
            with patch('main.generate_station') as mock_gen_station:
                with patch('main.generate_station_histories'):
                    with patch('main.generate_value_data'):
                        mock_station = MagicMock()
                        mock_station.id = 42
                        mock_gen_station.return_value = mock_station
                        
                        generate_climatological_stations(mock_session, "ppt")
                        
                        # Should create 3 stations (one for each period with data)
                        assert mock_gen_station.call_count == 3

    def test_skips_periods_without_data(self, mock_session, sample_history_dict_partial):
        """Test that periods without data are skipped."""
        csv_content = "history_id,lat,lon,elev,basin," + \
                     ",".join([f"monthlyyears_1971_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1971_{i}" for i in range(1, 4)]) + "," + \
                     ",".join([f"monthlyyears_1981_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1981_{i}" for i in range(1, 4)]) + "," + \
                     ",".join([f"monthlyyears_1991_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1991_{i}" for i in range(1, 4)]) + "\n"
        
        csv_content += ",".join([sample_history_dict_partial[k] for k in sample_history_dict_partial.keys()]) + "\n"
        
        with patch("builtins.open", mock_open(read_data=csv_content)):
            with patch('main.generate_station') as mock_gen_station:
                with patch('main.generate_station_histories'):
                    with patch('main.generate_value_data'):
                        mock_station = MagicMock()
                        mock_station.id = 42
                        mock_gen_station.return_value = mock_station
                        
                        generate_climatological_stations(mock_session, "ppt")
                        
                        # Should create 1 station (only 1981 has data)
                        assert mock_gen_station.call_count == 1

    @pytest.mark.parametrize("variable", ["ppt", "tmax", "tmin"])
    def test_handles_different_variables(self, variable, mock_session, sample_history_dict_complete):
        """Test processing different climatological variables."""
        csv_content = "history_id,lat,lon,elev,basin," + \
                     ",".join([f"monthlyyears_1971_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1971_{i}" for i in range(1, 4)]) + "," + \
                     ",".join([f"monthlyyears_1981_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1981_{i}" for i in range(1, 4)]) + "," + \
                     ",".join([f"monthlyyears_1991_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1991_{i}" for i in range(1, 4)]) + "\n"
        
        csv_content += ",".join([sample_history_dict_complete[k] for k in sample_history_dict_complete.keys()]) + "\n"
        
        with patch("builtins.open", mock_open(read_data=csv_content)):
            with patch('main.generate_station') as mock_gen_station:
                with patch('main.generate_station_histories'):
                    with patch('main.generate_value_data') as mock_gen_value:
                        mock_station = MagicMock()
                        mock_station.id = 42
                        mock_gen_station.return_value = mock_station
                        
                        generate_climatological_stations(mock_session, variable)
                        
                        # Check that generate_value_data was called with correct variable
                        for call_args in mock_gen_value.call_args_list:
                            assert call_args[0][1] == variable
