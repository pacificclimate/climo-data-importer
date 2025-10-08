"""
Tests for file reading functions.
"""
import pytest
from unittest.mock import patch, mock_open
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from main import (
    HistoryLine,
    StationDataLine,
    read_station_info_file,
    read_data_file,
)


class TestFileReading:
    """Test cases for file reading functions."""

    @pytest.mark.parametrize("variable", ["ppt", "tmax", "tmin"])
    def test_read_station_info_file(self, variable, sample_history_dict_complete):
        """Test reading station info files for different variables."""
        csv_content = "history_id,lat,lon,elev,basin," + \
                     ",".join([f"monthlyyears_1971_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1971_{i}" for i in range(1, 4)]) + "," + \
                     ",".join([f"monthlyyears_1981_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1981_{i}" for i in range(1, 4)]) + "," + \
                     ",".join([f"monthlyyears_1991_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1991_{i}" for i in range(1, 4)]) + "\n"
        
        csv_content += ",".join([sample_history_dict_complete[k] for k in sample_history_dict_complete.keys()]) + "\n"
        
        with patch("builtins.open", mock_open(read_data=csv_content)):
            stations = read_station_info_file(variable)
            
            assert len(stations) == 1
            assert isinstance(stations[0], HistoryLine)
            assert stations[0].history_id == 12345

    @pytest.mark.parametrize("variable,period,station_id", [
        ("ppt", "1971_2000", "12345"),
        ("tmax", "1981_2010", "54321"),
        ("tmin", "1991_2020", "99999"),
    ])
    def test_read_data_file(self, variable, period, station_id, sample_station_data_dicts):
        """Test reading data files for different variables and periods."""
        csv_content = "obs_time,datum\n"
        for data_dict in sample_station_data_dicts:
            csv_content += f"{data_dict['obs_time']},{data_dict['datum']}\n"
        
        with patch("builtins.open", mock_open(read_data=csv_content)):
            data_lines = read_data_file(variable, period, station_id)
            
            assert len(data_lines) == 3
            assert all(isinstance(line, StationDataLine) for line in data_lines)
            assert data_lines[0].obs_time == "1971-01-01"
            assert data_lines[0].datum == 10.5
