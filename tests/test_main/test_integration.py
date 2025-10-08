"""
Integration-style tests using mocks to verify the full flow.
"""
import pytest
from unittest.mock import patch, mock_open, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from main import generate_climatological_stations


class TestIntegrationWithMocks:
    """Integration-style tests using mocks to verify the full flow."""

    def test_full_station_generation_flow(self, mock_session, sample_history_dict_complete):
        """Test the complete flow of generating a station with all components."""
        csv_content = "history_id,lat,lon,elev,basin," + \
                     ",".join([f"monthlyyears_1971_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1971_{i}" for i in range(1, 4)]) + "," + \
                     ",".join([f"monthlyyears_1981_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1981_{i}" for i in range(1, 4)]) + "," + \
                     ",".join([f"monthlyyears_1991_{i}" for i in range(1, 13)]) + "," + \
                     ",".join([f"joint_stations_1991_{i}" for i in range(1, 4)]) + "\n"
        
        csv_content += ",".join([sample_history_dict_complete[k] for k in sample_history_dict_complete.keys()]) + "\n"
        
        data_csv = "obs_time,datum\n1971-01-01,10.5\n"
        
        def mock_open_multi(filename, *args, **kwargs):
            if 'composite_station_info' in filename:
                return mock_open(read_data=csv_content)()
            else:
                return mock_open(read_data=data_csv)()
        
        # Mock period query
        mock_period = MagicMock()
        mock_period.id = 1
        
        # Mock variable query
        mock_var = MagicMock()
        mock_var.id = 1
        
        # Set up query chain for both period and variable
        def query_side_effect(model):
            mock_query = MagicMock()
            if 'Period' in str(model):
                mock_query.filter_by.return_value.first.return_value = mock_period
            else:  # Variable
                mock_query.filter_by.return_value.first.return_value = mock_var
            return mock_query
        
        mock_session.query.side_effect = query_side_effect
        
        with patch("builtins.open", side_effect=mock_open_multi):
            with patch('main.ClimatologicalStation') as mock_station_class:
                with patch('main.ClimatologicalStationXHistory') as mock_history_class:
                    with patch('main.ClimatologicalValue') as mock_value_class:
                        mock_station = MagicMock()
                        mock_station.id = 42
                        mock_station_class.return_value = mock_station
                        
                        generate_climatological_stations(mock_session, "ppt")
                        
                        # Verify station was created 3 times (once per period)
                        assert mock_station_class.call_count == 3
                        
                        # Verify histories were created:
                        # 3 base histories (1 per period) + 9 joint histories (3 joint × 3 periods)
                        assert mock_history_class.call_count == 12
                        
                        # Verify values were created
                        assert mock_value_class.call_count >= 3  # At least one per period
