"""
Tests for generate_station_histories function.
"""
import pytest
from unittest.mock import patch
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from main import generate_station_histories


class TestGenerateStationHistories:
    """Test cases for generate_station_histories function."""

    @pytest.mark.parametrize("station_id,joint_stations", [
        (1, [101, 102, 103]),
        (42, [201, 202, 203]),
        (999, [301, 302, 303]),
    ])
    def test_creates_history_records(self, station_id, joint_stations, mock_session):
        """Test that history records are created for each joint station."""
        with patch('main.ClimatologicalStationXHistory') as mock_history_class:
            generate_station_histories(mock_session, station_id, joint_stations)
            
            assert mock_history_class.call_count == len(joint_stations)
            assert mock_session.add.call_count == len(joint_stations)
            # No commit assertion - we no longer commit in this function

    def test_history_record_attributes(self, mock_session):
        """Test that history records have correct attributes."""
        station_id = 42
        joint_stations = [101, 102, 103]
        
        with patch('main.ClimatologicalStationXHistory') as mock_history_class:
            generate_station_histories(mock_session, station_id, joint_stations)
            
            calls = mock_history_class.call_args_list
            for i, joint_id in enumerate(joint_stations):
                assert calls[i][1] == {
                    'climo_station_id': station_id,
                    'history_id': joint_id,
                    'role': 'joint'
                }

    def test_filters_none_values(self, mock_session):
        """Test that None values in joint_stations are filtered out."""
        station_id = 42
        joint_stations = [101, None, 103]  # Middle value is None
        
        with patch('main.ClimatologicalStationXHistory') as mock_history_class:
            generate_station_histories(mock_session, station_id, joint_stations)
            
            # Should only create 2 records (skipping None)
            assert mock_history_class.call_count == 2
            assert mock_session.add.call_count == 2
