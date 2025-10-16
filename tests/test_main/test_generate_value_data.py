"""
Tests for generate_value_data function.
"""
import pytest
from unittest.mock import patch, mock_open, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from main import generate_value_data


class TestGenerateValueData:
    """Test cases for generate_value_data function."""

    @pytest.mark.parametrize("variable,period", [
        ("ppt", "1971_2000"),
        ("tmax", "1981_2010"),
        ("tmin", "1991_2020"),
    ])
    def test_creates_value_records(self, variable, period, mock_session, sample_station_data_dicts):
        """Test that value records are created from data file."""
        csv_content = "obs_time,datum\n"
        for data_dict in sample_station_data_dicts:
            csv_content += f"{data_dict['obs_time']},{data_dict['datum']}\n"
        
        # Mock the variable query
        mock_var = MagicMock()
        mock_var.id = 1
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_var
        
        monthlyyears = [30, 30, 30]  # 3 months of data
        
        with patch("builtins.open", mock_open(read_data=csv_content)):
            with patch('main.ClimatologicalValue') as mock_value_class:
                generate_value_data(mock_session, variable, period, 42, "12345", monthlyyears)
                
                assert mock_value_class.call_count == 3
                assert mock_session.add.call_count == 3
                # No commit assertion - we no longer commit in this function

    def test_value_record_attributes(self, mock_session, sample_station_data_dicts):
        """Test that value records have correct attributes."""
        csv_content = "obs_time,datum\n1971-01-01,10.5\n"
        
        # Mock the variable query
        mock_var = MagicMock()
        mock_var.id = 1
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_var
        
        monthlyyears = [30]  # 1 month of data with 30 contributing years
        
        with patch("builtins.open", mock_open(read_data=csv_content)):
            with patch('main.ClimatologicalValue') as mock_value_class:
                generate_value_data(mock_session, "ppt", "1971_2000", 42, "12345", monthlyyears)
                
                mock_value_class.assert_called_with(
                    climo_station_id=42,
                    climo_variable_id=1,
                    value_time="1971-01-01",
                    value=10.5,
                    num_contributing_years=30
                )
