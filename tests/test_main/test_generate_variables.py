"""
Tests for generate_climatological_variables function.
"""
import pytest
from unittest.mock import patch
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from main import generate_climatological_variables


class TestGenerateClimatologicalVariables:
    """Test cases for generate_climatological_variables function."""

    def test_generates_four_variables(self, mock_session):
        """Test that four climatological variables are generated."""
        with patch('main.ClimatologicalVariable') as mock_var_class:
            generate_climatological_variables(mock_session)
            
            assert mock_var_class.call_count == 4
            mock_session.add_all.assert_called_once()
            mock_session.flush.assert_called_once()  # We now flush instead of commit


    @pytest.mark.parametrize("standard_name,display_name,unit,duration,short_name,cell_methods,net_var_name", [
        ("lwe_thickness_of_precipitation_amount", "Precipitation Climatology", "mm", "monthly", "lwe_thickness_of_precipitation_amount t: sum within months t: mean over years", "t: sum within months t: mean over years", "Precip_Climatology"),
        ("air_temperature", "Temperature Climatology (Max.)", "celsius", "monthly", "air_temperature t: maximum within days t: mean within months t: mean over years", "t: maximum within days t: mean within months t: mean over years", "Tx_Climatology"),
        ("air_temperature", "Temperature Climatology (Min.)", "celsius", "monthly", "air_temperature t: minimum within days t: mean within months t: mean over years", "t: minimum within days t: mean within months t: mean over years", "Tn_Climatology"),
    ])
    def test_variable_attributes(self, standard_name, display_name, unit, duration, short_name, cell_methods, net_var_name, mock_session):
        """Test that variables have correct attributes."""
        with patch('main.ClimatologicalVariable') as mock_var_class:
            generate_climatological_variables(mock_session)
            
            calls = mock_var_class.call_args_list
            assert any(
                call[1] == {
                    'standard_name': standard_name,
                    'display_name': display_name,
                    'unit': unit,
                    'duration': duration,
                    'short_name': short_name,
                    'cell_methods': cell_methods,
                    'net_var_name': net_var_name
                }
                for call in calls
            )
