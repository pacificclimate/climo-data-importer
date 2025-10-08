"""
Tests for generate_climatological_periods function.
"""
import pytest
from unittest.mock import patch
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from main import generate_climatological_periods


class TestGenerateClimatologicalPeriods:
    """Test cases for generate_climatological_periods function."""

    def test_generates_three_periods(self, mock_session):
        """Test that three climatological periods are generated."""
        with patch('main.ClimatologicalPeriod') as mock_period_class:
            generate_climatological_periods(mock_session)
            
            assert mock_period_class.call_count == 3
            mock_session.add_all.assert_called_once()
            mock_session.commit.assert_called_once()

    def test_period_dates(self, mock_session):
        """Test that periods have correct date ranges."""
        with patch('main.ClimatologicalPeriod') as mock_period_class:
            generate_climatological_periods(mock_session)
            
            calls = mock_period_class.call_args_list
            assert any(
                call[1] == {'start_date': '1971-01-01', 'end_date': '2000-12-31'}
                for call in calls
            )
            assert any(
                call[1] == {'start_date': '1981-01-01', 'end_date': '2010-12-31'}
                for call in calls
            )
            assert any(
                call[1] == {'start_date': '1991-01-01', 'end_date': '2020-12-31'}
                for call in calls
            )
