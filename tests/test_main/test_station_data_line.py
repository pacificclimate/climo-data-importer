"""
Tests for the StationDataLine class.
"""
import pytest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from main import StationDataLine


class TestStationDataLine:
    """Test cases for the StationDataLine class."""

    @pytest.mark.parametrize("obs_time,datum", [
        ("1971-01-01", "10.5"),
        ("2020-12-31", "-5.3"),
        ("1985-06-15", "0.0"),
    ])
    def test_initialization(self, obs_time, datum):
        """Test StationDataLine initialization with various data."""
        data_dict = {'obs_time': obs_time, 'datum': datum}
        line = StationDataLine(data_dict)
        
        assert line.obs_time == obs_time
        assert line.datum == float(datum)
        assert isinstance(line.datum, float)

    def test_repr(self):
        """Test string representation."""
        data_dict = {'obs_time': '1971-01-01', 'datum': '10.5'}
        line = StationDataLine(data_dict)
        repr_str = repr(line)
        
        assert "StationDataLine" in repr_str
        assert "obs_time=1971-01-01" in repr_str
        assert "datum=10.5" in repr_str
