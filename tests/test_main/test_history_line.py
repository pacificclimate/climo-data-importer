"""
Tests for the HistoryLine class.
"""
import pytest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from main import HistoryLine


class TestHistoryLine:
    """Test cases for the HistoryLine class."""

    @pytest.mark.parametrize("history_id,expected", [
        ("12345", 12345),
        ("0", 0),
        ("999999", 999999),
    ])
    def test_history_id_parsing(self, history_id, expected, sample_history_dict_complete):
        """Test history_id is correctly parsed as an integer."""
        sample_history_dict_complete['history_id'] = history_id
        line = HistoryLine(sample_history_dict_complete)
        assert line.history_id == expected
        assert isinstance(line.history_id, int)

    @pytest.mark.parametrize("lat,lon,elev", [
        ("49.2827", "-123.1207", "70.0"),
        ("0.0", "0.0", "0.0"),
        ("-45.5", "170.3", "1500.5"),
    ])
    def test_coordinate_parsing(self, lat, lon, elev, sample_history_dict_complete):
        """Test latitude, longitude, and elevation are correctly parsed as floats."""
        sample_history_dict_complete['lat'] = lat
        sample_history_dict_complete['lon'] = lon
        sample_history_dict_complete['elev'] = elev
        
        line = HistoryLine(sample_history_dict_complete)
        
        assert line.lat == float(lat)
        assert line.lon == float(lon)
        assert line.elev == float(elev)
        assert all(isinstance(x, float) for x in [line.lat, line.lon, line.elev])

    @pytest.mark.parametrize("basin_value,expected", [
        ("5", 5),
        ("123", 123),
        ("NaN", None),
        ("", None),
    ])
    def test_basin_parsing(self, basin_value, expected, sample_history_dict_complete):
        """Test basin is correctly parsed, handling NaN and empty strings."""
        sample_history_dict_complete['basin'] = basin_value
        line = HistoryLine(sample_history_dict_complete)
        assert line.basin == expected

    def test_monthlyyears_1971_complete(self, sample_history_dict_complete):
        """Test parsing of complete monthly years data for 1971 period."""
        line = HistoryLine(sample_history_dict_complete)
        assert len(line.monthlyyears_1971) == 12
        assert all(isinstance(x, int) for x in line.monthlyyears_1971)
        assert line.monthlyyears_1971[0] == 21  # First month

    def test_monthlyyears_with_nan(self, sample_history_dict_partial):
        """Test parsing of monthly years with NaN values."""
        line = HistoryLine(sample_history_dict_partial)
        assert len(line.monthlyyears_1971) == 12
        assert all(x is None for x in line.monthlyyears_1971)

    def test_joint_stations_complete(self, sample_history_dict_complete):
        """Test parsing of joint stations."""
        line = HistoryLine(sample_history_dict_complete)
        assert len(line.joint_stations_1971) == 3
        assert line.joint_stations_1971 == [101, 102, 103]
        assert len(line.joint_stations_1981) == 3
        assert line.joint_stations_1981 == [201, 202, 203]
        assert len(line.joint_stations_1991) == 3
        assert line.joint_stations_1991 == [301, 302, 303]

    @pytest.mark.parametrize("period,has_data_attr", [
        ("1971", "has_1971_data"),
        ("1981", "has_1981_data"),
        ("1991", "has_1991_data"),
    ])
    def test_data_availability_flags(self, period, has_data_attr, sample_history_dict_complete):
        """Test that data availability flags are correctly set."""
        line = HistoryLine(sample_history_dict_complete)
        assert getattr(line, has_data_attr) is True

    def test_partial_data_availability(self, sample_history_dict_partial):
        """Test data availability with partial data."""
        line = HistoryLine(sample_history_dict_partial)
        assert line.has_1971_data is False
        assert line.has_1981_data is True
        assert line.has_1991_data is False

    def test_repr(self, sample_history_dict_complete):
        """Test string representation."""
        line = HistoryLine(sample_history_dict_complete)
        repr_str = repr(line)
        assert "HistoryLine" in repr_str
        assert "history_id=12345" in repr_str
        assert "has_1971_data=True" in repr_str
