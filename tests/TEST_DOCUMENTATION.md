# Test Documentation for main.py

## Overview
This document describes the comprehensive test suite created for `main.py`. The test suite includes both mocked unit tests (53 tests) and end-to-end integration tests with a real PostgreSQL database (12 tests), for a total of **65 tests**.

## Test Structure

The test suite is organized into two main categories:

### 1. Unit Tests (53 tests) - `tests/test_main/`
Mocked tests using pytest fixtures and parameterizations to test individual functions in isolation.

### 2. Integration Tests (12 tests) - `tests/test_end_to_end.py`
End-to-end tests using a real PostgreSQL database via `testing.postgresql` with pycds Alembic migrations.

## Unit Test Classes (53 tests)

### 1. TestHistoryLine (18 tests)
**Location:** `tests/test_main/test_history_line.py`

Tests the `HistoryLine` class which deserializes station metadata from CSV files.

**Parameterized Tests:**
- `test_history_id_parsing`: Tests parsing of history IDs with values [12345, 0, 999999]
- `test_coordinate_parsing`: Tests lat/lon/elevation parsing with various coordinate sets
- `test_basin_parsing`: Tests basin field parsing including NaN and empty string handling [5, 123, "NaN", ""]
- `test_data_availability_flags`: Tests availability flags for each period [1971, 1981, 1991]

**Other Tests:**
- `test_monthlyyears_1971_complete`: Validates complete monthly year data parsing
- `test_monthlyyears_with_nan`: Tests handling of NaN values in monthly data
- `test_joint_stations_complete`: Validates joint station ID parsing
- `test_partial_data_availability`: Tests when only some periods have data
- `test_repr`: Tests string representation

### 2. TestStationDataLine (4 tests)
**Location:** `tests/test_main/test_station_data_line.py`

Tests the `StationDataLine` class which deserializes observation data.

**Parameterized Tests:**
- `test_initialization`: Tests with various date and datum combinations
  - ["1971-01-01", "10.5"]
  - ["2020-12-31", "-5.3"]
  - ["1985-06-15", "0.0"]

**Other Tests:**
- `test_repr`: Tests string representation

### 3. TestFileReading (6 tests)
**Location:** `tests/test_main/test_file_reading.py`

Tests the file reading utility functions with mocked file I/O.

**Parameterized Tests:**
- `test_read_station_info_file`: Tests reading station info for each variable [ppt, tmax, tmin]
- `test_read_data_file`: Tests reading data files for different combinations:
  - ["ppt", "1971_2000", "12345"]
  - ["tmax", "1981_2010", "54321"]
  - ["tmin", "1991_2020", "99999"]

### 4. TestGenerateClimatologicalPeriods (2 tests)
**Location:** `tests/test_main/test_generate_periods.py`

Tests the `generate_climatological_periods` function.

**Tests:**
- `test_generates_three_periods`: Verifies 3 periods are created
- `test_period_dates`: Validates the correct date ranges for all periods

### 5. TestGenerateClimatologicalVariables (4 tests)
**Location:** `tests/test_main/test_generate_variables.py`

Tests the `generate_climatological_variables` function.

**Tests:**
- `test_generates_three_variables`: Verifies 3 variables are created

**Parameterized Tests:**
- `test_variable_attributes`: Tests each variable's complete attributes (7 fields):
  - ["ppt", "Precipitation", "mm", "monthly", "ppt", "time: sum", "ppt"]
  - ["tmax", "Maximum Temperature", "degC", "monthly", "tmax", "time: maximum", "tmax"]
  - ["tmin", "Minimum Temperature", "degC", "monthly", "tmin", "time: minimum", "tmin"]

### 6. TestGenerateStation (4 tests)
**Location:** `tests/test_main/test_generate_station.py`

Tests the `generate_station` function with climo_period_id parameter.

**Parameterized Tests:**
- `test_station_creation`: Tests station creation with different basin values [5, 123, None]
  - Includes climo_period_id parameter
  - Validates all expected fields in ClimatologicalStation

**Other Tests:**
- `test_station_type_is_composite`: Verifies stations are created with type "composite"

### 7. TestGenerateStationHistories (5 tests)
**Location:** `tests/test_main/test_generate_station_histories.py`

Tests the `generate_station_histories` function with correct field names.

**Parameterized Tests:**
- `test_creates_history_records`: Tests with different station IDs and joint stations:
  - [1, [101, 102, 103]]
  - [42, [201, 202, 203]]
  - [999, [301, 302, 303]]

**Other Tests:**
- `test_history_record_attributes`: Validates correct attribute assignment (climo_station_id, history_id, role)
- `test_filters_none_values`: Tests that None values in joint_stations list are properly filtered out

### 8. TestGenerateValueData (4 tests)
**Location:** `tests/test_main/test_generate_value_data.py`

Tests the `generate_value_data` function with monthlyyears parameter.

**Parameterized Tests:**
- `test_creates_value_records`: Tests with different variable/period combinations:
  - ["ppt", "1971_2000"]
  - ["tmax", "1981_2010"]
  - ["tmin", "1991_2020"]
  - Includes monthlyyears parameter and variable query mocking

**Other Tests:**
- `test_value_record_attributes`: Validates correct field names:
  - climo_station_id (not station_id)
  - climo_variable_id (not variable)
  - value_time (not obs_time)
  - value (not datum)
  - num_contributing_years

### 9. TestGenerateClimatologicalStations (5 tests)
**Location:** `tests/test_main/test_generate_climatological_stations.py`

Tests the `generate_climatological_stations` function.

**Tests:**
- `test_processes_all_history_lines`: Verifies all history lines are processed
- `test_skips_periods_without_data`: Validates that periods with no data are skipped

**Parameterized Tests:**
- `test_handles_different_variables`: Tests processing for each variable [ppt, tmax, tmin]

### 10. TestIntegrationWithMocks (1 test)
**Location:** `tests/test_main/test_integration.py`

Integration-style test using mocks to verify the complete flow.

**Tests:**
- `test_full_station_generation_flow`: Tests the entire station generation process
  - Validates 3 stations created (one per period)
  - Validates 12 history records (3 base + 9 joint)
  - Includes period and variable query mocking


## Integration Test Class (12 tests)

### TestEndToEndDatabaseOperations
**Location:** `tests/test_end_to_end.py`

Comprehensive end-to-end tests using a real PostgreSQL database with pycds Alembic migrations.

**Database Setup:**
- Uses `testing.postgresql` to create temporary PostgreSQL instance
- Runs pycds Alembic migrations to create full schema
- Seeds Network, Station, and History records from CSV data
- Uses environment variable to point to test data directory

**All 12 Tests:**
1. `test_generate_climatological_periods` - Creates 3 period records in database
2. `test_generate_climatological_variables` - Creates 3 variable records with all 7 fields
3. `test_generate_station_with_basin` - Station creation with basin_id populated
4. `test_generate_station_without_basin` - Station creation with basin_id=None
5. `test_generate_station_histories` - Creates base and joint history linkages
6. `test_generate_value_data` - Creates monthly value records with contributing years
7. `test_full_station_import_single_variable` - Complete workflow test
8. `test_data_integrity_station_references` - Validates foreign key integrity
9. `test_read_station_info_file_with_test_data` - File I/O with real CSV
10. `test_read_data_file_with_test_data` - Data file reading with real CSV
11. `test_multiple_periods_same_history_id` - Same station across multiple periods
12. `test_value_data_count_matches_file` - Validates 12 monthly values per station

**Key Validations:**
- Real database constraints (NOT NULL, foreign keys)
- Correct field names (climo_station_id, climo_variable_id, etc.)
- Proper data flow from CSV → HistoryLine → Database
- None value filtering in joint_stations
- Contributing years tracking from monthlyyears columns

## Key Testing Techniques Used

### 1. Mocking (Unit Tests)
- **SQLAlchemy Sessions**: All database operations use mocked sessions to avoid actual database dependencies
- **File I/O**: Uses `mock_open` to simulate CSV file reading
- **ORM Models**: Mocks `ClimatologicalPeriod`, `ClimatologicalVariable`, `ClimatologicalStation`, etc.
- **Query Results**: Mocks database query results for variable lookups

### 2. Real Database (Integration Tests)
- **PostgreSQL**: Uses `testing.postgresql` for real database instance
- **Alembic Migrations**: Runs actual pycds migrations to create schema
- **Seeded Data**: Pre-populates Network, Station, and History records
- **Transaction Rollback**: Each test runs in a transaction that rolls back

### 3. Parameterization
Extensive use of `@pytest.mark.parametrize` to test multiple scenarios:
- Different data types (integers, floats, strings, None)
- Edge cases (empty strings, NaN values, zero values)
- Different variables (ppt, tmax, tmin)
- Different time periods (1971_2000, 1981_2010, 1991_2020)
- Multiple parameter combinations

### 4. Fixtures
Reusable test data defined as pytest fixtures:
- `sample_history_dict_complete`: Complete history line with all periods
- `sample_history_dict_partial`: History line with only one period
- `sample_station_data_dicts`: Sample observation data
- `mock_session`: Reusable mock SQLAlchemy session
- `test_db_engine`: PostgreSQL database engine with migrations
- `test_session`: Database session with transaction rollback

## Test Evolution and Bug Discovery

The test suite evolved through several iterations:

1. **Initial Phase**: Created 52 mocked unit tests, all passing
2. **Integration Phase**: Added 12 end-to-end tests with real database
3. **Discovery Phase**: Integration tests revealed 6 critical bugs:
   - Missing 5 fields in ClimatologicalVariable creation
   - Wrong field names in ClimatologicalValue (station_id → climo_station_id)
   - Wrong field names in ClimatologicalStationXHistory
   - Missing monthlyyears parameter in generate_value_data()
   - Missing climo_period_id parameter in generate_station()
   - Missing None filtering in joint_stations processing
4. **Fix Phase**: Updated main.py to fix all issues
5. **Update Phase**: Updated all 13 mocked tests to match new signatures
6. **Success Phase**: All 65 tests now passing

**Key Learning:** Mocked tests passed even with incorrect field names because mocks don't validate against real schema. Integration tests caught these issues immediately.

## Running the Tests

### Run all tests:
```bash
poetry run pytest -v
# Result: 65 tests (53 unit + 12 integration)
```

### Run only unit tests:
```bash
poetry run pytest tests/test_main/ -v
# Result: 53 tests
```

### Run only integration tests:
```bash
poetry run pytest tests/test_end_to_end.py -v
# Result: 12 tests
```

### Run specific test class:
```bash
poetry run pytest tests/test_main/test_history_line.py::TestHistoryLine -v
```

### Run specific test:
```bash
poetry run pytest tests/test_main/test_history_line.py::TestHistoryLine::test_basin_parsing -v
```

### Run with coverage:
```bash
poetry run pytest --cov=src.main --cov-report=html
```

### Run tests matching pattern:
```bash
poetry run pytest -k "station" -v  # All tests with "station" in name
```

## Test Coverage Summary

### Unit Tests Coverage
- ✅ All data model classes (HistoryLine, StationDataLine)
- ✅ All file reading functions
- ✅ All database generation functions
- ✅ Edge cases (NaN, empty strings, missing data, None values)
- ✅ Different data types and value ranges
- ✅ Integration scenarios with mocks
- ✅ Updated signatures with new parameters

### Integration Tests Coverage
- ✅ Real PostgreSQL database operations
- ✅ Alembic migration execution
- ✅ Database constraints (NOT NULL, foreign keys)
- ✅ Full data import workflow
- ✅ CSV file reading with actual test data
- ✅ Referential integrity validation
- ✅ None value handling in joint_stations
- ✅ Contributing years data flow

## Test Statistics

**Total Tests:** 65
- Unit Tests: 53 (81.5%)
- Integration Tests: 12 (18.5%)

**By Test Type:**
- Parameterized: 38 tests
- Non-parameterized: 27 tests

**By Category:**
- Data Models: 22 tests
- File I/O: 6 tests
- Database Operations: 37 tests

**Status:**
- ✅ Passing: 65/65 (100%)
- ❌ Failing: 0/65 (0%)

## Future Enhancements

Potential additions to the test suite:
1. ✅ ~~Add tests with actual database using `testing.postgresql`~~ (COMPLETED)
2. Add performance tests for large CSV files
3. Add error handling tests for malformed CSV data
4. Add tests for concurrent data import scenarios
5. Add validation tests for data quality constraints
6. Add tests for tmax and tmin variables (currently only ppt is tested end-to-end)
7. Add tests for edge cases like stations with no joint_stations
8. Add tests for historical data updates and versioning

````

## Running the Tests

### Run all tests:
```bash
poetry run pytest tests/test_main.py -v
```

### Run specific test class:
```bash
poetry run pytest tests/test_main.py::TestHistoryLine -v
```

### Run specific test:
```bash
poetry run pytest tests/test_main.py::TestHistoryLine::test_basin_parsing -v
```

### Run with coverage:
```bash
poetry run pytest tests/test_main.py --cov=src.main --cov-report=html
```

## Test Coverage

The test suite provides comprehensive coverage of:
- ✅ All data model classes (HistoryLine, StationDataLine)
- ✅ All file reading functions
- ✅ All database generation functions
- ✅ Edge cases (NaN, empty strings, missing data)
- ✅ Different data types and value ranges
- ✅ Integration scenarios

## Future Enhancements

Potential additions to the test suite:
1. Add tests with actual database using `testing.postgresql` (already available in conftest.py)
2. Add performance tests for large CSV files
3. Add error handling tests for malformed CSV data
4. Add tests for concurrent data import scenarios
5. Add validation tests for data integrity constraints
