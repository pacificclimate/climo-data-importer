# Test Data Directory

This directory contains sample climatological data files for testing purposes.

## Directory Structure

```
tests/data/
├── ppt_composite_station_file.csv    # Station metadata for precipitation
└── csv/
    └── ppt/                           # Precipitation data
        ├── 1971_2000/                 # Data for 1971-2000 climatological period
        ├── 1981_2010/                 # Data for 1981-2010 climatological period
        └── 1991_2020/                 # Data for 1991-2020 climatological period
```

## Data Files

### Station Metadata
- **ppt_composite_station_file.csv**: Contains metadata for 12 composite precipitation stations
  - History IDs: 404, 406, 11370, 11371, 11379, 11382, 10510, 10530, 11513, 11621, 11622, 1002

### Data Coverage by Period

| Period      | Files | Description |
|-------------|-------|-------------|
| 1971-2000   | 10    | Historical baseline period |
| 1981-2010   | 10    | Standard WMO reference period |
| 1991-2020   | 8     | Recent 30-year period |

**Total: 28 CSV data files**

## File Naming Convention

Data files follow the pattern: `{history_id}_ppt_{period}.csv`

Example: `404_ppt_1971_2000.csv`

## Data Format

Each data CSV file contains:
- `obs_time`: Observation date (format: DD-MMM-YYYY)
- `datum`: Precipitation value

## Source

These test files were copied from the production data directory `/data/csv/ppt/` and represent a subset of real climatological data for testing the import functionality.

## Usage in Tests

These files can be used to test:
- Station metadata parsing
- Data file reading and validation
- Database import functionality
- Data integrity across different climatological periods
- Handling of stations with partial period coverage

## Notes

- Not all stations have data for all three periods
- Some stations only have data for specific periods (e.g., station 11379 only has 1971-2000 data)
- The data represents composite station values, which may be derived from multiple source stations (see joint_stations columns in metadata)
