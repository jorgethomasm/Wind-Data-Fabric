# Wind Data Fabric

A Python toolkit for processing and managing wind farm SCADA data using DuckDB.

## Overview

Wind Data Fabric provides utilities to automatically discover and consolidate wind farm data files (CSV, Parquet, JSON, Excel) into efficient DuckDB databases. The toolkit handles various data formats and automatically creates optimized tables with indexes and documentation.

## Features

- **Multi-format Support**: Automatically loads CSV, Parquet, JSON, and Excel files
- **Dynamic Discovery**: Scans folders and loads all compatible data files
- **Smart Table Naming**: Generates clean table names from file names
- **Auto-indexing**: Creates indexes on common columns (timestamp, channel_id, id, date, datetime)
- **Path Escaping**: Handles file paths with special characters safely
- **Performance Tracking**: Reports processing time and database statistics
- **Cross-platform**: Works on Windows, Linux, and macOS

## Project Structure

```
Wind-Data-Fabric/
├── data/
│   ├── raw/                   # Raw data files organized by folder
│   │   ├── Kelmarsh/          # Example: wind farm data folder
│   │   │   ├── scada_data.parquet
│   │   │   ├── channels_metadata.csv
│   │   │   └── system_modes.json
│   │   └── your_folder/       # Your data files here
│   └── processed/             # Generated DuckDB databases
│       └── your_folder.duckdb
│       └── your_folder_schema.yaml
│       └── your_folder_log.txt
├── scripts/
│   ├── create_duckdb.py       # Main database creation script
│   └── utils/
│       └── file_readers.py    # File reading utilities
├── create_duckdb.bat          # Windows wrapper script
├── create_duckdb.sh           # Linux/Mac wrapper script
└── README.md
```

## Installation

### Prerequisites

- Python 3.8+
- DuckDB Python package

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd Wind-Data-Fabric
```

2. Install dependencies:
```bash
pip install duckdb
```

3. For Excel file support (optional):
```bash
# DuckDB will auto-install the excel extension when needed
```

## Usage

### Quick Start

Place your data files in a folder under `data/raw/`, then create a DuckDB database:

**Windows:**
```cmd
create_duckdb.bat your_folder_name
```

**Linux/Mac:**
```bash
chmod +x create_duckdb.sh  # First time only
./create_duckdb.sh your_folder_name
```

### Python Script Direct Usage

```bash
# From project root
python scripts/create_duckdb.py --folder your_folder_name

# With custom base path
python scripts/create_duckdb.py --folder your_folder_name --base-path /path/to/project
```

### Supported File Formats

The script automatically detects and loads:

- **Parquet files** (`.parquet`): High-performance columnar format
- **CSV files** (`.csv`): Comma-separated values with auto-detection
- **JSON files** (`.json`): Structured JSON data
- **Excel files** (`.xlsx`, `.xls`): Spreadsheet data

### Example Workflow

1. **Prepare your data:**
```
data/raw/MyWindFarm/
 scada_data.parquet
 SCADA_Channels_Metadata.csv
 system_modes_mapping.json
 turbine_specs.xlsx
```

2. **Run the script:**
```bash
./create_duckdb.sh MyWindFarm
```

3. **Output:**
```
============================================================
DATABASE SUMMARY: MyWindFarm
============================================================
  scada_data                        1,234,567 rows   15 columns
  scada_channels_metadata                  45 rows    5 columns
  system_modes_mapping                     12 rows    3 columns
  turbine_specs                             8 rows   20 columns

  Raw files size: 2797.77 MB
  Database size: 552.76 MB
  Total elapsed time: 6.78 seconds
============================================================
```

4. **Access the database:**
```python
import duckdb

con = duckdb.connect('data/processed/MyWindFarm.duckdb')
result = con.execute('SELECT * FROM scada_data LIMIT 10').fetchall()
con.close()
```

## File Readers Utilities

The project includes reusable file reading utilities in `scripts/utils/file_readers.py`:

### Functions

- `read_parquet_to_table(con, parquet_path, table_name)`: Load Parquet files
- `read_csv_to_table(con, csv_path, table_name, auto_detect=True, **kwargs)`: Load CSV files
- `read_json_to_table(con, json_path, table_name, auto_detect=True)`: Load JSON files
- `read_excel_to_table(con, excel_path, table_name, sheet=None)`: Load Excel files

### Example Usage

```python
import duckdb
from scripts.utils.file_readers import read_csv_to_table, read_parquet_to_table

con = duckdb.connect('mydata.duckdb')

# Load a CSV file
result = read_csv_to_table(con, 'data.csv', 'my_table')
print(f"Loaded {result['rows']} rows")

# Load a Parquet file
result = read_parquet_to_table(con, 'data.parquet', 'another_table')
print(f"Columns: {result['columns']}")

con.close()
```

## Features in Detail

### Automatic Table Naming

File names are converted to valid SQL table names:
- `SCADA_Channels_Metadata.csv` → `scada_channels_metadata`
- `system-modes-mapping.json` → `system_modes_mapping`
- Special characters and spaces are replaced with underscores
- All names are lowercase

### Smart Indexing

Indexes are automatically created on commonly used columns:
- `timestamp`: For time-series queries
- `channel_id`: For channel-based filtering
- `id`: For primary key lookups
- `date`, `datetime`: For date-based queries

### Error Handling

- Failed file loads don't stop the entire process
- Detailed error messages for troubleshooting
- Validation checks for empty tables
- Graceful handling of missing optional files

## Configuration

### Command-line Arguments

```bash
python scripts/create_duckdb.py --help

Options:
  --folder FOLDER      Folder name with data files (required)
  --base-path PATH     Base project path (default: auto-detect)
```

### Logging

The script uses Python's logging module with timestamps:
- `INFO`: Progress updates and successful operations
- `WARNING`: Missing files, empty tables, failed indexes
- `ERROR`: Critical failures

## Troubleshooting

### Common Issues

**Issue**: "No data files found"
- **Solution**: Ensure files are in `data/raw/<folder_name>/`
- Check file extensions (`.csv`, `.parquet`, `.json`, `.xlsx`, `.xls`)

**Issue**: "Module not found: utils.file_readers"
- **Solution**: Run from project root, not from scripts folder
- Or use the provided batch/bash scripts

**Issue**: Excel files not loading
- **Solution**: DuckDB will auto-install the excel extension
- Ensure you have internet connection for first-time setup

**Issue**: Path with spaces causes errors
- **Solution**: The file readers handle this automatically with proper escaping

## Performance Tips

1. **Use Parquet**: Fastest loading and best compression
2. **Index Strategy**: Only index columns you'll query frequently
3. **Batch Processing**: Process multiple wind farms in sequence
4. **Memory**: Large files are streamed efficiently by DuckDB

## Contributing

Contributions are welcome! Please ensure:
- Code follows existing style
- File readers handle edge cases (special chars, empty files)
- Error messages are clear and actionable

## License

MIT License

Copyright (c) 2025 Jorge A. Thomas-Meléndez

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Author

Jorge A. Thomas-Meléndez   
- Email: jorgethomasm@ieee.org
- [LinkedIn](https://www.linkedin.com/in/jorge-thomas-ba751b8/)

## Acknowledgments

Built with:
- [DuckDB](https://duckdb.org/): High-performance analytical database
- Python standard library: pathlib, logging, argparse
