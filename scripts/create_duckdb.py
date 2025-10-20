"""
Create DuckDB database from SCADA CSV/Parquet files.

Usage:
    python create_duckdb.py --folder windfarm_A

Author: jorgethomasm@ieee.org
"""

import duckdb
import argparse
from pathlib import Path
import logging
import time
import yaml
from datetime import datetime
from utils.file_readers import read_parquet_to_table, read_csv_to_table, read_json_to_table, read_excel_to_table

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DuckDBBuilder:
    """Build DuckDB database for exported SCADA tabular data files."""
    
    def __init__(self, folder_name: str, base_path: Path = None):
        self.folder_name = folder_name  # Folder name can be the Wind Farm name

        # If no base_path provided, use the parent directory of the scripts folder
        if base_path is None:
            # Get the directory where this script is located (scripts folder)
            script_dir = Path(__file__).parent
            # Go up one level to the project root (Wind-Data-Fabric)
            self.base_path = script_dir.parent
        else:
            self.base_path = Path(base_path)

        self.raw_path = self.base_path / "data" / "raw" / folder_name
        self.processed_path = self.base_path / "data" / "processed"
        self.db_path = self.processed_path / f"{folder_name}.duckdb"

        # Ensure processed directory exists
        self.processed_path.mkdir(parents=True, exist_ok=True)

    def _discover_data_files(self):
        """Discover all data files in the raw folder and categorize by type."""
        discovered = {
            'parquet': [],
            'csv': [],
            'json': [],
            'excel': []
        }

        if not self.raw_path.exists():
            logger.warning(f"Raw data path does not exist: {self.raw_path}")
            return discovered

        # Discover parquet files
        discovered['parquet'] = list(self.raw_path.glob("*.parquet"))

        # Discover CSV files
        discovered['csv'] = list(self.raw_path.glob("*.csv"))

        # Discover JSON files
        discovered['json'] = list(self.raw_path.glob("*.json"))

        # Discover Excel files (xlsx, xls)
        discovered['excel'] = list(self.raw_path.glob("*.xlsx")) + list(self.raw_path.glob("*.xls"))

        return discovered

    def _generate_table_name(self, file_path: Path) -> str:
        """Generate a clean table name from file path."""
        # Remove extension and sanitize
        name = file_path.stem
        # Replace spaces and special chars with underscores
        name = name.replace(' ', '_').replace('-', '_')
        # Remove any non-alphanumeric chars except underscore
        name = ''.join(c for c in name if c.isalnum() or c == '_')
        # Ensure lowercase
        return name.lower()

    def _load_all_files(self, con, discovered):
        """Load all discovered files into DuckDB tables."""
        tables_created = []

        # Load parquet files
        for parquet_file in discovered['parquet']:
            table_name = self._generate_table_name(parquet_file)
            logger.info(f"Loading parquet: {parquet_file.name} → {table_name}")
            try:
                result = read_parquet_to_table(con, parquet_file, table_name)
                logger.info(f"  ✓ Loaded {result['rows']:,} rows, {len(result['columns'])} columns")
                tables_created.append(table_name)
            except Exception as e:
                logger.error(f"  ✗ Failed to load {parquet_file.name}: {e}")

        # Load CSV files
        for csv_file in discovered['csv']:
            table_name = self._generate_table_name(csv_file)
            logger.info(f"Loading CSV: {csv_file.name} → {table_name}")
            try:
                result = read_csv_to_table(con, csv_file, table_name, auto_detect=True)
                logger.info(f"  ✓ Loaded {result['rows']:,} rows, {len(result['columns'])} columns")
                tables_created.append(table_name)
            except Exception as e:
                logger.error(f"  ✗ Failed to load {csv_file.name}: {e}")

        # Load JSON files
        for json_file in discovered['json']:
            table_name = self._generate_table_name(json_file)
            logger.info(f"Loading JSON: {json_file.name} → {table_name}")
            try:
                result = read_json_to_table(con, json_file, table_name, auto_detect=True)
                logger.info(f"  ✓ Loaded {result['rows']:,} rows, {len(result['columns'])} columns")
                tables_created.append(table_name)
            except Exception as e:
                logger.error(f"  ✗ Failed to load {json_file.name}: {e}")

        # Load Excel files
        for excel_file in discovered['excel']:
            table_name = self._generate_table_name(excel_file)
            logger.info(f"Loading Excel: {excel_file.name} → {table_name}")
            try:
                result = read_excel_to_table(con, excel_file, table_name)
                logger.info(f"  ✓ Loaded {result['rows']:,} rows, {len(result['columns'])} columns")
                tables_created.append(table_name)
            except Exception as e:
                logger.error(f"  ✗ Failed to load {excel_file.name}: {e}")

        logger.info(f"Successfully created {len(tables_created)} tables")
        return tables_created

    def build(self):
        """Main build process - dynamically load all discovered data files."""
        start_time = time.time()
        logger.info(f"Building database for {self.folder_name}")

        # Discover available data files
        discovered = self._discover_data_files()

        # Log what was found
        total_files = sum(len(files) for files in discovered.values())
        logger.info(f"Discovered {total_files} data files:")
        for file_type, files in discovered.items():
            if files:
                logger.info(f"  {file_type.upper()}: {len(files)} file(s)")

        if total_files == 0:
            logger.warning(f"No data files found in {self.raw_path}")
            return

        # Connect to DuckDB
        con = duckdb.connect(str(self.db_path))

        try:
            # Load all discovered files into tables
            self._load_all_files(con, discovered)

            # Add indexes
            self._create_indexes(con)

            # Add table comments
            self._add_documentation(con)

            # Validate
            self._validate_database(con)

            # Export schema to YAML
            self._export_schema(con)

            # Calculate elapsed time
            elapsed_time = time.time() - start_time

            logger.info(f"✓ Database created successfully: {self.db_path}")
            self._print_summary(con, elapsed_time)

        except Exception as e:
            logger.error(f"Error building database: {e}")
            raise
        finally:
            con.close()
    
    def _create_indexes(self, con):
        """Create indexes for performance on common columns across all tables."""
        logger.info("Creating indexes...")

        # Get all tables
        tables = [table[0] for table in con.execute("SHOW TABLES").fetchall()]

        # Common columns to index if they exist
        index_candidates = ['timestamp', 'channel_id', 'id', 'date', 'datetime']

        indexes_created = 0
        for table in tables:
            try:
                # Get columns for this table
                columns = [col[0] for col in con.execute(f"DESCRIBE {table}").fetchall()]

                # Create indexes for matching columns
                for column in index_candidates:
                    if column in columns:
                        idx_name = f"idx_{table}_{column}"
                        try:
                            con.execute(f"CREATE INDEX {idx_name} ON {table}({column})")
                            logger.info(f"  ✓ Created index {idx_name}")
                            indexes_created += 1
                        except Exception as e:
                            logger.warning(f"  Could not create index {idx_name}: {e}")
            except Exception as e:
                logger.warning(f"  Could not process table {table}: {e}")

        if indexes_created == 0:
            logger.info("  No indexes created (no matching columns found)")
    
    def _add_documentation(self, con):
        """Add table comments based on common naming patterns."""
        logger.info("Adding documentation...")

        # Get all tables
        tables = [table[0] for table in con.execute("SHOW TABLES").fetchall()]

        # Common naming patterns and their descriptions
        comment_patterns = {
            #'scada': "Time series data from SCADA system",
            'metadata': "Metadata and lookup information",
            'mapping': "Mapping or lookup table",
            'modes': "System operating modes",
            'digital_io': "Digital I/O states and descriptions",
            'channels': "Channel definitions and metadata"
        }

        for table in tables:
            # Try to find a matching pattern
            comment = f"Data table: {table}"
            for pattern, desc in comment_patterns.items():
                if pattern in table.lower():
                    comment = desc
                    break

            try:
                con.execute(f"COMMENT ON TABLE {table} IS '{comment}'")
            except Exception as e:
                logger.warning(f"  Could not add comment to {table}: {e}")
    
    def _validate_database(self, con):
        """Basic validation checks."""
        logger.info("Validating database...")

        # Check row counts
        tables = con.execute("SHOW TABLES").fetchall()
        for (table,) in tables:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if count == 0:
                logger.warning(f"  Table {table} is empty!")

    def _export_schema(self, con):
        """Export database schema to YAML file."""
        logger.info("Exporting schema to YAML...")

        schema_data = {
            'database': self.folder_name,
            'created': datetime.now().isoformat(),
            'db_file': str(self.db_path),
            'tables': {}
        }

        # Get all tables
        tables = [table[0] for table in con.execute("SHOW TABLES").fetchall()]

        for table in tables:
            try:
                # Get row count
                row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

                # Get column information
                columns_info = con.execute(f"DESCRIBE {table}").fetchall()
                columns = []
                for col in columns_info:
                    col_dict = {
                        'name': col[0],
                        'type': col[1],
                        'nullable': col[2] == 'YES'
                    }
                    columns.append(col_dict)

                # Get indexes for this table
                try:
                    indexes_result = con.execute(f"""
                        SELECT index_name
                        FROM duckdb_indexes()
                        WHERE table_name = '{table}'
                    """).fetchall()
                    indexes = [idx[0] for idx in indexes_result]
                except Exception:
                    indexes = []

                # Get table comment if available
                try:
                    comment_result = con.execute(f"""
                        SELECT comment
                        FROM duckdb_tables()
                        WHERE table_name = '{table}'
                    """).fetchone()
                    comment = comment_result[0] if comment_result and comment_result[0] else None
                except Exception:
                    comment = None

                # Build table schema
                table_schema = {
                    'row_count': row_count,
                    'columns': columns
                }

                if indexes:
                    table_schema['indexes'] = indexes

                if comment:
                    table_schema['description'] = comment

                schema_data['tables'][table] = table_schema

            except Exception as e:
                logger.warning(f"  Could not export schema for table {table}: {e}")

        # Write to YAML file
        schema_file = self.processed_path / f"{self.folder_name}_schema.yaml"
        try:
            with open(schema_file, 'w') as f:
                yaml.dump(schema_data, f, default_flow_style=False, sort_keys=False, indent=2)
            logger.info(f"  ✓ Schema exported to: {schema_file}")
        except Exception as e:
            logger.error(f"  ✗ Failed to write schema file: {e}")

    def _print_summary(self, con, elapsed_time=None):
        """Print database summary."""
        print("\n" + "="*60)
        print(f"DATABASE SUMMARY: {self.folder_name}")
        print("="*60)

        tables = con.execute("SHOW TABLES").fetchall()

        for (table,) in tables:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            cols = len(con.execute(f"DESCRIBE {table}").fetchall())
            print(f"  {table:30} {count:>12,} rows  {cols:>3} columns")

        # Database size
        size_mb = self.db_path.stat().st_size / (1024 * 1024)
        print(f"\n  Database size: {size_mb:.2f} MB")

        # Elapsed time
        if elapsed_time is not None:
            print(f"  Total elapsed time: {elapsed_time:.2f} seconds")

        print("="*60 + "\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Create a DuckDB database file from SCADA data")
    parser.add_argument(
        "--folder",
        required=True,
        help="Folder name with (raw) data files (e.g., Kelmarsh)"
    )
    parser.add_argument(
        "--base-path",
        default=None,
        help="Base project path (default: auto-detect from script location)"
    )

    args = parser.parse_args()

    # Process single folder (wind farm)
    if args.base_path:
        builder = DuckDBBuilder(args.folder, Path(args.base_path))
    else:
        builder = DuckDBBuilder(args.folder)
    builder.build()

if __name__ == "__main__":
    main()
