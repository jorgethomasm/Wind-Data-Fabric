"""
Create DuckDB database from SCADA CSV/Parquet files.

Usage:
    python create_duckdb.py --farm windfarm_A
    python create_duckdb.py --farm all
"""

import duckdb
import argparse
from pathlib import Path
import logging
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WindFarmDBBuilder:
    """Build DuckDB database for wind farm SCADA data."""
    
    def __init__(self, farm_name: str, base_path: Path = Path(".")):
        self.farm_name = farm_name
        self.base_path = base_path
        self.raw_path = base_path / "data" / "raw" / farm_name
        self.processed_path = base_path / "data" / "processed"
        self.db_path = self.processed_path / f"{farm_name}.duckdb"
        
        # Ensure processed directory exists
        self.processed_path.mkdir(parents=True, exist_ok=True)
    
    def build(self):
        """Main build process."""
        logger.info(f"Building database for {self.farm_name}")
        
        # Connect to DuckDB
        con = duckdb.connect(str(self.db_path))
        
        try:
            # Create tables
            self._create_scada_data_table(con)
            self._create_channels_metadata_table(con)
            self._create_digital_io_table(con)
            self._create_system_modes_table(con)
            
            # Add indexes
            self._create_indexes(con)
            
            # Add table comments
            self._add_documentation(con)
            
            # Validate
            self._validate_database(con)
            
            logger.info(f"✓ Database created successfully: {self.db_path}")
            self._print_summary(con)
            
        except Exception as e:
            logger.error(f"Error building database: {e}")
            raise
        finally:
            con.close()
    
    def _create_scada_data_table(self, con):
        """Create main SCADA data table from parquet."""
        logger.info("Creating scada_data table...")
        
        parquet_file = self.raw_path / "scada_data.parquet"
        
        if not parquet_file.exists():
            # Try alternative naming
            parquet_files = list(self.raw_path.glob("*.parquet"))
            if parquet_files:
                parquet_file = parquet_files[0]
            else:
                raise FileNotFoundError(f"No parquet file found in {self.raw_path}")
        
        con.execute(f"""
            CREATE TABLE scada_data AS 
            SELECT * FROM read_parquet('{parquet_file}')
        """)
        
        logger.info(f"  Loaded {con.execute('SELECT COUNT(*) FROM scada_data').fetchone()[0]:,} rows")
    
    def _create_channels_metadata_table(self, con):
        """Create channels metadata lookup table."""
        logger.info("Creating channels_metadata table...")
        
        csv_file = self.raw_path / "SCADA_Channels_Metadata.csv"
        
        if not csv_file.exists():
            logger.warning(f"  Metadata file not found: {csv_file}")
            return
        
        con.execute(f"""
            CREATE TABLE channels_metadata AS 
            SELECT * FROM read_csv('{csv_file}', auto_detect=true)
        """)
        
        # Try to add primary key if channel_id column exists
        try:
            columns = [col[0] for col in con.execute("DESCRIBE channels_metadata").fetchall()]
            if 'channel_id' in columns:
                con.execute("ALTER TABLE channels_metadata ADD PRIMARY KEY (channel_id)")
            elif 'id' in columns:
                con.execute("ALTER TABLE channels_metadata ADD PRIMARY KEY (id)")
        except Exception as e:
            logger.warning(f"  Could not add primary key: {e}")
        
        logger.info(f"  Loaded {con.execute('SELECT COUNT(*) FROM channels_metadata').fetchone()[0]} channels")
    
    def _create_digital_io_table(self, con):
        """Create digital I/O states mapping table."""
        logger.info("Creating digital_io_mappings table...")
        
        csv_file = self.raw_path / "system_digital_io_states_mappings.csv"
        
        if not csv_file.exists():
            logger.warning(f"  Digital I/O file not found: {csv_file}")
            return
        
        con.execute(f"""
            CREATE TABLE digital_io_mappings AS 
            SELECT * FROM read_csv('{csv_file}', auto_detect=true)
        """)
        
        logger.info(f"  Loaded {con.execute('SELECT COUNT(*) FROM digital_io_mappings').fetchone()[0]} mappings")
    
    def _create_system_modes_table(self, con):
        """Create system modes mapping table from JSON."""
        logger.info("Creating system_modes table...")
        
        json_file = self.raw_path / "system_modes_mapping.json"
        
        if not json_file.exists():
            logger.warning(f"  System modes file not found: {json_file}")
            return
        
        con.execute(f"""
            CREATE TABLE system_modes AS 
            SELECT * FROM read_json('{json_file}', auto_detect=true)
        """)
        
        logger.info(f"  Loaded {con.execute('SELECT COUNT(*) FROM system_modes').fetchone()[0]} modes")
    
    def _create_indexes(self, con):
        """Create indexes for performance."""
        logger.info("Creating indexes...")
        
        indexes = [
            ("idx_scada_timestamp", "scada_data", "timestamp"),
            ("idx_scada_channel", "scada_data", "channel_id"),
        ]
        
        for idx_name, table, column in indexes:
            try:
                # Check if column exists
                columns = [col[0] for col in con.execute(f"DESCRIBE {table}").fetchall()]
                if column in columns:
                    con.execute(f"CREATE INDEX {idx_name} ON {table}({column})")
                    logger.info(f"  ✓ Created index {idx_name}")
            except Exception as e:
                logger.warning(f"  Could not create index {idx_name}: {e}")
    
    def _add_documentation(self, con):
        """Add table and column comments."""
        logger.info("Adding documentation...")
        
        comments = {
            "scada_data": "Time series data from SCADA system",
            "channels_metadata": "Lookup table: channel definitions and metadata",
            "digital_io_mappings": "Lookup table: digital I/O state descriptions",
            "system_modes": "Lookup table: system operating mode definitions"
        }
        
        for table, comment in comments.items():
            try:
                con.execute(f"COMMENT ON TABLE {table} IS '{comment}'")
            except:
                pass
    
    def _validate_database(self, con):
        """Basic validation checks."""
        logger.info("Validating database...")
        
        # Check row counts
        tables = con.execute("SHOW TABLES").fetchall()
        for (table,) in tables:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if count == 0:
                logger.warning(f"  Table {table} is empty!")
    
    def _print_summary(self, con):
        """Print database summary."""
        print("\n" + "="*60)
        print(f"DATABASE SUMMARY: {self.farm_name}")
        print("="*60)
        
        tables = con.execute("SHOW TABLES").fetchall()
        
        for (table,) in tables:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            cols = len(con.execute(f"DESCRIBE {table}").fetchall())
            print(f"  {table:30} {count:>12,} rows  {cols:>3} columns")
        
        # Database size
        size_mb = self.db_path.stat().st_size / (1024 * 1024)
        print(f"\n  Database size: {size_mb:.2f} MB")
        print("="*60 + "\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Create DuckDB from SCADA data")
    parser.add_argument(
        "--farm",
        required=True,
        help="Wind farm name (e.g., windfarm_A) or 'all'"
    )
    parser.add_argument(
        "--base-path",
        default=".",
        help="Base project path (default: current directory)"
    )
    
    args = parser.parse_args()
    base_path = Path(args.base_path)
    
    if args.farm == "all":
        # Process all wind farms
        raw_path = base_path / "data" / "raw"
        farms = [d.name for d in raw_path.iterdir() if d.is_dir()]
        
        logger.info(f"Found {len(farms)} wind farms: {', '.join(farms)}")
        
        for farm in farms:
            try:
                builder = WindFarmDBBuilder(farm, base_path)
                builder.build()
            except Exception as e:
                logger.error(f"Failed to build {farm}: {e}")
    else:
        # Process single wind farm
        builder = WindFarmDBBuilder(args.farm, base_path)
        builder.build()


if __name__ == "__main__":
    main()