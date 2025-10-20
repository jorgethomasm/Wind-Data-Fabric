from pathlib import Path
from scripts.create_duckdb import DuckDBBuilder

class TestDuckDBBuilder:
    
    def test_generate_table_name_removes_spaces(self):
        """Table names should replace spaces with underscores."""
        builder = DuckDBBuilder("test_farm")
        result = builder._generate_table_name(Path("my data.csv"))
        assert result == "my_data"
    
    def test_generate_table_name_removes_hyphens(self):
        """Table names should replace hyphens with underscores."""
        builder = DuckDBBuilder("test_farm")
        result = builder._generate_table_name(Path("data-2024.parquet"))
        assert result == "data_2024"
    
    def test_generate_table_name_lowercase(self):
        """Table names should be lowercase."""
        builder = DuckDBBuilder("test_farm")
        result = builder._generate_table_name(Path("MyTable.CSV"))
        assert result == "mytable"
    
    def test_generate_table_name_removes_special_chars(self):
        """Table names should remove special characters."""
        builder = DuckDBBuilder("test_farm")
        result = builder._generate_table_name(Path("data@#$%.json"))
        assert result == "data"