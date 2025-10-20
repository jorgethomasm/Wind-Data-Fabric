"""
File readers utility for DuckDB.

Author: jorgethomasm@ieee.org
"""

from pathlib import Path
from typing import Dict, Union
import duckdb


def _escape_path(path: Union[str, Path]) -> str:
    """Return a SQL-safe single-quoted path."""
    p = str(path)
    return p.replace("'", "''")


def read_parquet_to_table(con: duckdb.DuckDBPyConnection, parquet_path: Union[str, Path], table_name: str) -> Dict:
    """
    Create a table from a parquet file:
      CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')
    Returns dict with table, rows, columns.
    """
    p = Path(parquet_path)
    if not p.exists():
        raise FileNotFoundError(p)
    path_sql = _escape_path(p)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{path_sql}')")
    rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    cols = [c[0] for c in con.execute(f"DESCRIBE {table_name}").fetchall()]
    return {"table": table_name, "rows": rows, "columns": cols}


def read_csv_to_table(con: duckdb.DuckDBPyConnection, csv_path: Union[str, Path], table_name: str, auto_detect: bool = True, **read_csv_kwargs) -> Dict:
    """
    Create a table from a CSV file using read_csv.
    Additional keyword args will be formatted as SQL literals where supported (e.g. sep=',', header=True).
    """
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(p)
    path_sql = _escape_path(p)

    # Build optional args string
    args = []
    if auto_detect:
        args.append("auto_detect=true")
    for k, v in read_csv_kwargs.items():
        if isinstance(v, bool):
            args.append(f"{k}={'true' if v else 'false'}")
        elif isinstance(v, (int, float)):
            args.append(f"{k}={v}")
        else:
            val = str(v).replace("'", "''")
            args.append(f"{k}='{val}'")
    args_sql = ", " + ", ".join(args) if args else ""

    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv('{path_sql}'{args_sql})")
    rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    cols = [c[0] for c in con.execute(f"DESCRIBE {table_name}").fetchall()]
    return {"table": table_name, "rows": rows, "columns": cols}


def read_json_to_table(con: duckdb.DuckDBPyConnection, json_path: Union[str, Path], table_name: str, auto_detect: bool = True) -> Dict:
    """
    Create a table from a JSON file using read_json.
    """
    p = Path(json_path)
    if not p.exists():
        raise FileNotFoundError(p)
    path_sql = _escape_path(p)
    args_sql = "auto_detect=true" if auto_detect else ""
    args_fragment = f", {args_sql}" if args_sql else ""
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json('{path_sql}'{args_fragment})")
    rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    cols = [c[0] for c in con.execute(f"DESCRIBE {table_name}").fetchall()]
    return {"table": table_name, "rows": rows, "columns": cols}


def read_excel_to_table(con: duckdb.DuckDBPyConnection, xls_path: Union[str, Path], table_name: str, sheet: Union[str, int, None] = None) -> Dict:
    """
    Create a table from an Excel file. Attempts to INSTALL/LOAD the 'excel' extension if needed.
    sheet may be a name or index (0-based). If None, DuckDB will read the first sheet.
    """
    p = Path(xls_path)
    if not p.exists():
        raise FileNotFoundError(p)
    path_sql = _escape_path(p)

    # Ensure excel extension is available
    try:
        con.execute("LOAD 'excel'")
    except Exception:
        # Try to install if load failed
        try:
            con.execute("INSTALL 'excel'")
            con.execute("LOAD 'excel'")
        except Exception as e:
            raise RuntimeError("DuckDB Excel extension is not available and could not be installed/loaded") from e

    if sheet is None:
        sheet_arg = ""
    else:
        # Escape single quotes in sheet name/index and build the argument safely
        s = str(sheet).replace("'", "''")
        sheet_arg = f", sheet='{s}'"

    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_excel('{path_sql}'{sheet_arg})")
    rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    cols = [c[0] for c in con.execute(f"DESCRIBE {table_name}").fetchall()]
    return {"table": table_name, "rows": rows, "columns": cols}
