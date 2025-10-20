@echo off
REM Windows batch script to create DuckDB database from data files
REM Usage: create_duckdb.bat <folder_name>
REM Example: create_duckdb.bat Kelmarsh

if "%1"=="" (
    echo Error: Missing folder name argument
    echo Usage: create_duckdb.bat ^<folder_name^>
    echo Example: create_duckdb.bat Kelmarsh
    exit /b 1
)

set FOLDER_NAME=%1

echo Creating DuckDB database for folder: %FOLDER_NAME%
echo.

python scripts\create_duckdb.py --folder %FOLDER_NAME%

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Success! Database created.
) else (
    echo.
    echo Error: Database creation failed.
    exit /b %ERRORLEVEL%
)
