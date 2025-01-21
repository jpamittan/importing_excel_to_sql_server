Exporting Excel file to SQL server database

Key features of this script:

1.  Supports both row-by-row and batch import modes
2.  Includes error handling and validation
3.  Progress reporting during import
4.  Configuration management using JSON file
5.  Command-line interface for easy usage
6.  Supports any Excel file structure (automatically uses column names)

Important notes:

1. Ensure your SQL Server table structure matches your Excel file columns
2. The script assumes the first row of your Excel file contains column headers
3. For large files, use batch mode (-b flag) for better performance
4. Make sure you have appropriate permissions on the SQL Server database
5. Store sensitive database credentials securely in the config file

The script includes progress logging (log.txt) and error handling to help you track the import process and troubleshoot any issues that might arise.

Prerequisite:
Install latest Python 3.12.x

Setup:

1.  Open config.json file. Change the database connection and user authentication credentials.

2.  Open Command Prompt/PowerShell/Terminal. Change directory to the project folder

    cd path_to_project_folder/import_excel_sql

3.  Create Python virtual environment.

    python -m venv .venv

5.  Activating virtual environment.

    source .venv/Scripts/activate

6.  To install dependencies.

    pip install -r requirements.txt

8.  Executing the script via Command Prompt/PowerShell/Terminal to start importing.

    Arguments:
    
    -e | --excel : Path location of excel file tobe imported to SQL Server.

    -t | --table : Target table name where to be inserted.

    -b | --batch : Enable batch mode upload.

    a. Importing by row.

        python import_excel_sql.py -e ./sample/persons.xlsx -t persons

    b. Importing by batch

        python import_excel_sql.py -e ./sample/persons.xlsx -t persons -b
