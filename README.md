# Adjacency Data Pipeline

This repository contains a Dagster pipeline that processes and loads adjacency data from an Excel file into a MySQL database.
The pipeline is defined using a series of Dagster resources and operations, making it easy to manage data extraction, transformation, and loading (ETL) tasks, as well as a scheduled job to run the pipeline daily.


## Project Structure


├── pipelines
├──── pipleines/adjacency_data_pipeline.py  # Main pipeline definition and operations
├── schedules
├──── schedules/daily_schedule.py           # Dagster schedule configuration
├── main.py                                 # Entry point for Dagster jobs and schedules
└── workspace.yaml                          # Dagster workspace configuration

## Project Structure

- **Resources**:
  - `file_resource`: Provides the file path and sheet names from an Excel (.xlsx) file containing the data to be processed.
  - `db_resource`: Creates a SQLAlchemy engine for connecting to a MySQL database.

- **Operations**:
  - `extract_actors_data_local_file`: Extracts actors data from the specified sheet in the Excel file and returns it as a DataFrame.
  - `load_actors_data`: Loads the extracted actors data into the `actors` table in the MySQL database.
  - `extract_adjacency_data_local_file`: Extracts adjacency matrix data from the specified sheet in the Excel file and returns it as a DataFrame.
  - `transform_adjacency_data`: Transforms the adjacency matrix data into a format suitable for insertion into the MySQL database.
  - `load_adjacency_data`: Loads the transformed adjacency data into the `adjacency_list` table in the MySQL database.

- **Job**:
  - `adjacency_data_pipeline`: Orchestrates the entire process by executing the defined operations in sequence.

- **Schedule**:
  - `daily_adjacency_data_pipeline_schedule`: A Dagster schedule that triggers the adjacency_data_pipeline job to run daily at 9 AM (Mexico City timezone).

## Getting Started

### Prerequisites

- **Python 3.8+**
- **Dagster**
- **SQLAlchemy**
- **Pandas**
- **MySQL Database**

### Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/macma80/dagsterProject.git
    cd dagsterProject
    ```

2. *TODO this step*. Install the required Python packages: 
    ```bash
    pip install -r requirements.txt
    ```

3. Ensure that your MySQL database is up and running. Set the `MYSQL_DB_CONN_STRING` environment variable with the connection string to your MySQL database.

### Configuration

- Pipeline Configuration
    The pipeline is configured to use the following settings:

    1. File Resource Configuration:
        file_path: Path to the Excel file containing the adjacency matrix and actor data.
        adjacency_sheet_name: Name of the sheet containing the adjacency matrix data.
        actors_sheet_name: Name of the sheet containing the actor data.

    2. Database Resource Configuration:
        var_name: Name of the environment variable that stores the MySQL connection string.

### Schedule Configuration
The pipeline is scheduled to run daily at 9 AM (Mexico City timezone) using the following cron expression:
0 9 * * *

### Running the Pipeline

You can run the pipeline using Dagster's CLI or UI.

1. Run the pipeline using ...:
    ```bash
    dagster pipeline execute -f main.py
    ```

2. Alternatively, you can use the Dagster UI to run and monitor the pipeline.

### Pipeline Flow

1. **Extract Actor Data**: Reads actor data from the Excel file
2. **Load Actor Data**: Loads the extracted data into the `actors` table in the MySQL database.
3. **Extract Adjacency Data**: Reads the adjacency matrix data from the Excel file.
4. **Transform Adjacency Data**: Transforms the extracted adjacency data to a format suitable for database insertion.
5. **Load Adjacency Data**: Loads the transformed data into the `adjacency_list` table in the MySQL database.

## Notes

- The pipeline assumes that the Excel file has a specific structure (e.g., certain rows and columns are skipped, and column names are manually defined). Ensure that your input files match these expectations.
- The database operations replace existing tables by default. If you want to append data instead, modify the `if_exists` parameter in the `to_sql` method calls.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue if you find any bugs or have suggestions for improvement.

## License

This project is licensed under the MIT License. 
