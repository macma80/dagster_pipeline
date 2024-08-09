# Adjacency Data Pipeline

This repository contains a Dagster pipeline that processes and loads adjacency data from an Excel file into a MySQL database.
The pipeline is defined using a series of Dagster resources and operations, making it easy to manage data extraction, transformation, and loading (ETL) tasks, as well as a scheduled job to run the pipeline daily. Note that this ETL pipeline performs a full load, this is done for three reasons: because of the data characteristics, to simplify the logic and to avoid the potential errors or inconsistencies caused by incremental load. This project currently reads the Excel file from local storage. However, the design is flexible and can be extended in the future to support reading files directly from AWS S3 or other cloud storage solutions. This option is left out for future improvements. 


## Project Structure

```bash
├── main.py                              # Entry point for Dagster jobs and schedules  
│   ├── pipelines                        
│   │   ├── adjacency_data_pipeline.py   # Pipeline job definition and operations
│   ├── schedules
│   │   ├── daily_schedule.py            # Dagster schedule configuration
├── job_config.py                        # Job configuration in JSON format
├── workspace.yaml                       # Dagster workspace configuration
└── README.md
```

- **Resources**:
  - `file_resource`: Provides the file path and sheet names from an Excel (.xlsx) file containing the data to be processed.
  - `db_resource`: Provides the environmental variable name containing MySQL connection string. 

- **Operations**:
  - `extract_nodes_data_local_file`: Extracts nodes data from the specified sheet in the Excel file and returns it as a DataFrame.
  - `load_nodes_data`: Loads the extracted nodes data into the `nodes` table in the MySQL database.
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

2. Install the required Python packages: 
    ```bash
    pip install -r requirements.txt
    ```

3. Ensure that your MySQL database is up and running. 
   1. If it doesn't exist, create MySQL table `nodes`
    ```bash
    CREATE TABLE nodes (
        id CHAR(10) PRIMARY KEY,
        num_id SMALLINT NOT NULL, 
        name VARCHAR(255) NOT NULL
    );
    ```

   2. If it doesn't exist, create MySQL table `adjacency_list`

    ```bash
    CREATE TABLE adjacency_list (
        from_node_id CHAR(10),
        to_node_id CHAR(10),
        weight SMALLINT,
        PRIMARY KEY (from_node_id, to_node_id)
    );
    ```

4. Set the `MYSQL_DB_CONN_STRING` environment variable with the connection string to your MySQL database. 
```
export MYSQL_DB_CONN_STRING=mysql://username:password@hostname:port/database
```
### Configuration

- Pipeline Configuration. Pipeline is configured through file `job_config.py` and has the following settings:

    1. File Resource Configuration:
        1. `file_path`: Path to the Excel file containing the adjacency matrix and nodes data.
        2. `adjacency_sheet_name`: Name of the sheet containing the adjacency matrix data.
        3. `nodes_sheet_name`: Name of the sheet containing the nodes data.

    2. Database Resource Configuration:
        1. `var_name`: Name of the environment variable containing the MySQL connection string.

### Schedule Configuration
The pipeline is scheduled to run daily at 9 AM (Mexico City timezone) using the following cron expression:
`0 9 * * *`

### Running the Pipeline

You can run the pipeline using Dagster's CLI or UI.

1. Run the pipeline using Dagster's CLI:
    ```bash
    dagster pipeline execute -f main.py
    ```

2. Alternatively, you can use the Dagster UI to run and monitor the pipeline.

### Pipeline Flow

1. **Extract Nodes Data**: Performed via `extract_nodes_data_local_file` op.
2. **Load Nodes Data**: Performed via `load_nodes_data` op.
3. **Extract Adjacency Data**: Performed via `load_nodes_data` op.
4. **Transform Adjacency Data**: Performed via `extract_adjacency_data_local_file` op.
5. **Load Adjacency Data**: Performed via `load_adjacency_data` op.

## Notes

- The pipeline assumes that the Excel file has a specific structure (e.g., certain rows and columns are skipped, and column names are manually defined). Ensure that your input files match these expectations.
- Since the ETL performs a full load, the database operations replace existing tables by default. This is done via the `if_exists` parameter in the `to_sql` method calls.


