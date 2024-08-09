import os
from dagster import job, op, resource
from sqlalchemy import create_engine
import pandas as pd


@resource(config_schema={"file_path": str, "adjacency_sheet_name": str, "actors_sheet_name": str})
def file_resource(init_context):
    """
    A Dagster resource that provides the file path to an Excel (.xlsx) file and the names of sheets to be read.
    This resource is used to supply the path of the Excel file along with the names of the sheets
    that will be read in other parts of the pipeline.
    :param init_context: The initialization context that provides access to the resource configuration.
    :return: dict: A dictionary containing:
            - "file_path": str: The path to the Excel (.xlsx) file.
            - "adjacency_sheet_name": str: The name of the adjacency matrix sheet.
            - "actors_sheet_name": str: The name of the actors sheet.
    """
    # Read file_path and sheet names from resource configuration
    file_path = init_context.resource_config["file_path"]
    adjacency_sheet_name = init_context.resource_config["adjacency_sheet_name"]
    actors_sheet_name = init_context.resource_config["actors_sheet_name"]

    # Validate file_path has been provided.
    # TODO: Validate if file_path exists.
    if not file_path:
        raise ValueError("file_path cannot be empty")

    # Create and return file_resource dictionary
    return {
        "file_path": file_path,
        "adjacency_sheet_name": adjacency_sheet_name,
        "actors_sheet_name": actors_sheet_name
    }


@resource(config_schema={"var_name": str})
def db_resource(init_context):
    """
    A Dagster resource that provides a SQLAlchemy engine for connecting to a MySQL database.
    This resource retrieves the MySQL connection string from an environment variable, specified by the `var_name`
    It then creates and returns a SQLAlchemy engine that can be used to interact with the MySQL database.
    :param init_context: The initialization context that provides access to the resource configuration.
    :return: sqlalchemy.engine.Engine: A SQLAlchemy engine connected to the MySQL database.
    """
    # Retrieve environment variable name from resource configuration
    var_name = init_context.resource_config["var_name"]

    # Read environment variable
    connection_string = os.getenv(var_name)

    # Validate environment variable has been set up
    if not connection_string:
        raise ValueError(f"Environment variable {var_name} not set")

    # Create and return SQLAlchemy engine
    return create_engine(connection_string)


@op(required_resource_keys={"file_resource"})
def extract_actors_data_local_file(context) -> pd.DataFrame:
    """
    Reads actors data from a specified sheet in an Excel (.xlsx) file and returns it as a DataFrame.
    :param context: The execution context provided by Dagster. It includes access to resources and logging capabilities.
    The `file_resource` resource is used to obtain configuration values such as the file path and sheet name.
    :return: pd.DataFrame: A DataFrame containing the actors data read from the specified sheet in the Excel file.
    """
    # Get file_path and corresponding sheet_name from resource
    file_path = context.resources.file_resource["file_path"]
    sheet_name = context.resources.file_resource["actors_sheet_name"]
    context.log.info(f"Reading {sheet_name} sheet from {file_path}")

    # Read Excel file and corresponding sheet
    # NOTE: File provided starts from row 4 and has no column names. The following line adapts to that scenario,
    # but should be validated with the Data team, if this will always be the case.
    # Since columns have no headers in the file provided, they are explicitly named to match MySql table schema
    actors_df = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl", skiprows=3,
                              names=["id_num", "id", "name"])
    return actors_df


@op(required_resource_keys={"db_resource"})
def load_actors_data(context, df: pd.DataFrame):
    """
    Loads actors data from a DataFrame into MySQL database table 'actors'.
    :param context: The execution context provided by Dagster, which includes access to resources and logging
    capabilities.
    :param df: The DataFrame containing actors data to be loaded into the database. The DataFrame should have columns
    matching the schema of the 'actors' table.
    :return:
    """
    # Get the database engine from the resource
    engine = context.resources.db_resource
    context.log.info("Loading actors data")

    # Load data into 'actors' table.
    # NOTE: Argument if_exists="replace" will drop the table before inserting new values.
    # Change to "append" if values will be inserted to the existing table.
    df.to_sql("actors", con=engine, if_exists="replace", index=False)
    context.log.info("Actors data loaded successfully")


@op(required_resource_keys={"file_resource"})
def extract_adjacency_data_local_file(context) -> pd.DataFrame:
    """
    Extracts adjacency matrix data from a specified Excel (.xlsx) file and returns it as a DataFrame.
    :param context: The execution context provided by Dagster.
    :return: pd.DataFrame: A DataFrame containing the adjacency matrix data from the "Matriz de adyacencia" sheet.
    The DataFrame columns are renamed for consistency, with 'Unnamed: 0' changed to 'id_num' and
    'Unnamed: 1' changed to 'id' to match MySql table schema.
    """
    # Get file_path and corresponding sheet_name from resource
    file_path = context.resources.file_resource["file_path"]
    sheet_name = context.resources.file_resource["adjacency_sheet_name"]
    context.log.info(f"Reading {sheet_name} sheet from {file_path}")

    # Read Excel file and corresponding sheet
    # NOTE: argument header is set up to row 1, since that is the row that contains the id for each actor.
    adjacency_df = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl", header=1)

    # Rename columns to match table schema
    # NOTE: Excel file provided has no name in columns A and B, therefore they are renamed to match table schema.

    adjacency_df.rename(columns={'Unnamed: 0': 'id_num', 'Unnamed: 1': 'id'}, inplace=True)
    return adjacency_df


@op()
def transform_adjacency_data(context, df: pd.DataFrame):
    """
    Method transforms adjacency matrix data by converting rows with columns containing the value 1
    into a DataFrame suitable for insertion into MySql database adjacency_list.
    :param _: The execution context provided by Dagster, which is not used.
    :param df: The input DataFrame containing adjacency matrix data.
    :return: DataFrame containing pairs of nodes where the value is 1 in the original DataFrame.
            Each row in the resulting DataFrame has two columns:
            - 'from_node_id': The value from the 'id' column in the original DataFrame.
            - 'to_node_id': The column name in the original DataFrame where the value is 1.
    """
    # Prepare a list to hold data for insertion
    data_for_insertion = []

    # Process each row and column in the original DataFrame
    # TODO: Code below can be optimized for large adjacency matrix.
    for _, row in df.iterrows():
        row_name = row['id']
        # Prepare a tuple for each column where value is 1
        for col_name in df.columns:
            if col_name not in ['id_num', 'id']:
                if row[col_name] == 1:
                    data_for_insertion.append((row_name, col_name))

    # Create a DataFrame from the data to insert
    insertion_df = pd.DataFrame(data_for_insertion, columns=['from_node_id', 'to_node_id'])
    return insertion_df


@op(required_resource_keys={"db_resource"})
def load_adjacency_data(context, df: pd.DataFrame):
    """
    Loads adjacency data into a MySql table 'adjacency_list'. If the table already exists, it will be replaced.
    :param context: The execution context provided by Dagster
    :param df: The DataFrame containing adjacency data to be loaded into the database.
            The DataFrame must be structured appropriately for insertion into the 'adjacency_list' table.
    :return: None: This function does not return any value.
    """
    # Get the database engine from the resource
    engine = context.resources.db_resource
    context.log.info("Loading adjacency_list data")

    # Load data into 'adjacency_list' table
    # NOTE: If table already exists, it will be replaced.
    # Change to "append" if values will be inserted to the existing table.
    df.to_sql("adjacency_list", con=engine, if_exists="replace", index=False)
    context.log.info("Adjacency data loaded successfully")


@job(resource_defs={"file_resource": file_resource, "db_resource": db_resource},
     config={
         "resources": {
             "file_resource": {
                 "config": {
                     "file_path": "/home/mauricio/PycharmProjects/dagsterProject/Matriz_de_adyacencia_data_team.xlsx",
                     "adjacency_sheet_name": "Matriz de adyacencia",
                     "actors_sheet_name": "Lista de actores"
                 }
             },
             "db_resource": {
                 "config": {
                     "var_name": "MYSQL_DB_CONN_STRING"
                 }
             }
         }
     })
def adjacency_data_pipeline():
    """
    Defines a Dagster job to process and load adjacency data from an Excel file into a MySQL database.
    :return: None: This job does not return any value
    """
    # Extract actor data from the specified Excel file.
    actors_df = extract_actors_data_local_file()

    # Load the extracted actor data into the 'actors' table in the MySQL database.
    load_actors_data(actors_df)

    # Extract adjacency data from the Excel file.
    adjacency_df = extract_adjacency_data_local_file()

    # Transform the adjacency data to a format suitable for insertion.
    insertion_df = transform_adjacency_data(adjacency_df)

    # Load the transformed adjacency data into the 'adjacency_list' table in the MySQL database.
    load_adjacency_data(insertion_df)
    return
