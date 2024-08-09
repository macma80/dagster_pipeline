from dagster import ScheduleDefinition, DefaultScheduleStatus
from pipelines import adjacency_data_pipeline


job_config = {
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
}

# Cron schedule configuration. This cron schedule runs every day at 9 AM
daily_adjacency_data_pipeline_schedule = ScheduleDefinition(
    name="daily_actors_data_pipeline_schedule",
    cron_schedule="0 9 * * *",
    job=adjacency_data_pipeline,
    run_config=job_config,
    execution_timezone="America/Mexico_City",
    default_status=DefaultScheduleStatus.RUNNING,
)
