from dagster import Definitions
from pipelines.adjacency_data_pipeline import adjacency_data_pipeline
from schedules.daily_schedule import daily_adjacency_data_pipeline_schedule

# Define the jobs and schedules that are part of the Dagster pipeline execution environment.
all_definitions = Definitions(
    jobs=[adjacency_data_pipeline],
    schedules=[daily_adjacency_data_pipeline_schedule],
)
