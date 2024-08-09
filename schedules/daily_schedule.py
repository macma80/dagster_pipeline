from dagster import ScheduleDefinition, DefaultScheduleStatus
from pipelines import adjacency_data_pipeline
from job_config import JOB_CONFIG

# Cron schedule configuration. This cron schedule runs every day at 9 AM
daily_adjacency_data_pipeline_schedule = ScheduleDefinition(
    name="daily_adjacency_data_pipeline_schedule",
    cron_schedule="0 9 * * *",
    job=adjacency_data_pipeline,
    run_config=JOB_CONFIG,
    execution_timezone="America/Mexico_City",
    default_status=DefaultScheduleStatus.RUNNING,
)
