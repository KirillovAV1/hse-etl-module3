from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator



SQL_BUILD_USER_ACTIVITY = """
TRUNCATE TABLE mart_user_activity;

INSERT INTO mart_user_activity (
    user_id,
    sessions_count,
    avg_session_duration_minutes,
    total_actions
)
SELECT
    user_id,
    COUNT(*) AS sessions_count,
    ROUND(AVG(EXTRACT(EPOCH FROM (end_time - start_time)) / 60.0), 2) AS avg_session_duration_minutes,
    COALESCE(SUM(json_array_length(actions::json)), 0) AS total_actions
FROM user_sessions
GROUP BY user_id;
"""


SQL_BUILD_SUPPORT_STATS = """
TRUNCATE TABLE mart_support_stats;

INSERT INTO mart_support_stats (
    issue_type,
    tickets_count,
    open_tickets,
    avg_resolution_hours
)
SELECT
    issue_type,
    COUNT(*) AS tickets_count,
    SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_tickets,
    ROUND(AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600.0), 2) AS avg_resolution_hours
FROM support_tickets
GROUP BY issue_type;
"""

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="build_analytics_marts",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    build_user_activity = PostgresOperator(
        task_id="build_user_activity_mart",
        postgres_conn_id="hse",
        sql=SQL_BUILD_USER_ACTIVITY,
    )

    build_support_stats = PostgresOperator(
        task_id="build_support_stats_mart",
        postgres_conn_id="hse",
        sql=SQL_BUILD_SUPPORT_STATS,
    )

    [build_user_activity, build_support_stats]