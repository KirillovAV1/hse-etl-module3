import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pymongo import MongoClient

MONGO_URI = "mongodb://mongo:27017/"
MONGO_DB = "etl_mongo_db"


def extract_mongo_data(**context):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    sessions = list(db["user_sessions"].find({}, {"_id": 0}))
    events = list(db["event_logs"].find({}, {"_id": 0}))
    tickets = list(db["support_tickets"].find({}, {"_id": 0}))

    for row in sessions:
        row["start_time"] = serialize_dt(row.get("start_time"))
        row["end_time"] = serialize_dt(row.get("end_time"))

    for row in events:
        row["timestamp"] = serialize_dt(row.get("timestamp"))

    for row in tickets:
        row["created_at"] = serialize_dt(row.get("created_at"))
        row["updated_at"] = serialize_dt(row.get("updated_at"))
        for msg in row.get("messages", []):
            msg["timestamp"] = serialize_dt(msg.get("timestamp"))

    context["ti"].xcom_push(key="sessions_raw", value=sessions)
    context["ti"].xcom_push(key="events_raw", value=events)
    context["ti"].xcom_push(key="tickets_raw", value=tickets)


def transform_sessions(**context):
    sessions = context["ti"].xcom_pull(task_ids="extract_mongo_data", key="sessions_raw")

    result = []
    for row in sessions:
        result.append({
            "session_id": row["session_id"],
            "user_id": row["user_id"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "pages_visited": json.dumps(row.get("pages_visited", []), ensure_ascii=False),
            "device": row.get("device"),
            "actions": json.dumps(row.get("actions", []), ensure_ascii=False),
        })

    context["ti"].xcom_push(key="sessions_transformed", value=result)


def transform_events(**context):
    events = context["ti"].xcom_pull(task_ids="extract_mongo_data", key="events_raw")

    result = []
    for row in events:
        result.append({
            "event_id": row["event_id"],
            "event_time": row["timestamp"],
            "event_type": row["event_type"],
            "details": json.dumps(row.get("details"), ensure_ascii=False),
        })

    context["ti"].xcom_push(key="events_transformed", value=result)


def transform_tickets(**context):
    tickets = context["ti"].xcom_pull(task_ids="extract_mongo_data", key="tickets_raw")

    result = []
    for row in tickets:
        result.append({
            "ticket_id": row["ticket_id"],
            "user_id": row["user_id"],
            "status": row["status"],
            "issue_type": row["issue_type"],
            "messages": json.dumps(row.get("messages", []), ensure_ascii=False),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        })

    context["ti"].xcom_push(key="tickets_transformed", value=result)


def load_sessions_to_postgres(**context):
    rows = context["ti"].xcom_pull(task_ids="transform_sessions", key="sessions_transformed")
    if not rows:
        return

    pg_hook = PostgresHook(postgres_conn_id="hse")


    stmt = """
    INSERT INTO user_sessions (
        session_id, user_id, start_time, end_time, pages_visited, device, actions
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (session_id) DO UPDATE SET
        user_id = EXCLUDED.user_id,
        start_time = EXCLUDED.start_time,
        end_time = EXCLUDED.end_time,
        pages_visited = EXCLUDED.pages_visited,
        device = EXCLUDED.device,
        actions = EXCLUDED.actions
    """

    values = [
        (
            row["session_id"],
            row["user_id"],
            row["start_time"],
            row["end_time"],
            row["pages_visited"],
            row["device"],
            row["actions"],
        )
        for row in rows
    ]

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt, values)
        conn.commit()


def load_events_to_postgres(**context):
    rows = context["ti"].xcom_pull(task_ids="transform_events", key="events_transformed")
    if not rows:
        return

    pg_hook = PostgresHook(postgres_conn_id="hse")

    stmt = """
    INSERT INTO event_logs (
        event_id, event_time, event_type, details
    )
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (event_id) DO UPDATE SET
        event_time = EXCLUDED.event_time,
        event_type = EXCLUDED.event_type,
        details = EXCLUDED.details
    """

    values = [
        (
            row["event_id"],
            row["event_time"],
            row["event_type"],
            row["details"],
        )
        for row in rows
    ]

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt, values)
        conn.commit()


def load_tickets_to_postgres(**context):
    rows = context["ti"].xcom_pull(task_ids="transform_tickets", key="tickets_transformed")
    if not rows:
        return

    pg_hook = PostgresHook(postgres_conn_id="hse")

    stmt = """
    INSERT INTO support_tickets (
        ticket_id, user_id, status, issue_type, messages, created_at, updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticket_id) DO UPDATE SET
        user_id = EXCLUDED.user_id,
        status = EXCLUDED.status,
        issue_type = EXCLUDED.issue_type,
        messages = EXCLUDED.messages,
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at
    """

    values = [
        (
            row["ticket_id"],
            row["user_id"],
            row["status"],
            row["issue_type"],
            row["messages"],
            row["created_at"],
            row["updated_at"],
        )
        for row in rows
    ]

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt, values)
        conn.commit()


def serialize_dt(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return value


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
        dag_id="mongo_to_postgres_etl",
        default_args=default_args,
        schedule='@daily',
        catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id="extract_mongo_data",
        python_callable=extract_mongo_data,
    )

    transform_sessions_task = PythonOperator(
        task_id="transform_sessions",
        python_callable=transform_sessions,
    )

    transform_events_task = PythonOperator(
        task_id="transform_events",
        python_callable=transform_events,
    )

    transform_tickets_task = PythonOperator(
        task_id="transform_tickets",
        python_callable=transform_tickets,
    )

    load_sessions_task = PythonOperator(
        task_id="load_sessions_to_postgres",
        python_callable=load_sessions_to_postgres,
    )

    load_events_task = PythonOperator(
        task_id="load_events_to_postgres",
        python_callable=load_events_to_postgres,
    )

    load_tickets_task = PythonOperator(
        task_id="load_tickets_to_postgres",
        python_callable=load_tickets_to_postgres,
    )

    extract_task >> [
        transform_sessions_task,
        transform_events_task,
        transform_tickets_task,
    ]

    transform_sessions_task >> load_sessions_task
    transform_events_task >> load_events_task
    transform_tickets_task >> load_tickets_task
