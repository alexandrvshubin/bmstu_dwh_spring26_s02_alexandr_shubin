from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.timetables.interval import CronDataIntervalTimetable

with DAG(
    dag_id='hw23_scd2_campaigns',
    start_date=pendulum.datetime(2026, 1, 23, tz='UTC'),
    end_date=pendulum.datetime(2026, 1, 25, tz='UTC'),
    schedule=CronDataIntervalTimetable('0 0 * * *', timezone='UTC'),
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={'retries': 1, 'retry_delay': timedelta(seconds=30)},
    dagrun_timeout=timedelta(minutes=10),
) as dag:
    context = {
        'HW_DAG_ID': '{{ dag.dag_id }}',
        'HW_RUN_ID': '{{ run_id }}',
        'HW_TASK_ID': '{{ task.task_id }}',
        'HW_START': '{{ data_interval_start.isoformat() }}',
        'HW_END': '{{ data_interval_end.isoformat() }}',
    }
    scd2 = BashOperator(
        task_id='build_user_scd2',
        bash_command='python /opt/airflow/jobs/pipeline.py scd2',
        env=context,
        append_env=True,
        do_xcom_push=False,
    )
    recipients = BashOperator(
        task_id='build_campaign_recipients',
        bash_command='python /opt/airflow/jobs/pipeline.py recipients',
        env=context,
        append_env=True,
        do_xcom_push=False,
    )
    scd2 >> recipients
