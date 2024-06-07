from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import \
    DataprocSubmitJobOperator
from airflow.sensors.time_delta import TimeDeltaSensorAsync
from airflow.utils.dates import days_ago
from airflow.models import Pool

PROJECT_ID = 'PROJECT_ID'
REGION = 'REGION'
CLUSTER_NAME = "{{ dag_run.conf['cluster_name'] }}"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

SPARK_JOB = {
    'reference': {'project_id': PROJECT_ID},
    'placement': {'cluster_name': CLUSTER_NAME},
    'spark_job': {
        'jar_file_uris': [
            'file:///usr/lib/spark/examples/jars/spark-examples.jar'
        ],
        'main_class': 'org.apache.spark.examples.SparkPi',
    },
}

with DAG(
        'spark_on_dataproc_dag',
        default_args=default_args,
        description='A DAG to run multiple Spark jobs '
                    'on an existing Dataproc cluster',
        schedule_interval=None,
        catchup=False,
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    pool_name = 'spark_cluster'
    pool = Pool.get_pool(pool_name)
    slots = pool.slots

    for i in range(slots):

        submit_job_1 = DataprocSubmitJobOperator(
            task_id=f'submit_job_1_{i}',
            job=SPARK_JOB,
            region=REGION,
            project_id=PROJECT_ID,
            deferrable=True,
        )

        delay_after_task_1 = TimeDeltaSensorAsync(
            task_id=f'delay_after_task_1_{i}',
            delta=timedelta(seconds=20),
        )

        submit_job_2 = DataprocSubmitJobOperator(
            task_id=f'submit_job_2_{i}',
            job=SPARK_JOB,
            region=REGION,
            project_id=PROJECT_ID,
            deferrable=True,
        )

        delay_after_task_2 = TimeDeltaSensorAsync(
            task_id=f'delay_after_task_2_{i}',
            delta=timedelta(seconds=20),
        )

        start >> submit_job_1 >> delay_after_task_1 >> submit_job_2 >> delay_after_task_2 >> end
