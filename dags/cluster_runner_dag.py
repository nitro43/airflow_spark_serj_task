from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models import Variable, Pool
import json

PROJECT_ID = 'myapi-383419'
CLUSTER_NAME = 'test-cluster-zero'
REGION = 'us-central1'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_pool_slots(**kwargs):
    pool = Pool.get_pool('spark_cluster')
    slots = pool.slots
    kwargs['ti'].xcom_push(key='pool_slots', value=slots)


def generate_cluster_config(**kwargs):
    slots = kwargs['ti'].xcom_pull(task_ids='get_slots', key='pool_slots')
    master_config = json.loads(Variable.get('master_config'))
    worker_config = json.loads(Variable.get('worker_config'))
    worker_config['num_instances'] = slots

    python_package_versions = Variable.get(
        'python_package_versions', deserialize_json=True
    )
    pip_packages = ' '.join(
        [
            f'{pkg}=={version}'
            for pkg, version in python_package_versions.items()
        ]
    )

    cluster_config = ClusterGenerator(
        project_id=PROJECT_ID,
        region=REGION,
        master_machine_type=master_config['machine_type_uri'],
        master_disk_size=master_config['disk_config']['boot_disk_size_gb'],
        worker_machine_type=worker_config['machine_type_uri'],
        worker_disk_size=worker_config['disk_config']['boot_disk_size_gb'],
        num_workers=worker_config['num_instances'],
        init_actions_uris=[
            f'gs://goog-dataproc-initialization-actions-{REGION}/python/pip-install.sh'
        ],
        metadata={'PIP_PACKAGES': pip_packages},
        num_preemptible_workers=0,
    ).make()

    return cluster_config


with DAG(
    'cluster_runner',
    default_args=default_args,
    description='A DAG to create a cluster, trigger another DAG, '
                'and then delete the cluster',
    schedule_interval=None,
    catchup=False,
) as dag:

    get_slots = PythonOperator(
        task_id='get_slots',
        python_callable=get_pool_slots,
        provide_context=True,
    )

    generate_cluster_config_task = PythonOperator(
        task_id='generate_cluster_config',
        python_callable=generate_cluster_config,
        provide_context=True,
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=generate_cluster_config_task.output,
    )

    trigger_spark_dag = TriggerDagRunOperator(
        task_id='trigger_spark_dag',
        trigger_dag_id='spark_on_dataproc_dag',
        conf={'cluster_name': CLUSTER_NAME, 'cluster_slots': get_slots.output},
        wait_for_completion=True,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )

    (
        get_slots
        >> generate_cluster_config_task
        >> create_cluster
        >> trigger_spark_dag
        >> delete_cluster
    )
