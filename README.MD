### Report on Task Evaluation

#### Initial Task

**Create a DAG in Airflow (Cloud Composer) that monitors the "spark_cluster" pool and adjusts the Spark (Dataproc) cluster based on the "Scheduled Slots".**

**Job Details:**
- **Resource Requirements:** The jobs are data-intensive. Some processes require substantial RAM, while others have minimal RAM requirements.
- **Job Volume:** Approximately 3,000 jobs need to be executed within a 4-hour window.
- **Data Handling:** Each job processes data from around 10 different files, with each file potentially being up to 1 GB in size.
- **Cool Down Period:** There are intervals between jobs, so a cool-down period of a few minutes must be accounted for.
- **Cluster Availability:** The DAG must be capable of creating the cluster at any time and delete it when not in use.

**Requirements:**
- **Python Package Versions:** It must be possible to specify the versions of various Python packages used in the cluster. These versions should be sourced from Airflow variables.
- **Cluster Specifications:** Any cluster-specific configurations must also be sourced from Airflow variables.

**Questions:**
- **Scalability:** Is this an effective method for scaling Spark clusters?
- **Pros and Cons:** What are the advantages and disadvantages of this approach?

---

### Evaluation of Completed Work

#### DAG Implementation

1. **Monitoring the "spark_cluster" Pool:**
   - Implemented in `cluster_runner_dag.py` using the `Pool` class to get the slots and monitor the "spark_cluster" pool.
   - **Code:**
     ```python
     def get_pool_slots(**kwargs):
         pool = Pool.get_pool('spark_cluster')
         slots = pool.slots
         kwargs['ti'].xcom_push(key='pool_slots', value=slots)
     ```

2. **Adjusting the Spark (Dataproc) Cluster Based on Scheduled Slots:**
   - Implemented in `cluster_runner_dag.py` by dynamically generating the cluster configuration based on the number of slots.
   - **Code:**
     ```python
     def generate_cluster_config(**kwargs):
         slots = kwargs['ti'].xcom_pull(task_ids='get_slots', key='pool_slots')
         master_config = json.loads(Variable.get('master_config'))
         worker_config = json.loads(Variable.get('worker_config'))
         worker_config['num_instances'] = slots
     ```

3. **Job Handling:**
   - `spark_on_dataproc_dag.py` handles the submission of Spark jobs using `DataprocSubmitJobOperator`.
   - **Code:**
     ```python
     submit_job_1 = DataprocSubmitJobOperator(
         task_id=f'submit_job_1_{i}',
         job=SPARK_JOB,
         region=REGION,
         project_id=PROJECT_ID,
         deferrable=True,
     )
     ```

4. **Cool Down Period:**
   - Implemented in `spark_on_dataproc_dag.py` using `TimeDeltaSensorAsync` to account for intervals between jobs.
   - **Code:**
     ```python
     delay_after_task_1 = TimeDeltaSensorAsync(
         task_id=f'delay_after_task_1_{i}',
         delta=timedelta(seconds=20),
     )
     ```

5. **Cluster Creation and Deletion:**
   - Implemented in `cluster_runner_dag.py` using `DataprocCreateClusterOperator` and `DataprocDeleteClusterOperator`.
   - **Code:**
     ```python
     create_cluster = DataprocCreateClusterOperator(
         task_id='create_cluster',
         project_id=PROJECT_ID,
         region=REGION,
         cluster_name=CLUSTER_NAME,
         cluster_config=generate_cluster_config_task.output,
     )

     delete_cluster = DataprocDeleteClusterOperator(
         task_id='delete_cluster',
         project_id=PROJECT_ID,
         cluster_name=CLUSTER_NAME,
         region=REGION,
     )
     ```

6. **Python Package Versions:**
   - Versions are sourced from Airflow variables and included in the cluster initialization.
   - **Variable file: `python_package_versions.json`**:
     ```json
     {
       "mock": "5.1.0"
     }
     ```
   - **Code:**
     ```python
     python_package_versions = Variable.get(
         'python_package_versions', deserialize_json=True
     )
     pip_packages = ' '.join(
         [
             f'{pkg}=={version}'
             for pkg, version in python_package_versions.items()
         ]
     )
     ```

7. **Cluster Specifications:**
   - Sourced from Airflow variables and used to configure the cluster dynamically.
   - **Variable file: `cluster_config.json`**:
     ```json
     {
       "master_config": {
         "num_instances": 1,
         "machine_type_uri": "n1-standard-2",
         "disk_config": {
           "boot_disk_type": "pd-standard",
           "boot_disk_size_gb": 32
         }
       },
       "worker_config": {
         "num_instances": 2,
         "machine_type_uri": "n1-standard-4",
         "disk_config": {
           "boot_disk_type": "pd-standard",
           "boot_disk_size_gb": 32
         }
       }
     }
     ```

#### Calculations

1. **Number of jobs executed by one worker node in 4 hours** (assuming average job completion time is 5 minutes):
   - Number of jobs per node = Total time interval / Average time per job
   - Number of jobs per node = 240 minutes / 5 minutes = 48 jobs

2. **Total number of required worker nodes**:
   - Total number of worker nodes = Total number of jobs / Number of jobs per node
   - Total number of worker nodes = 3000 jobs / 48 jobs ≈ 63 nodes

3. **Physical memory required for each job**:
   - Memory per job = 10 GB

4. **Number of jobs that can run concurrently on one node considering memory**:
   - Concurrent jobs per node = 64 GB (node RAM) / 10 GB (memory per job)
   - Concurrent jobs per node = 64 / 10 ≈ 6 jobs

5. **Recalculation of the total number of worker nodes considering parallelism**:
   - Number of jobs per node in 4 hours = 48
   - Number of nodes considering parallelism = Total number of jobs / (Number of jobs per node * Concurrent jobs per node)
   - Number of nodes considering parallelism = 3000 / (48 * 6) ≈ 10.42 ≈ 11 nodes

#### Final Configuration

- **Master Configuration**:
  - Number of instances: 3
  - Machine type: `n2-highmem-8`
  - Disk type: `pd-ssd`
  - Disk size: 50 GB

- **Worker Configuration**:
  - Number of instances: 11
  - Machine type: `n2-highmem-8`
  - Disk type: `pd-ssd`
  - Disk size: 50 GB

- **Configuration file: `task_cluster_configuration.json`**:
  ```json
  {
    "master_config": {
      "num_instances": 3,
      "machine_type_uri": "n2-highmem-8",
      "disk_config": {
        "boot_disk_type": "pd-ssd",
        "boot_disk_size_gb": 50
      }
    },
    "worker_config": {
      "num_instances": 11,
      "machine_type_uri": "n2-highmem-8",
      "disk_config": {
        "boot_disk_type": "pd-ssd",
        "boot_disk_size_gb": 50
      }
    }
  }
  ```

### Summary

- **Number of jobs**: 3000 jobs
- **Total time interval**: 240 minutes
- **Average time to complete one job**: assumed to be 5 minutes
- **Data volume for each job**: 10 GB
- **Machine type**: `n2-highmem-8` (8 vCPU, 64 GB RAM)
- **Number of concurrent jobs per node**: 6 jobs
- **Total number of worker nodes**: 11 nodes
- **Number of master nodes**: 3 nodes for increased reliability and load distribution

These calculations and configurations ensure the required performance and reliability for processing all jobs within the specified time frame. The average time to complete one job is assumed and requires validation with real data.

### Pros and Cons

**Pros:**
- **Scalability:** The solution effectively scales the Spark cluster based on workload demands.
- **Resource Efficiency:** Dynamically adjusts resources to meet job requirements, optimizing costs.
- **Flexibility:** Allows specification of Python package versions and cluster configurations via Airflow variables.

**Cons:**
- **Complexity:** Requires careful setup and configuration of Airflow DAGs and variables.
- **Cluster Initialization Time:** Time to create and delete clusters may impact overall job completion time.
- **Reliability:** Potential for errors in cluster creation/deletion processes, which could disrupt job execution.