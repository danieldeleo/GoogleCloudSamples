import datetime
import os

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': 'dannydataproc'
}

with models.DAG(
        'Sqoop-Workflow',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        # Give the cluster a unique name by appending the date scheduled.
        # See https://airflow.apache.org/code.html#default-variables
        cluster_name='sqoop-cluster-{{ ds_nodash }}',
        num_workers=2,
        zone='us-east4-a',
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1',
        init_actions_uris=['gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh'],
        properties={'hive:hive.metastore.warehouse.dir': 'gs://dannydataproc/hive-warehouse'},
        metadata={'enable-cloud-sql-hive-metastore': 'false', 'additional-cloud-sql-instances': 'dannydataproc:us-central1:sqooptest', 'hive-metastore-instance': 'dannydataproc:us-central1:sqooptest'})

    # Run the Hadoop wordcount example installed on the Cloud Dataproc cluster
    # master node.
    run_dataproc_hadoop = dataproc_operator.DataProcHadoopOperator(
        task_id='run_dataproc_hadoop',
        main_class='org.apache.sqoop.Sqoop',
        dataproc_hadoop_jars=['gs://dannydataproc/sqoop-1.4.7-hadoop260.jar','file:///usr/share/java/mysql-connector-java-5.1.42.jar','gs://dannydataproc/avro-tools-1.8.2.jar'],
        arguments=['import', '-Dmapreduce.job.user.classpath.first=true', '--connect=jdbc:mysql://127.0.0.1/guestbook', '--username=sqoop', '--password-file=gs://dannydataproc/passwordFile.txt', '--target-dir=gs://dannydataproc/entities', '--table=entries', '--as-avrodatafile', '--delete-target-dir'],
        cluster_name='sqoop-cluster-{{ ds_nodash }}')

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='sqoop-cluster-{{ ds_nodash }}',
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    # Define DAG dependencies.
    create_dataproc_cluster >> run_dataproc_hadoop >> delete_dataproc_cluster