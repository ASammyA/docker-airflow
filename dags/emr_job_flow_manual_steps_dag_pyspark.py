# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import timedelta

import airflow
from airflow import DAG
# from airflow.contrib.operators.emr_create_job_flow_operator \
#         import EmrCreateJobFlowOperator
# from airflow.contrib.operators.emr_add_steps_operator \
#         import EmrAddStepsOperator
# from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
# from airflow.contrib.operators.emr_terminate_job_flow_operator \
#         import EmrTerminateJobFlowOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from datetime import datetime
from datetime import date

# import sys
# sys.path.append("/usr/local/airflow/plugins")
# from get_emr_available_cluster import get_avilable_cluster_id

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['ahmad.sammy.a@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

# CLUSTER_ID = ''

def retrieve_s3_file(**kwargs):
    s3_location = kwargs['dag_run'].conf['s3_location'] 
    kwargs['ti'].xcom_push( key = 's3location', value = s3_location)

# record current time to include in file name
def record_time(**kwargs):
    current_time = datetime.now().strftime("%H-%M")
    timestamp = '_' + str(date.today().month) + '-' + str(date.today().day) + '_' + str(current_time)
    kwargs['ti'].xcom_push( key = 'timestamp', value = timestamp)

SPARK_STEPS = [
    {
        'Name': 'datajob',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit', 
                '--master', 'yarn',
                '--deploy-mode','cluster',
                '--num-executors','2',
                '--driver-memory','512m',
                '--executor-memory','3g',
                '--executor-cores','2',
                '--py-files', 's3://sammy-midterm-code/job.zip',
                's3://sammy-midterm-code/workflow_entry.py',
                '-p', "{'input_path':'{{ task_instance.xcom_pull('get_s3_file_path', key='s3location') }}','name':'demo', 'file_type':'txt', 'output_path': 's3://sammy-midterm-output/output{{ task_instance.xcom_pull('record_time', key='timestamp') }}/', 'partition_column': 'job'}"
            ]
        }
    }
]

# /usr/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 2 --driver-memory 512m --executor-memory 3g --executor-cores 2 --py-files s3://sammy-midterm-code/job.zip s3://sammy-midterm-code/workflow_entry.py -p "{'input_path':'s3://sammy-de-midterm/banking.csv','name':'demo', 'file_type':'txt', 'output_path':'s3://sammy-midterm-output', 'partition_column': 'job'}"

JOB_FLOW_OVERRIDES = {
    'Name': 'Sammy_DE_Midterm_EMR_Cluster',
    'ReleaseLabel': 'emr-6.4.0',
    'Applications': [
        {
            'Name': 'Hadoop'
        },
        {
            'Name': 'Hive'
        },
        {
            'Name': 'Spark'
        }
    ],
    'Instances': {
        "Ec2KeyName": "Sammy_DE_Midterm",
        "InstanceGroups": [
            {
                'Name': 'Primary node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'c5a.xlarge',
                'InstanceCount': 1
            },
            {
                'Name': 'Primary node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'c5a.xlarge',
                'InstanceCount': 2
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'LogUri': 's3://sammy-midterm-emr-logs',
    "Configurations": [
        {
            "Classification":"hive-site", 
            "Properties":{
                "hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            },
            "Configurations":[]
        },
        {
            "Classification":"spark-hive-site", 
            "Properties":{
                "hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            },
            "Configurations":[]
        }
    ],
    'Steps': SPARK_STEPS,
}            

dag = DAG(
    'emr_job_flow_manual_steps_dag_pyspark',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval=None
)

get_s3_file_path = PythonOperator(
        task_id='get_s3_file_path',
        provide_context=True,
        python_callable=retrieve_s3_file,
        dag=dag
    )

record_time = PythonOperator(
        task_id='record_time',
        provide_context=True,
        python_callable=record_time,
        dag=dag
    )

start_emr_cluster = EmrCreateJobFlowOperator(
        task_id='start_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
        dag=dag
    )

check_emr_success = EmrJobFlowSensor(
        task_id='check_emr_success',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='start_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        dag=dag
    )

[get_s3_file_path, record_time] >> start_emr_cluster >> check_emr_success