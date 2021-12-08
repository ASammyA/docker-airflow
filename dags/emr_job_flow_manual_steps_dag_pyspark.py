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
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from datetime import datetime
from datetime import date
import boto3
import ast

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['ahmad.sammy.a@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'provide_context': True
}

def retrieve_s3_file(**kwargs):
    s3_location = kwargs['dag_run'].conf['s3_location'] 
    kwargs['ti'].xcom_push( key = 's3location', value = s3_location)

# record current time to include in file name
def record_time(**kwargs):
    current_time = datetime.now().strftime("%H-%M")
    timestamp = '_' + str(date.today().month) + '-' + str(date.today().day) + '_' + str(current_time)
    kwargs['ti'].xcom_push( key = 'timestamp', value = timestamp)

def stop_ec2():
    session = boto3.Session().client('ec2', region_name= 'us-east-1')
    # ec2_resource = boto3.resource('ec2', region_name= 'us-east-1')
    # instance = ec2_resource.Instance('i-0e0833f712d8d1b8a')
    session.stop_instances(
        InstanceIds=['i-0e0833f712d8d1b8a']
    )

def output_path(**kwargs):
    client = boto3.Session().client('emr', region_name= 'us-east-1')
    # cluster_id = "{{ task_instance.xcom_pull(task_ids='start_emr_cluster', key='return_value') }}"
    # cluster_id = kwargs['task_ids'].xcom_pull( task_ids='start_emr_cluster', key='return_value' )
    # kwargs.get('templates_dict').get('cluster_id')
    # cluster_id=kwargs['ti'].xcom_pull(task_ids='start_emr_cluster')
    cluster_id = kwargs['task_instance'].xcom_pull(task_ids='start_emr_cluster', key='return_value')
    steps = client.list_steps(
       ClusterId=cluster_id
    )
    params = steps['Steps'][0]['Config']['Args'][17]
    params_dict = ast.literal_eval(params)
    path = params_dict['output_path']
    return path

def create_crawler(**kwargs):
    client = boto3.Session().client('glue', region_name= 'us-east-1')
    output_path = kwargs['ti'].xcom_pull(task_ids='get_output_path', key='return_value')
    split = output_path.split('/')
    new_name = split[3]
    client.create_crawler(
        Name=new_name,
        Role='service-role/AWSGlueServiceRole-midterm_crawler_role',
        DatabaseName=new_name,
        Targets={
            'S3Targets':[
                {
                    'Path': output_path
                }
            ]
        }
    )
    kwargs['ti'].xcom_push( key = 'new_name', value = new_name)

def run_crawler(**kwargs):
    client = boto3.Session().client('glue', region_name= 'us-east-1')
    new_name = kwargs['ti'].xcom_pull(task_ids='create_crawler', key='new_name')
    client.start_crawler(
        Name=new_name
    )

def run_superset_ec2():
    client = boto3.Session().client('ec2', region_name= 'us-east-1')
    client.start_instances(
        InstanceIds=['i-0a4c0ce76c11235a0']
    )

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

get_output_path = PythonOperator(
        task_id='get_output_path',
        python_callable=output_path,
        privode_context=True,
        # templates_dict={"cluster_id":"{{ task_instance.xcom_pull(task_ids='start_emr_cluster', key='return_value') }}"},
        dag=dag
    )

create_crawler = PythonOperator(
        task_id='create_crawler',
        python_callable=create_crawler,
        privode_context=True,
        dag=dag
    )

run_crawler = PythonOperator(
        task_id='run_crawler',
        python_callable=run_crawler,
        privode_context=True,
        dag=dag
    )

run_superset_ec2 = PythonOperator(
        task_id='run_superset_ec2',
        python_callable=run_superset_ec2,
        provide_context=False,
        dag=dag
    )

# stop_ec2 = PythonOperator(
#         task_id='stop_ec2',
#         python_callable=stop_ec2,
#         dag=dag
#     )

[get_s3_file_path, record_time] >> start_emr_cluster >> [check_emr_success, get_output_path] >> create_crawler >> run_crawler >> run_superset_ec2 #>> stop_ec2