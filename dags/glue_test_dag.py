import boto3
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def create_crawler():
    client = boto3.Session().client('glue', region_name= 'us-east-1')
    test_name ='ec2_test_1'
    client.create_crawler(
        Name=test_name,
        Role='service-role/AWSGlueServiceRole-midterm_crawler_role',
        DatabaseName=test_name,
        Targets={
            'S3Targets':[
                {
                    'Path': 's3://sammy-midterm-output/output_12-8_10-06/'
                }
            ]
        }
    )

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['ahmad.sammy.a@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG(
    'glue_test_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval=None
)

create_crawler = PythonOperator(
        task_id='create_crawler',
        python_callable=create_crawler,
        dag=dag
    )

create_crawler