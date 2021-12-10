import json
import subprocess
import boto3
import time

def lambda_handler(event, context):
    # TODO implement
    
    records = event['Records'][0]['s3']
    bucket_name = records['bucket']['name']
    file_name = records['object']['key']
    
    process_data = 's3://' + bucket_name + '/' + file_name
    
    print(f"file {process_data} was uploaded. Starting lambda function.")
    
    # Boto3 session for airflow EC2
    session = boto3.Session().client('ec2', region_name= 'us-east-1')
    ec2_resource = boto3.resource('ec2', region_name= 'us-east-1')
    instance = ec2_resource.Instance('i-0e0833f712d8d1b8a')
    
    # If EC2 is not running, start it 
    if instance.state['Name'] != 'running':
        print("Airflow EC2 instance not running. Starting it now")
        session.start_instances(
            InstanceIds=['i-0e0833f712d8d1b8a']
        )
    # Function to check if EC2 is running
    def ec2_running():
        
        instance = ec2_resource.Instance('i-0e0833f712d8d1b8a')

        if instance.state['Name'] == 'running':
            return True
        else:
            return False

    # Loop function to watch EC2 state until it finds it running   
    def watch_ec2(): 
        watching = True  #Initial/controlling condition for the while loop
        seconds_lapsed = 0  #Counter to track how long the run takes
        timeout = 300  #Maximum time in seconds we are allowing the run to take

        while watching:
    
            if ec2_running() == True:
    
                print('Airflow EC2 instance running.')
                watching = False
    
            elif seconds_lapsed == timeout:
    
                #output timeout report
                print("timeout")
    
                #break the loop
                watching = False
            
            elif ec2_running() == False:
    
                #wait 5 seconds and update the counter, continue the loop
                time.sleep(5)
                seconds_lapsed += 5
                m = int(seconds_lapsed / 60)
                s = seconds_lapsed % 60
    
                print(f'Airflow EC2 instance not running yet. Total time lapsed: {m}:{s}')
                continue
    
            else:
    
                #output error
                print("unknown error")
    
                #break the loop
                watching = False
            
    watch_ec2()
    
    # Wait 1 minute to give time for airflow docker to be healthy
    print('waiting for 1 minute to give time for airflow containers to be healthy.')
    time.sleep(60)
    print('Finish waiting. Sending data to airflow now.')
    
    endpoint = 'http://3.213.19.89:8080/api/experimental/dags/emr_job_flow_manual_steps_dag_pyspark/dag_runs'
    
    data = json.dumps({"conf": {'s3_location': process_data}})
    
    subprocess.run(['curl', '-X', 'POST', endpoint, '--insecure', '-d', data])
    
    return {
        'statusCode': 200
    }