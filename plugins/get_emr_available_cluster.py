import boto3
import time

def get_avilable_cluster_id():
    clusters = boto3.Session().client('emr', region_name= 'us-east-1').list_clusters()['Clusters']
    for x in clusters:
        if (x['Status']['State']) in ['RUNNING','WAITING']:
            return x['Id']

def watch_clusters(): 
    running = True  #Initial/controlling condition for the while loop
    seconds_lapsed = 0  #Counter to track how long the run takes
    timeout = 1200  #Maximum time in seconds we are allowing the run to take

    while running:

        if get_avilable_cluster_id() != None:

            print('Found an available cluster. Returning Cluster Id.')
            return get_avilable_cluster_id()

        elif seconds_lapsed == timeout:

            #output timeout report
            print("timeout")

            #break the loop
            running = False
        
        elif get_avilable_cluster_id() == None:

            #wait 5 seconds and update the counter, continue the loop
            time.sleep(5)
            seconds_lapsed += 5
            m = int(seconds_lapsed / 60)
            s = seconds_lapsed % 60

            print(f'No available cluster running. Waiting 5 seconds and checking again. Total time lapsed: {m}:{s}')
            continue

        else:

            #output error
            print("unknown error")

            #break the loop
            running = False