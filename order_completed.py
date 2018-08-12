#!env python 2.7
'''
Script to complte orders and write the completed orders to a new Kinesis stream. 
Assumption here is trips are mark completed after n time, though in real life it should be triggered after actual completion

Kinesalite as part of Localstack used as local AWS Kinesis instance https://github.com/localstack/localstack

Faker python package used to generate fake order data https://github.com/joke2k/faker

Boto 3 is the Amazon Web Services (AWS) SDK for Python https://boto3.readthedocs.io/en/latest/ 

'''

from __future__ import print_function
import sys
import boto3
import json
from datetime import datetime
from datetime import timedelta
import time
from faker import Factory

faker = Factory.create()
faker.seed(9876)


def CompleteOrder(read_stream_name, write_stream_name, wait_minutes):

    # end point url is critical for local testing
    kinesis_client = boto3.client('kinesis', endpoint_url='http://localhost:4568', region_name='us-east-1', aws_access_key_id='x',aws_secret_access_key='x')
    # get the shard id
    response = kinesis_client.describe_stream(StreamName=read_stream_name)
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    # get the starting shard iterator. Last Sequence number iterator can also be used to start from the last read record
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=read_stream_name, ShardId=shard_id, ShardIteratorType='TRIM_HORIZON')['ShardIterator']
    
    total_wait = 0
    
    while total_wait <= (wait_minutes * 60):
        # start counter
        start = time.time()
        # get orders from queue
        out = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=15)
        
        # if no records are available wait for 10 seconds before checking again
        if not out['Records']:
            time.sleep(10)
            total_wait += 10
            continue
        
        for rec in out["Records"]:
            jdat = json.loads(rec["Data"])
            print(jdat)
            trip_assign_str = rec['ApproximateArrivalTimestamp'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            trip_assign_time = datetime.strptime(trip_assign_str, '%Y-%m-%d %H:%M:%S.%f')
            
            elapse_seconds = (datetime.now() - trip_assign_time).total_seconds()

            if elapse_seconds < (wait_minutes * 30): # assumption that trip completes in (wait_minutes/2)
                slptime = float((wait_minutes * 30) - elapse_seconds)
                strmessage = 'Min threshold to complete orders not reached. Sleeping for ' + str(slptime) + ' seconds.'
                print(strmessage)
                time.sleep(slptime)

            order_completed = {'order_id' : jdat['order_id']}
            kinesis_client.put_record(
                        StreamName=write_stream_name,
                        Data=json.dumps(order_completed),
                        PartitionKey='abc')
            
        shard_iterator = out["NextShardIterator"]

        # stop counter
        end = time.time()
        # measure execution time
        time_taken = (end - start)
        # unnecessary section to display meaningful message during execution
        strmessage = "Total time (including wait) for order completion " + str(time_taken)
        print(strmessage)
        time.sleep(1)

if __name__=='__main__':

    '''
    Main method, expects 3 system arguments - Read Stream, Write Stream and Inactivity Threshold

    :type read_stream_name: String
    :param read_stream_name: Name of the Order Stream to read assigned order records 

    :type write_stream_name: String
    :param write_stream_name: Name of the Order Stream to write order completion 

    :type inactivity_threshold: Float
    :param inactivity_threshold: Number of minutes the script should continue to insert records

    '''
    if len (sys.argv) != 4 :
        read_stream_name = 'order_assigned'
        write_stream_name = 'order_completed'
        inactivity_threshold = 5
    else:
        read_stream_name = str(sys.argv[1])
        write_stream_name = str(sys.argv[2])
        inactivity_threshold = float(sys.argv[3])
    
    CompleteOrder(read_stream_name, write_stream_name, inactivity_threshold)