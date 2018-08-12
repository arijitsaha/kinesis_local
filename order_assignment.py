#!env python 2.7

'''
Script to assign orders to drivers and write the assignment to a new Kinesis stream

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

def AssignOrder(read_stream_name, write_stream_name, batch_size, wait_minutes):

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
        out = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=batch_size)
        
        # if no records are available wait for 10 seconds before checking again
        if not out['Records']:
            time.sleep(10)
            total_wait += 10
            continue
        
        for rec in out["Records"]:
            jdat = json.loads(rec["Data"])
            order_assignment = {'order_id' : jdat['order_id'], 'driver' : faker.name()}
            kinesis_client.put_record(
                        StreamName=write_stream_name,
                        Data=json.dumps(order_assignment),
                        PartitionKey=jdat['ship_from_region'])
            
        shard_iterator = out["NextShardIterator"]

        # stop counter
        end = time.time()
        # measure execution time
        time_taken = (end - start)
        # unnecessary section to display meaningful message during execution
        strmessage = "Assigned " + str(batch_size) + "orders in time " + str(time_taken)
        print(strmessage)
        #time.sleep(2)

if __name__=='__main__':
    '''
    Main method, expects 3 system arguments - Read Stream, Write Stream and Inactivity Threshold

    :type read_stream_name: String
    :param read_stream_name: Name of the Order Stream to read order records 

    :type write_stream_name: String
    :param write_stream_name: Name of the Order Stream to write order assignment 

    :type inactivity_threshold: Float
    :param inactivity_threshold: Number of minutes the script should continue to insert records

    '''
    if len (sys.argv) != 4 :
        read_stream_name = 'order_created'
        write_stream_name = 'order_assigned'
        inactivity_threshold = 5
    else:
        read_stream_name = str(sys.argv[1])
        write_stream_name = str(sys.argv[2])
        inactivity_threshold = float(sys.argv[3])

    num_per_batch = 15 # start with 15 as batch size
    
    AssignOrder(read_stream_name, write_stream_name, num_per_batch, inactivity_threshold)
