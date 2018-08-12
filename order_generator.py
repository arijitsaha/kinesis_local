#!env python 2.7

'''
Script to generate orders and write it to Kinesis stream

Kinesalite as part of Localstack used as local AWS Kinesis instance https://github.com/localstack/localstack

Faker python package used to generate fake order data https://github.com/joke2k/faker

'''

from __future__ import print_function
import sys
import boto3
import json
from datetime import datetime
from datetime import timedelta
import random
import time
from faker import Factory

faker = Factory.create()
faker.seed(9876)

def OrderCreation(num_rec):
    '''
    Fake order creation

    :type num_rec: Integer
    :param num_rec: Number of fake order records 

    '''
    # generate fake order dataset
    return [{'order_id': random.choice('ABCDEF')+ faker.numerify('-####-')+ random.choice('STU')+ faker.numerify('###'),  # random number eg:235-533
            'ship_from_region': 'reg' + faker.numerify('#'),  # random region code
            'ship_to_region': 'reg' + faker.numerify('#'),  # random region code
            'pickup_time' : str(datetime.now() + timedelta(minutes=random.randint(5,30)))[:-10], #random order time
            'price' : faker.numerify('###'), # random order value
            'created_ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] #order generation timestamp
            } for _ in range(num_rec)]


def InsertOrderinStream(stream_name, num_rec):
    '''

    Insert order records in Kinesis stream

    :type stream_name: String
    :param stream_name: Name of the Order Stream to write new order records 

    :type num_rec: Integer
    :param num_rec: Number of fake order records 

    '''
    # end point url is critical for local testing
    kinesis_client = boto3.client('kinesis', endpoint_url='http://localhost:4568', region_name='us-east-1', aws_access_key_id='x',aws_secret_access_key='x')
    orders = OrderCreation(num_rec)
    
    start = time.time()

    # loop and insert individual record to stream instead of a blob of records
    for order_record in orders:
        kinesis_client.put_record(
                        StreamName=stream_name,
                        Data=json.dumps(order_record),
                        PartitionKey=order_record['ship_from_region'])
    
    end = time.time()
    # measure execution time
    time_taken = (end - start)
    return time_taken

if __name__=='__main__':
    '''
    Main method, expects 2 system arguments - Stream Name and Number of minutes to run the producer

    :type stream_name: String
    :param stream_name: Name of the Order Stream to write new order records 

    :type minutes_running: Float
    :param minutes_running: Number of minutes the script should continue to insert records

    '''
    if len (sys.argv) != 3 :
        stream_name = 'order_created'
        minutes_running = 1
    else:
        stream_name = str(sys.argv[1])
        minutes_running = float(sys.argv[2])

    num_per_batch = 15 # start with 15 as batch size
    
    # Calculate start & end time
    start_time = datetime.now()
    end_time = start_time + timedelta(minutes=minutes_running)

    while True:
        
        # break condition from an otherwise infinite loop. 
        # Other business logic before shutting down the producer can be handled here
        now = datetime.now()
        if (end_time < now) or (num_per_batch == 0):
            break
        
        response = InsertOrderinStream(stream_name, num_per_batch)
        
        # unnecessary section to display meaningful message during execution
        elapsed_time = now - start_time
        strmessage = "Inserted " + str(num_per_batch) + " orders in " + str(response) + " seconds. Total elapsed Time = " + str(elapsed_time)
        print (strmessage)
        
        # adjust batch size based on time taken & 1 sec as benchmark
        if response > 0.92:
            num_per_batch -= 1
        else:
            num_per_batch += 1
    
        #time.sleep(2)
    