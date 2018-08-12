#!env python 2.7

'''
Script to view Kinesis stream. Accepts stream name as parameter

'''

from __future__ import print_function
import sys
import boto3
import json
from datetime import datetime
from datetime import timedelta
import time

def ViewStream(stream_name):
    # end point url is critical for local testing
    kinesis_client = boto3.client('kinesis', endpoint_url='http://localhost:4568', region_name='us-east-1'
                                  , aws_access_key_id='x',aws_secret_access_key='x')
    # get the shard id
    response = kinesis_client.describe_stream(StreamName=stream_name)
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    # get the starting shard iterator. Last Sequence number iterator can also be used to start from the last read record
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=stream_name, ShardId=shard_id
                                                       , ShardIteratorType='TRIM_HORIZON')['ShardIterator']
    all_rec = {}
    while True:
        out = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=10)
        
        if not out['Records']:
            break

        for rec in out["Records"]:
            print(rec)
            time.sleep(1)

        shard_iterator = out["NextShardIterator"]

if __name__=='__main__':
    '''
    Main method, expects 3 system arguments - Read Stream, Write Stream and Inactivity Threshold

    :type read_stream_name: String
    :param read_stream_name: Name of the Order Stream to read order records 

    '''
    if len (sys.argv) != 2 :
        stream_name = 'order_created'
    else:
        stream_name = str(sys.argv[1])

    ViewStream(stream_name)