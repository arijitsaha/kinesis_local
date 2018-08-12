#!env sh

awslocal kinesis delete-stream --stream-name order_created
awslocal kinesis delete-stream --stream-name order_assigned
awslocal kinesis delete-stream --stream-name order_completed

awslocal kinesis create-stream --stream-name order_created --shard-count 1
awslocal kinesis create-stream --stream-name order_assigned --shard-count 1
awslocal kinesis create-stream --stream-name order_completed --shard-count 1