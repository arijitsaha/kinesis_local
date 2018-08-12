# README

#### Project to simulate a pipeline of realtime processing with event streaming data using AWS Kinesis Streams.

Following packages are used for running kinesis streams in local development environment

- Localstack: https://github.com/localstack/localstack/
  Simulates a local AWS development environment. Kinesalite is part of this package.
 
- Boto 3: https://boto3.readthedocs.io/en/latest/
  Boto is the Amazon Web Services (AWS) SDK for Python, which allows Python developers to write software that makes use of Amazon services like Kinesis and S3. 
  Boto provides an easy to use, object-oriented API as well as low-level direct service access.

- Faker: https://github.com/joke2k/faker/
  Faker is a Python package used to generate fake data
  
 
 #### Project structure 
 
 Following are the major scripts
 
 - reset_order_streams.sh: Deletes & Recreates the necessary Kinesis streams
 - order_generator.py: Generates fakes orders and inserts the event order_created
 - order_assignment.py: Assigns orders to drivers and inserts the event order_assigned
 - order_completed.py: Marks orders as completed and inserts order_id in order_completed
 - view_stream.py: To view the various order event stream
 - metrics_top_regions.ipynb: Notebook to calcuate Top N regions based on number of orders created
 
 #### Installations and Setup
 
- pip install localstack
- pip install awscli-local
- pip install faker
- pip install boto3
- localstack start --docker
- source kinesis_local/bin/activate

#### Examples on how to run the scripts

- bash reset_order_streams.sh 
- python order_generator.py 'order_created' 2
- python order_assignment.py 'order_created' 'order_assigned' 2
- python order_completed.py 'order_assigned' 'order_completed' 2
- python view_stream.py 'order_created' 
- python view_stream.py 'order_assigned' 
- python view_stream.py 'order_completed' 

