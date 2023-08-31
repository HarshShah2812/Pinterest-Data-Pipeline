# Pinterest Data Pipeline

> This project is about creating a data processing pipeline with the help of AWS Cloud, that is similar to the system Pinterest employs to crunch many data points, in order to decide how to provide more value to their users.

## Summary

I have created two data pipelines: Batch & Streaming. 

The first pipeline follows the "Extract, Load, Transform" principle (ELT), whereby data is collected, which is then sent through an API to a Kafka cluster, and then stored in an S3 bucket. The data is then cleaned and queried using Spark on Databricks.

The second pipeline follows the "Extract, Transform, Load" principle (ETL), whereby data is sent to Kinesis streams via an API, then cleaned in Databricks using Spark, and finally saved in Delta tables.

## Requirements

- Install Python (3.10.0)
- Install SQLAlchemy
- Copy the key-pair associated with your EC2 instance into a file named using the format UserId-key-pair.pem (0e4c2ab6fb3b being my UserId)

- In a terminal, connect to an EC2 instance and start a Confluent server, using the following commands:
```
ssh -i 0e4c2ab6fb3b-key-pair.pem ec2-user@ec2-34-229-53-171.compute-1.amazonaws.com
```
```
export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar
```
You can also automate this process by adding the export command to the .bashrc file located in the home directory of the EC2 instance. You can open the file using `nano ~/.bashrc`.\n
```
cd confluent-7.2.0/bin
```
```
./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties
```
- In a separate terminal, from your directory, execute the following:
```
python user_posting_emulation_batch.py
```
- If you wish to see the data being sent to the Kafka consumer, you can create and run consumers for each of the Kafka topics by going to the `kafka_2.12-2.8.1/bin` folder and running the following command: 
```
./kafka-console-consumer.sh --bootstrap-server BootstrapServerString --consumer.config client.properties --group students --topic <topic_name> --from-beginning
```
Replace **BootstrapServerString** with the string you obtain from the client information corresponding to the MSK Cluster on AWS, and **topic_name** with the Kafka topic you've created for each type of record (pin, geo, user), which, for example, will take the format UserId.pin for pin data. You will need to set up separate EC2 instances for each of the consumers.

## Project Outline

### Milestone 1 & 2

I set up an AWS account and dowloaded the Pinterest infrastructure, which initially was a posting emulation that simply took rows from a Pinterest database and printed them out in the terminal.

### Milestone 3

I created the key-pair.pem file for connecting to the EC2 instance, then set up Kafka on the instance and installed the IAM MSK authentication package in the libs folder within the Kafka installation folder. This package is essential to connect to MSK clusters that require IAM authentication. I then made a client.properties file inside the kafka_folder/bin directory to configure the Kafka client to use IAM Authentication to the cluster.

Finally, I created the Kafka topics: 0e4c2ab6fb3b.pin, 0e4c2ab6fb3b.geo, 0e4c2ab6fb3b.user.

### Milestone 4

I used MSK Connect to connect the MSK cluster to an S3 bucket via the EC2 client. I did this by firstly downloading the Confluent connector and copying it to the S3 bucket (user-0e4c2ab6fb3b-bucket), using the following commands in the EC2 instance:
'''
# download connector from Confluent"
"wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip",
# copy connector to our S3 bucket"
"aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://user-0e4c2ab6fb3b-bucket/kafka-connect-s3/"
'''
I then created a custom plugin in the MSK Connect console, calling it 0e4c2ab6fb3b-plugin.

Next, I created a connector with MSK Connect, calling it 0e4c2ab6fb3b-connector

### Milestone 5

Here, I configured an API to send data to the MSK Connector using a Kafka REST Proxy integration. In order to do this, I installed the Confluent package for the Kafka REST Proxy on my EC2 client machine, then I modified the kafka-rest.properties file to allow the REST Proxy to perform IAM authentication. I then modified the user_posting_emulation_batch script to send data to each of the 3 Kafka topics using the API invoke URL.

### Milestone 6 & 7

I set up my Databricks account, ingested the data stored in the 3 topics, cleaned it and queried it.

In Databricks, I created a notebook called batch_data_cleaning_and_querying, which mounts the S3 bucket, cleans each of the dataframes by ensuring that all data is of the correct datatype, that any empty entries and entries with no relevant data return Null values, as well as merging or dropping columns and reordering them.

I then queried the data using PySpark SQL, which included for example finding out the most popular category in each country, as well as even finding the median follower count of users based on their joining year and age group.

### Milestone 8

I created a Directed Acyclical Graph (DAG) in order to automate the Databricks workbook I created in the previous milestone so that it gets triggered to run every 2 hours. I then uploaded it to a Managed Workflows for Apache Airflow (MWAA) environment. After doing this, I could now see the DAG in the Airflow UI on the MWAA environment and could manually start it.

### Milestone 9

In this milestone, I created the streaming data pipeline. Firstly, I used Kinesis Data Streams to create 3 data streams, one for each Pinterest table. Then I configured the REST API used for the Batch data pipeline to allow it to invoke the following Kinesis actions:
- List the streams
- Create, describe and delete streams
- Add records to the streams

Next, I edited the previous user posting emulation script, calling it user_posting_emulation_streaming, which sends stream data to their corresponding streams in Kinesis.

I created another notebook called streaming_data_cleaning_and_querying, which reads data from the data streams into their corresponding dataframes using structured streaming. I performed transformations on the data in the same way that I did to the batch data, but this time, the data was then saved into Delta tables.

Here is a flowchart that demonstrates what is happening when both the batch and streaming data pipelines are run:

![Flowchart](C:\Users\hshah\Desktop\PDP\pinterest-data-pipeline\screenshot_2023-04-20_at_10.10.06-1[7622].png)
