# Pinterest Data Pipeline

> This project is about creating a data processing pipeline with the help of AWS Cloud, that is similar to the system Pinterest employs to crunch many data points, in order to decide how to provide more value to their users.

## Summary

We will be creating two data pipelines: Batch & Streaming. 

In the first pipeline, we will follow the "Extract, Load, Transform" principle (ELT), whereby we will be collecting the data, which will be sent through an API to a Kafka cluster, and then stored in an S3 bucket. The data will then be cleaned and queried using Spark on Databricks.

In the second pipeline, we will follow the "Extract, Transform, Load" principle (ETL), whereby we will be sending data to Kinesis streams via an API, then cleaning the data in Databricks using Spark, and finally saving the data in Delta tables.

