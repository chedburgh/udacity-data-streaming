## Udacity SF Crime Stats

In this project, you will be provided with a real-world dataset, extracted from Kaggle, on San Francisco crime incidents, and you will provide statistical analyses of the data using Apache Spark Structured Streaming. You will draw on the skills and knowledge you've learned in this course to create a Kafka server to produce data, and ingest data through Spark Structured Streaming.

You can try to answer the following questions with the dataset:

What are the top types of crimes in San Fransisco?
What is the crime density by location?

### Screenshots

Find various screen shots under the "screenshots" directory.

### Questions

Question: How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Answer: The effect was to change the throughput. With  maxRatePerPartition and maxOffsetPerTrigger in the low hundreds throughput was reasonable fast. Changing these settings to the low thousands and throughput degraded. Directly it effected processedRowsPerSecond. Batch size should not be to large.

Question: What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

Answer: Possibly the most efficient properties were spark.default.parallelism, spark.streaming.kafka.maxRatePerPartition / spark.streaming.kafka.maxRatePerPartition. It is possible to check the results in Spark Progress Report (under inputRowsPerSecond or processedRowsPerSecond) or in Spark UI Streaming page (under Min, Median and Max rate of records/second).


