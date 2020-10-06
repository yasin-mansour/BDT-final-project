Apache Hive is a data warehouse developed by Facebook to perform data-intensive versatile tasks that support analysis of hug datasets stored in Hadoop HDFS and other compatible file systems such as Amazon S3. 

Impala is the Hadoop native query language build over HDFS and embedded into HDFS and Apache HBase through long-lived demons on the data nodes.

each one of them has it is own advantages and disadvantages over the other, for instance, Hive can run complicated and versatile tasks and can be plugged with other software, but impala can mostly work with Hadoop hdfs and have some data format supporting issue that Hive does not have because it is more configurable.

impala work faster than Hive because it does not require a starting time like hive because it works over the long-lived demons on the data node. however, Hive is match suitable for complicated queries that require long time because it can provide result even if some failure happened in the process, but Impala is not the perfect choice for these type of queries because a failure on one data node can terminate the whole query.

In general, Impala is perfect for real time queries and for the queries that need to be executed in a short time over the hdfs, and Hive is perfect for the more complicated query that needs long time to finish execution. 

Finally, we can get the most of both in the same application by running Impala on the same Hive directory which will give us access to the same dataset in the Impala and Hive, the same way I have done on my project.
