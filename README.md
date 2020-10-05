this application is to pull all the tags from twitter using a spark stream application and save them using a small reduce function to the hdfs, to do more query on the saved data you can load this data to either hive or impala.


1. run the spark application (App.java)
    you need to get consumerKey,consumerSecret,accessToken, and accessTokenSecret for a twitter project to run this code .
    and you need to set this values inside the App class.
     ```
      String consumerKey = "";
  	  String consumerSecret = "";
  	  String accessToken = "";
  	  String accessTokenSecret = "";
      ```

2. Create hive table command:

```
CREATE external table hashtags (tag String, count int)
PARTITIONED BY(createdat String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
LOCATION
"/tmp/finalProject";
```


3. reload saved data by spark to hdfs into hive before running any query to load the new added data
```
MSCK REPAIR TABLE hashtags;
``` 

4. select the top 10 tags from hive 
```
select tag, sum(count) as count from hashtags group by tag order by count desc limit 10;
```

5. create table in impala
```
CREATE external table impalahashtags (tag String, count int)
PARTITIONED BY(createdat String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
LOCATION
"/tmp/finalProject";
```

6. refresh impala from the new loaded data on hdfs
```
refresh impalahashtags
```


Project Demo https://youtu.be/gYLQY90iugg
