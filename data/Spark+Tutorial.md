
#### Upload the file to HDInsight 
> scp fligths.zip sshuser@clustername-ssh.azurehdinsight.net:flights.zip
> unzip flights.zip

#### Connect to the cluster by using SSH
> ssh sshuser@clustername-ssh.azurehdinsight.net

#### Copy csv file to hadoop storage
> hadoop fs -copyFromLocal flight.csv /data/flights.csv


```pyspark
from pyspark.sql import *
from pyspark.sql.types import *
```

    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>2</td><td>application_1537658221498_0003</td><td>pyspark</td><td>idle</td><td><a target="_blank" href="http://hn0-myspar.mn02ipz3mu1e1orzdgtui0o3fh.cx.internal.cloudapp.net:8088/proxy/application_1537658221498_0003/">Link</a></td><td><a target="_blank" href="http://wn4-myspar.mn02ipz3mu1e1orzdgtui0o3fh.cx.internal.cloudapp.net:30060/node/containerlogs/container_e01_1537658221498_0003_01_000001/livy">Link</a></td><td>✔</td></tr></table>


    SparkSession available as 'spark'.



```pyspark
csvFile = spark.read.csv('wasb:///data/flights.csv',  header=True)
csvFile.write.saveAsTable("flights")
```


```pyspark
df = spark.sql('select * from flights')
df.limit(3).show()
```

    +----+----------+--------------+------+-----------------+---------------------+---------------------+------+----------------+----------------+---------------+-------------------+-------------------+----+--------------+--------------+---------+-------------+---------+-------------+-------------+-------------+---------+--------------+-------------------+----+
    |YEAR|   FL_DATE|UNIQUE_CARRIER|FL_NUM|ORIGIN_AIRPORT_ID|ORIGIN_AIRPORT_SEQ_ID|ORIGIN_CITY_MARKET_ID|ORIGIN|ORIGIN_CITY_NAME|ORIGIN_STATE_ABR|DEST_AIRPORT_ID|DEST_AIRPORT_SEQ_ID|DEST_CITY_MARKET_ID|DEST|DEST_CITY_NAME|DEST_STATE_ABR|DEP_DELAY|DEP_DELAY_NEW|ARR_DELAY|ARR_DELAY_NEW|CARRIER_DELAY|WEATHER_DELAY|NAS_DELAY|SECURITY_DELAY|LATE_AIRCRAFT_DELAY|_c25|
    +----+----------+--------------+------+-----------------+---------------------+---------------------+------+----------------+----------------+---------------+-------------------+-------------------+----+--------------+--------------+---------+-------------+---------+-------------+-------------+-------------+---------+--------------+-------------------+----+
    |2018|2018-01-12|            UA|   264|            13485|              1348502|                33485|   MSN|     Madison, WI|              WI|          11292|            1129202|              30325| DEN|    Denver, CO|            CO|    -2.00|         0.00|   -22.00|         0.00|         null|         null|     null|          null|               null|null|
    |2018|2018-01-12|            UA|   263|            11292|              1129202|                30325|   DEN|      Denver, CO|              CO|          13930|            1393006|              30977| ORD|   Chicago, IL|            IL|     2.00|         2.00|    -5.00|         0.00|         null|         null|     null|          null|               null|null|
    |2018|2018-01-12|            UA|   262|            14747|              1474703|                30559|   SEA|     Seattle, WA|              WA|          12264|            1226402|              30852| IAD|Washington, DC|            VA|    -3.00|         0.00|   -12.00|         0.00|         null|         null|     null|          null|               null|null|
    +----+----------+--------------+------+-----------------+---------------------+---------------------+------+----------------+----------------+---------------+-------------------+-------------------+----+--------------+--------------+---------+-------------+---------+-------------+-------------+-------------+---------+--------------+-------------------+----+


```pyspark
spark.sql('drop table flights')
```

    DataFrame[]


```pyspark

```
