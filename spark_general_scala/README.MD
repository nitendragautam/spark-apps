## Building the Package

```sbt clean package```




## Connecting Apache Spark with Apache Hive

To use Apache Hive from Spark shell or spark applications ,it should have access to hive-site.xml
and mysql common linrary jar

* Make a Symbollic link of hive-site.xml at Spark Path c
ln -s /usr/local/hive/conf/hive-site.xml /usr/local/spark/conf/hive-site.xml
  
* Copy Mysql jar to spark path

```
cp mysql-connector-java-5.1.44.jar $SPARK_HOME/spark/jars/
```

* Add this property to hive-site.xml 

```
<property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
</property>
```





## Scala Version

Check Scala Version here

https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.12/2.4.0



## Word Count

## Run Spark Job for Java Word Count

```

$SPARK_HOME/bin/spark-submit --class "com.nitendratech.JavaWordCount" --master "local[2]" /Users/nitendragautam/nitendra_items/gitlab_projects/big_data_projects/sparkapplications/target/scala-2.12/sparkprojects_2.12-1.0.jar /Users/nitendragautam/nitendra_items/gitlab_projects/big_data_projects/sparkapplications/in/inputWordCount.txt


```