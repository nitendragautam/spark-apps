## Building the Package
```
mvn clean install

```



## Connecting Apache Spark with Apache Hive

To use Apache Hive from Spark shell or spark applications ,it should have access to hive-site.xml
and mysql common library jar

* Make a Symbolic link of hive-site.xml at Spark Path c
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



## Word Count

## Run Spark Job for Java Word Count

I am giving a relative path in spark-submit command.You might have to use full path depending upon the operating system you used

```

$SPARK_HOME/bin/spark-submit --class "com.nitendratech.JavaWordCount" --master "local[2]" ./target/javaspark-1.0.jar 
./datasets/inputWordCount.txt


```