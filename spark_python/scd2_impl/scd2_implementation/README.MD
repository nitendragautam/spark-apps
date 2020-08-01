## Implementing SCD2 in Pyspark

## Source 


```
+------+--------------+
|src_id|      src_attr|
+------+--------------+
|     1|  Hello World!|
|     2|Hello PySpark!|
|     4|  Hello Scala!|
+------+--------------+
```

column called ```id``` is the key for existing data


## Target Data

```
+---+----------------+-----------+----------+----------+----------+
| id|            attr|is_currenrt|is_deleted|start_date|  end_date|
+---+----------------+-----------+----------+----------+----------+
|  1|          Hello!|      false|     false|2018-01-01|2018-12-31|
|  1|    Hello World!|       true|     false|2019-01-01|9999-12-31|
|  2|    Hello Spark!|       true|     false|2019-02-01|9999-12-31|
|  3|Hello Old World!|       true|     false|2019-02-01|9999-12-31|
+---+----------------+-----------+----------+----------+----------+

```

## Purpose
* To merge the source data into the target data set following the full merge pattern

```
+---+----------------+----------+----------+----------+----------+
| id|            attr|is_current|is_deleted|start_date|  end_date|
+---+----------------+----------+----------+----------+----------+
|  1|          Hello!|     false|     false|2018-01-01|2018-12-31|
|  1|    Hello World!|      true|     false|2019-01-01|9999-12-31|
|  2|    Hello Spark!|     false|     false|2019-02-01|2020-05-16|
|  2|  Hello PySpark!|      true|     false|2020-05-17|9999-12-31|
|  3|Hello Old World!|     false|      true|2019-02-01|9999-12-31|
|  4|    Hello Scala!|      true|     false|2020-05-17|9999-12-31|
+---+----------------+----------+----------+----------+----------+
```

## Implement Full Join Between source and target data frames

 Fullouter join type is used and the join keys are on column id and end_date. 
 
 A new column action is also added to work what actions needs to be implemented for each record.

There are four possibilities for the actions:

* UPSERT(Update and Insert):
 Attributes have changed in the source and the existing records need to be expired and 
new records need to be inserted.

* DELETE: Business keys no longer exist in source table and the records in target table need to be deleted logically.

* INSERT: new business keys exist in source that need to be inserted into the target table directly.

* NOACTION: no changes to the attributes or the records in target table are not current.


## Reference

https://kontext.tech/column/spark/286/implement-scd-type-2-full-merge-via-spark-data-frames