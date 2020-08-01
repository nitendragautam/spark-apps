from pyspark import SparkContext,SparkConf, SQLContext
from pyspark.sql import Row,SparkSession
import json
from pyspark.sql.functions import udf, lit, when, date_sub
from datetime import datetime
from pyspark.sql.types import ArrayType,IntegerType,StructType,StructField,StringType,BooleanType, DateType




def get_source_data_frame(spark_session):

    source_schema = StructType([
        StructField("src_id", IntegerType(), True),
        StructField("src_attr", StringType(),True)
    ])

    source_data = [
        Row(1, "Hello World!"),
        Row(2, "Hello PySpark!"),
        Row(4, "Hello Scala!")
    ]
    spark_context = spark_session.sparkContext

    return spark_session.createDataFrame(spark_context.parallelize(source_data), source_schema)




def get_target_data_frame(spark_session):
    target_schema=StructType([
       StructField("id", IntegerType(),True),
       StructField("attr", StringType(), True),
       StructField("is_current", BooleanType(), True),
       StructField("is_deleted", BooleanType(), True),
       StructField("start_date", DateType(), True),
       StructField("end_date", DateType(), True)
   ])
    target_data = [
       Row(1, "Hello!", False, False, datetime.strptime(
           '2018-01-01', '%Y-%m-%d'), datetime.strptime('2018-12-31', '%Y-%m-%d')),
       Row(1, "Hello World!", True, False, datetime.strptime(
           '2019-01-01', '%Y-%m-%d'), datetime.strptime('9999-12-31', '%Y-%m-%d')),
       Row(2, "Hello Spark!", True, False, datetime.strptime(
           '2019-02-01', '%Y-%m-%d'), datetime.strptime('9999-12-31', '%Y-%m-%d')),
       Row(3, "Hello Old World!", True, False, datetime.strptime(
           '2019-02-01', '%Y-%m-%d'), datetime.strptime('9999-12-31', '%Y-%m-%d'))
   ]

    spark_context = spark_session.sparkContext
    return spark_session.createDataFrame(spark_context.parallelize(target_data), target_schema)

app_name ='Spark SQL SCD'

spark_session = SparkSession.builder\
                            .appName(app_name)\
                            .master("local[2]").getOrCreate()

# Set the Log Level to Error
spark_session.sparkContext.setLogLevel("ERROR")

target_df = get_target_data_frame(spark_session)

print(" Target DataFrame")
target_df.show()

print(" Source DataFrame")
source_df = get_source_data_frame(spark_session)

source_df.show()


## Implement full join between source and target dataframe

high_date = datetime.strptime('9999-12-31','%Y-%m-%d').date()


print(high_date)

current_date = datetime.today().date()


## Prepare for merge - Added Effective and end date

source_df_new = source_df.withColumn('src_start_date', lit(current_date))\
                        .withColumn('src_end_date',lit(high_date))


source_df_new.show()

print("FULL Merge, join on key column and also high date column to make only join to the latest records")

merge_df = target_df.join(source_df_new,(source_df_new.src_id == target_df.id)
                          & (source_df_new.src_end_date == target_df.end_date),how='fullouter')


merge_df.show()



print("Derive new Columns to Indicate the action")


merge_df = merge_df.withColumn('action',
                               when(merge_df.attr!= merge_df.src_attr,'UPSERT')
                               .when(merge_df.src_id.isNull() & merge_df.is_current, 'DELETE')
                               .when(merge_df.id.isNull(), 'INSERT')
                               .otherwise('NOACTION'))


merge_df.show()


## Implement all the SCD Type 2 actions by generating different data frames



## Generate the new DataFRames based on action code

column_names = ['id', 'attr', 'is_current', 'is_deleted', 'start_date', 'end_date']

# Records that need no action
merge_df_no_action =merge_df.filter(merge_df.action == 'NOACTION').select(column_names)

print("merge_df_no_action")
merge_df_no_action.show()

## Record that need insert only
merge_df_insert = merge_df.filter(merge_df.action =='INSERT').select(
    merge_df.src_id.alias('id'),
    merge_df.src_attr.alias('attr'),
    lit(True).alias('is_current'),
    lit(False).alias('is_deleted'),
    merge_df.src_start_date.alias('start_date'),
    merge_df.src_end_date.alias('end_date')
)


print("merge_df_insert")

merge_df_insert.show()


## for records that needs to be deleted

merge_df_deleted = merge_df.filter(merge_df.action == 'DELETE')\
                    .select(column_names)\
                       .withColumn('is_current',lit(False))\
                       .withColumn('is_deleted',lit(True))


print("merge_df_deleted")
merge_df_deleted.show()

## For records that needs to be expired and then inserted

merge_df_p4_1 = merge_df.filter(merge_df.action == 'UPSERT')\
                        .select(
                        merge_df.src_id.alias('id'),
                        merge_df.src_attr.alias('attr'),
                        lit(True).alias('is_current'),
                        lit(False).alias('is_deleted'),
                        merge_df.src_start_date.alias('start_date'),
                        merge_df.src_end_date.alias('end_date'))
print("merge_df_p4_1")

merge_df_p4_1.show()

merge_df_p4_2 = merge_df.filter(merge_df.action == 'UPSERT').withColumn(
                'end_date',
                date_sub(merge_df.src_start_date, 1))\
                .withColumn('is_current', lit(False))\
                .withColumn('is_deleted', lit(False)).select(column_names)

print("merge_df_p4_2")

merge_df_p4_2.show()


## Union the Data FRames

merge_df_final = merge_df_no_action\
                .unionAll(merge_df_insert)\
                .unionAll(merge_df_deleted)\
                .unionAll(merge_df_p4_1)\
                .unionAll(merge_df_p4_2)\
                .orderBy(['id','start_date'])

merge_df_final.show()