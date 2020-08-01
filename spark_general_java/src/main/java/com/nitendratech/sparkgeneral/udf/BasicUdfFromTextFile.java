package com.nitendratech.sparkgeneral.udf;

import com.nitendratech.sparkgeneral.util.AppUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.callUDF;

import java.io.Serializable;

/**
 * Created by @author nitendratech on 5/24/20
 */
public class BasicUdfFromTextFile implements Serializable {

    private static final long serialVersionUID = 3492970200940899012L;


    public static void main(String args[]){


        BasicUdfFromTextFile b = new BasicUdfFromTextFile();

        b.startSparkApp();


    }

    private void registerX2Udf(SparkSession sparkSession){
        // Register a new UDF

        sparkSession.udf().register("x2Multiplier", new UDF1<Integer,Integer>() {

            private static final long serialVersionUID = -5372447039252716846L;

            @Override
            public Integer call(Integer x)  {
                return x * 2;
            }}, DataTypes.IntegerType);
    }

    private void registerX3Udf(SparkSession sparkSession){
        sparkSession.udf().register("x3Multiplier", new UDF2<Integer,Integer, Integer>() {
            @Override
            public Integer call(Integer x1, Integer x2) throws Exception {
                return x1 *x2;
            }
        },DataTypes.IntegerType);

    }

    private  void startSparkApp(){

        SparkSession sparkSession = AppUtil.getSparkSession("BasicUdfFromTextFile","local[*]");


        registerX2Udf(sparkSession);
        registerX3Udf(sparkSession);

        String filePath ="datasets/tuple-data-file.csv";

        Dataset<Row> tupleDf = sparkSession.read()
                                .format("csv").option("inferSchema", "true")
                                .option("header", "false")
                                .load(filePath);

        tupleDf.show();

        /** Gives below result
         * +---+---+
         * |_c0|_c1|
         * +---+---+
         * |  1|  5|
         * |  2| 13|
         * |  3| 27|
         * |  4| 39|
         * |  5| 41|
         * |  6| 55|
         * +---+---+
         */

        tupleDf = tupleDf.withColumn("label", tupleDf.col("_c0")).drop("_c0");


        tupleDf = tupleDf.withColumn("value", tupleDf.col("_c1")).drop("_c1");

        tupleDf.show();

        /** tupleDf.show();
         * +-----+-----+
         * |label|value|
         * +-----+-----+
         * |    1|    5|
         * |    2|   13|
         * |    3|   27|
         * |    4|   39|
         * |    5|   41|
         * |    6|   55|
         * +-----+-----+
         */

        // Now Call the Udf Function
        tupleDf = tupleDf
                .withColumn("x2",callUDF("x2Multiplier",tupleDf.col("value")
                        .cast(DataTypes.IntegerType)));

        tupleDf.show();

        /**  tupleDf.show();
         * +-----+-----+---+
         * |label|value| x2|
         * +-----+-----+---+
         * |    1|    5| 10|
         * |    2|   13| 26|
         * |    3|   27| 54|
         * |    4|   39| 78|
         * |    5|   41| 82|
         * |    6|   55|110|
         * +-----+-----+---+
         */


        //Call UDF to pass two Column Values

        tupleDf = tupleDf
                .withColumn("x1*x2",callUDF("x3Multiplier",
                                            tupleDf.col("value"),
                                            tupleDf.col("x2"))
                                            .cast(DataTypes.IntegerType));

        tupleDf.show();

        /** tupleDf.show();
         * +-----+-----+---+-----+
         * |label|value| x2|x1*x2|
         * +-----+-----+---+-----+
         * |    1|    5| 10|   50|
         * |    2|   13| 26|  338|
         * |    3|   27| 54| 1458|
         * |    4|   39| 78| 3042|
         * |    5|   41| 82| 3362|
         * |    6|   55|110| 6050|
         * +-----+-----+---+-----+
         */
    }

}
