package com.nitendratech.sparkgeneral.sql;

import com.nitendratech.sparkgeneral.util.AppUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by @author nitendratech on 8/15/20
 */
public class BooksJoin {

    public static void main(String [] args){


        SparkSession sparkSession = AppUtil.getSparkSession("BooksJoin","local[*]");

    //Read Data from CSV


        JSONObject booksSchemaFromJson =
                AppUtil.readJsonFromFile("datasets/books_data/books_schema_details.json");




        JSONArray booksArray =
                AppUtil.getJsonArrayFromJsonObject(booksSchemaFromJson,"book_details");
        JSONArray authorArray =
                AppUtil.getJsonArrayFromJsonObject(booksSchemaFromJson,"author_details");


        String book_details = AppUtil.getValFrJsonArrayNoQuotes(booksArray);
        String author_details = AppUtil.getValFrJsonArrayNoQuotes(authorArray);



        //Read Data from CSV


        Dataset<Row> authorsData =sparkSession.read()
                .option("header",true)
                .csv("datasets/books_data/authors.csv");

        Dataset<Row> booksData =sparkSession.read()
                .option("header",true)
                .csv("datasets/books_data/books.csv");


        authorsData.createOrReplaceTempView("authorsDataV");

        booksData.createOrReplaceTempView("booksDataV");



        String authSQL = "SELECT " + author_details + " FROM authorsDataV author";

        String bookSQL = "SELECT " + book_details + " FROM booksDataV book";


        System.out.println(bookSQL);

       Dataset<Row> authDataset = sparkSession.sql(authSQL);
        Dataset<Row> bookDataset= sparkSession.sql(bookSQL);



        // Now Join two Tables with alias on joined Columns

        Dataset<Row> joinedDataset = authDataset.as("auth_data")
                .join(bookDataset.as("book_data"),
                        authDataset.col("auth_id")
                                .equalTo(bookDataset.col("book_id")));



        joinedDataset.show();



        joinedDataset.select(col("auth_data.auth_id")
                ,col("auth_data.author_name")
                ,col("auth_data.amazon_link")
                ,col("book_data.author_id")
                ,col("book_data.author_title")
                ,col("book_data.releaseDate")).show();


    }
}
