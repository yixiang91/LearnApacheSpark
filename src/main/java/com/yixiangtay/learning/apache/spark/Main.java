package com.yixiangtay.learning.apache.spark;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main {

    public static void main(String[] args) {
        // winutils
        // download from https://github.com/s911415/apache-hadoop-3.1.0-winutils
        System.setProperty("hadoop.home.dir", "C:/Hadoop");

        // logging
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // JavaSparkContext is used for JavaRDD operations
        // local[*] - using all cores
        // SparkConf conf = new SparkConf().setAppName("sparkApplication").setMaster("local[*]");
        // JavaSparkContext sc = new JavaSparkContext(conf);

        // SparkSession is used for SparkSQL operations
        SparkSession spark =
                SparkSession
                        .builder()
                        .appName("sparkSql")
                        .master("local[*]")
                        .config("spark.sql.warehouse.dir", "file:///C:/tmp/")
                        .getOrCreate();

        // multiple groupings
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
        dataset.createOrReplaceTempView("logging_view");
        Dataset<Row> results = spark.sql(
                "SELECT level, DATE_FORMAT(datetime, 'MMMM') AS month, COUNT(1) as total " +
                        "FROM logging_view " +
                        "GROUP BY level, month");
        results.show(60);

        results.createOrReplaceTempView("results_view");
        Dataset<Row> totals = spark.sql("SELECT SUM(total) FROM results_view");
        totals.show();

        spark.close();
    }

}
