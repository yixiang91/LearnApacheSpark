package com.yixiangtay.learning.apache.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

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

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
        // dataset.createOrReplaceTempView("logging_view");
        // Dataset<Row> results = spark.sql(
        //         "SELECT level, DATE_FORMAT(datetime, 'MMMM') AS month, COUNT(1) as total " +
        //                 "FROM logging_view " +
        //                 "GROUP BY level, month " +
        //                 "ORDER BY CAST(FIRST(DATE_FORMAT(datetime, 'M')) AS INT), level");

        // dataframes api
        dataset
                .select(col("level"),
                        date_format(col("datetime"), "MMMM").alias("month"),
                        date_format(col("datetime"), "M").alias("month_num").cast(DataTypes.IntegerType))
                .groupBy(col("level"), col("month"), col("month_num"))
                .count()
                .orderBy(col("month_num"), col("level"))
                .drop(col("month_num"))
                .show(60);

        spark.close();
    }

}
