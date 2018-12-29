package com.yixiangtay.learning.apache.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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

        // RDD is still being used in the underhood of the dataset
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
        dataset.show(10);

        // full sql syntax
        dataset.createOrReplaceTempView("my_students_view");
        Dataset<Row> results = spark.sql("SELECT distinct(year) FROM my_students_view WHERE subject='French' ORDER BY year DESC");
        results.show();

        spark.close();
    }

}
