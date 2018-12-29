package com.yixiangtay.learning.apache.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

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

        long numberOfRows = dataset.count();
        System.out.println("There are " + numberOfRows + " records.");

        Row firstRow = dataset.first();
        // get() returns Object
        String subjectUsingGet = firstRow.get(2).toString();
        // getAs() returns String
        String subjectUsingGetAs = firstRow.getAs("subject");
        System.out.println("Subject using get(): " + subjectUsingGet);
        System.out.println("Subject using getAs(): " + subjectUsingGetAs);
        int year = Integer.parseInt(firstRow.getAs("year"));
        System.out.println("Year: " + year);

        // filter using expressions
        Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");
        modernArtResults.show();

        // filter using lambdas
        Dataset<Row> modernArtResults2 = dataset.filter(
                (FilterFunction<Row>) row ->
                        row.getAs("subject").equals("Modern Art")
                        && Integer.parseInt(row.getAs("year")) >= 2007);
        modernArtResults2.show();

        // filter using columns
        Dataset<Row> modernArtResults3 =
                dataset.filter(
                        col("subject").equalTo("Modern Art")
                        .and(col("year").geq(2007))
        );
        modernArtResults3.show();

        spark.close();
    }

}
