package com.yixiangtay.learning.apache.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class ViewingFiguresStructuredVersion {

    public static void main(String[] args) throws StreamingQueryException {
        // winutils
        // download from https://github.com/s911415/apache-hadoop-3.1.0-winutils
        System.setProperty("hadoop.home.dir", "C:/Hadoop");

        // logging
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("structuredViewingReport")
                .config("spark.sql.shuffle.partitions", "10")
                .getOrCreate();

        Dataset<Row> df =
                session.readStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", "localhost:9092")
                        .option("subscribe", "viewrecords")
                        .load();

        // dataframe operations
        df.createOrReplaceTempView("viewing_figures");

        // columns: key, value, timestamp
        Dataset<Row> results =
                session.sql(
                        "SELECT window, CAST(value AS STRING) AS course_name, SUM(5) AS seconds_watched " +
                                "FROM viewing_figures " +
                                "GROUP BY WINDOW(timestamp, '2 minutes'), course_name"
                );

        // spark structured streaming works with micro-batches (by default)
        // as soon as data is obtained, job will be ran to update the current state of play
        StreamingQuery query =
                results.writeStream()
                        .format("console")
                        .outputMode(OutputMode.Update()) // complete mode is only possible for streaming aggregations
                        .option("truncate", false)
                        .option("numRows", 50)
                        .start();

        query.awaitTermination();

    }

}
