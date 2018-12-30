package com.yixiangtay.learning.apache.spark;

import java.text.SimpleDateFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class ExamResults {

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

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

        // user-defined functions (modern-way)
        // example #1
        spark.udf().register("hasPassed", (String grade, String subject) -> {
            if (subject.equals("Biology") && !grade.startsWith("A")) {
                return false;
            }
            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
        }, DataTypes.BooleanType);
        // using a literal to invoke a UDF (without registering it)
        // dataset = dataset.withColumn("pass", lit(col("grade").equalTo("A+")));
        // invoking a registered UDF
        dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject")));
        dataset.show(20);

        // example #2
        SimpleDateFormat input = new SimpleDateFormat("MMMM");
        SimpleDateFormat output = new SimpleDateFormat("M");
        spark.udf().register("monthNum", (String month) -> {
            java.util.Date inputDate = input.parse(month);
            return Integer.parseInt(output.format(inputDate));
        }, DataTypes.IntegerType);
        Dataset<Row> dataset2 = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
        dataset2.createOrReplaceTempView("logging_view");
        Dataset<Row> results = spark.sql(
                "SELECT level, DATE_FORMAT(datetime, 'MMMM') AS month, COUNT(1) AS total " +
                        "FROM logging_view " +
                        "GROUP BY level, month " +
                        "ORDER BY monthNum(month), level"
        );
        results.show();

        spark.close();
    }

}
