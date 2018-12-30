package com.yixiangtay.learning.apache.spark;

import java.util.Scanner;
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

        // spark sql syntax
        // set to a lower no. of partitions (for small datasets) for huge improvement in performance
        spark.conf().set("spark.sql.shuffle.partitions", "12");
        dataset.createOrReplaceTempView("logging_view");

        // SortAggregate
        Dataset<Row> results = spark.sql(
                "SELECT level, DATE_FORMAT(datetime, 'MMMM') AS month, COUNT(1) AS total " +
                        "FROM logging_view " +
                        "GROUP BY level, month " +
                        "ORDER BY CAST(FIRST(DATE_FORMAT(datetime, 'M')) AS INT), level"
        );
        results.drop("month_num");
        results.show();
        results.explain();
        // == Physical Plan ==
        // *(3) Project [level#10, month#14, total#15L]
        // +- *(3) Sort [aggOrder#44 ASC NULLS FIRST, level#10 ASC NULLS FIRST], true, 0
        //         +- Exchange rangepartitioning(aggOrder#44 ASC NULLS FIRST, level#10 ASC NULLS FIRST, 12)
        //         +- SortAggregate(key=[level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(GMT+08:00))#46], functions=[count(1), first(date_format(cast(datetime#11 as timestamp), M, Some(GMT+08:00)), false)])
        // +- *(2) Sort [level#10 ASC NULLS FIRST, date_format(cast(datetime#11 as timestamp), MMMM, Some(GMT+08:00))#46 ASC NULLS FIRST], false, 0
        //         +- Exchange hashpartitioning(level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(GMT+08:00))#46, 12)
        // +- SortAggregate(key=[level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(GMT+08:00)) AS date_format(cast(datetime#11 as timestamp), MMMM, Some(GMT+08:00))#46], functions=[partial_count(1), partial_first(date_format(cast(datetime#11 as timestamp), M, Some(GMT+08:00)), false)])
        // +- *(1) Sort [level#10 ASC NULLS FIRST, date_format(cast(datetime#11 as timestamp), MMMM, Some(GMT+08:00)) AS date_format(cast(datetime#11 as timestamp), MMMM, Some(GMT+08:00))#46 ASC NULLS FIRST], false, 0
        //         +- *(1) FileScan csv [level#10,datetime#11] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/Users/tayyi/IdeaProjects/apache-spark/src/main/resources/biglog.txt], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<level:string,datetime:string>

        // HashAggregate
        Dataset<Row> results2 = spark.sql(
                "SELECT level, DATE_FORMAT(datetime, 'MMMM') AS month, COUNT(1) AS total, FIRST(CAST(DATE_FORMAT(datetime, 'M') AS INT)) AS month_num " +
                        "FROM logging_view " +
                        "GROUP BY level, month " +
                        "ORDER BY month_num, level"
        );
        results2.drop("month_num");
        results2.show();
        results2.explain();
        // == Physical Plan ==
        // *(3) Sort [month_num#59 ASC NULLS FIRST, level#10 ASC NULLS FIRST], true, 0
        //         +- Exchange rangepartitioning(month_num#59 ASC NULLS FIRST, level#10 ASC NULLS FIRST, 12)
        //         +- *(2) HashAggregate(keys=[level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(GMT+08:00))#96], functions=[count(1), first(cast(date_format(cast(datetime#11 as timestamp), M, Some(GMT+08:00)) as int), false)])
        // +- Exchange hashpartitioning(level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(GMT+08:00))#96, 12)
        // +- *(1) HashAggregate(keys=[level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(GMT+08:00)) AS date_format(cast(datetime#11 as timestamp), MMMM, Some(GMT+08:00))#96], functions=[partial_count(1), partial_first(cast(date_format(cast(datetime#11 as timestamp), M, Some(GMT+08:00)) as int), false)])
        // +- *(1) FileScan csv [level#10,datetime#11] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/Users/tayyi/IdeaProjects/apache-spark/src/main/resources/biglog.txt], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<level:string,datetime:string>


        // spark java syntax
        dataset
                .select(col("level"),
                        date_format(col("datetime"), "MMMM").alias("month"),
                        date_format(col("datetime"), "M").alias("month_num").cast(DataTypes.IntegerType))
                .groupBy("level", "month", "month_num")
                .count().alias("total")
                .show();
        dataset.explain();
        // == Physical Plan ==
        // *(1) FileScan csv [level#10,datetime#11] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/Users/tayyi/IdeaProjects/apache-spark/src/main/resources/biglog.txt], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<level:string,datetime:string>

        // SortAggregation (slower O(N log N) but more memory efficient) v.s. HashAggregation (faster O(N) but less memory efficient)
        // spark will use HashAggregation where possible
        // HashAggregation is only possible if the data for the "value" is mutable (list of types that are mutable can be found in link in reference)
        // key(s): included in group by operation, value(s) - not included in group by operation
        // the "HashMap" used for HashAggregate is not a java HashMap (it is implemented in native memory using the Unsafe class, which is removed in java 9)
        // reference(s):
        // https://github.com/apache/spark/blob/master/sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/UnsafeRow.java

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        spark.close();
    }

}
