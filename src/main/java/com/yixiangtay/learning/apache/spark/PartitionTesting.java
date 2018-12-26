package com.yixiangtay.learning.apache.spark;

import java.util.Scanner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.spark_project.guava.collect.Iterables;
import scala.Tuple2;

public class PartitionTesting {

    @SuppressWarnings("resource")
    public static void main(String[] args)
    {
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/bigLog.txt");

        // 1. Initial no. of partitions
        System.out.println("Initial RDD Partition Size: " + initialRdd.getNumPartitions());

        JavaPairRDD<String, String> warningsAgainstDate = initialRdd.mapToPair( inputLine -> {
            String[] cols = inputLine.split(":");
            // (salting) hackish way to rename the keys in order to minimize no. of empty partitions
            // during wide transformation, to improve performance
            // String level = cols[0] + (int) (Math.random() * 11);
            String level = cols[0];
            String date = cols[1];
            return new Tuple2<>(level, date);
        });

        // 2. Expecting no change in no. of partitions after a narrow transformation
        System.out.println("After a narrow transformation we have " + warningsAgainstDate.getNumPartitions() + " partitions");

        // Now we're going to do a "wide" transformation
        // groupByKey should be avoided if possible (e.g. use reduceByKey - less data to be transferred during shuffling stage)
        JavaPairRDD<String, Iterable<String>> results = warningsAgainstDate.groupByKey();

        // persisting results data
        results = results.persist(StorageLevel.MEMORY_AND_DISK());

        // 3. No. of partitions after a wide transformation
        // there will be no change in no. of partitions (some will just be empty)
        System.out.println(results.getNumPartitions() + " partitions after the wide transformation");

        // 4. Determine size of each partition
        // data in RDD will be discarded after action
        results.foreach(it -> System.out.println("key " + it._1 + " has " + Iterables.size(it._2) + " elements"));

        // 5. Determine count of results
        // will re-run the previous steps (from reading in data from text file)
        //
        // in actual fact, will only re-run from the last shuffle step since output from last shuffle
        // step is written to disk (and spark uses it for the new action (count))
        System.out.println(results.count());

        System.out.println("Results count: " + results.count());

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        sc.close();
    }

}
