package com.yixiangtay.learning.apache.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        // winutils
        // download from https://github.com/s911415/apache-hadoop-3.1.0-winutils
        System.setProperty("hadoop.home.dir", "C:/Hadoop");

        // logging
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // local[*] - using all cores
        SparkConf conf = new SparkConf().setAppName("sparkApplication").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read from disk
        JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");

        // keyword ranking
        // transformations
        JavaRDD<String> lettersOnlyRDD = initialRDD.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
        JavaRDD<String> removedBlankLines = lettersOnlyRDD.filter(sentence -> sentence.trim().length() > 0);
        JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
        JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);
        JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(Util::isNotBoring);
        JavaPairRDD<String, Long> pairRDD = justInterestingWords.mapToPair(word -> new Tuple2<>(word, 1L));
        JavaPairRDD<String, Long> totals = pairRDD.reduceByKey((value1, value2) -> value1 + value2);
        JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String> (tuple._2, tuple._1));
        JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

        // System.out.println("There are a total of: " + sorted.getNumPartitions() + " partitions");
        // sorted.collect().forEach(System.out::println);

        // action
        List<Tuple2<Long, String>> results = sorted.take(10);
        results.forEach(System.out::println);

        // coalesce() - for reducing the no. of partitions needed just to give the right answer
        // say for e.g. after performing many transformations on multi-terabyte, multi-partition RDD, we're left with a small amount of data
        // for remaining transformations, any shuffle will be pointlessly expensive when done across many partitions

        // collect() - for gathering a small RDD into the driver node (usually for printing)
        // only use it if you're sure the RDD will fit into a single JVM's RAM
        // if the results are big, we'd write to a (e.g. HDFS) file

        // hack to pause the program
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        // narrow transformation
        // - spark does not need to move any data across partitions (e.g. mapToPair)

        // wide transformation (shuffling)
        // - spark needs to serialize the data for transferring across partitions / nodes (e.g. groupByKey)
        // - expensive operation

        sc.close();
    }

}
