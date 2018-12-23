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
        JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input-spring.txt");

        // keyword ranking
        JavaRDD<String> lettersOnlyRDD = initialRDD.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
        JavaRDD<String> removedBlankLines = lettersOnlyRDD.filter(sentence -> sentence.trim().length() > 0);
        JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
        JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);
        JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(Util::isNotBoring);
        JavaPairRDD<String, Long> pairRDD = justInterestingWords.mapToPair(word -> new Tuple2<>(word, 1L));
        JavaPairRDD<String, Long> totals = pairRDD.reduceByKey((value1, value2) -> value1 + value2);
        JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String> (tuple._2, tuple._1));
        JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

        List<Tuple2<Long, String>> results = sorted.take(10);
        results.forEach(System.out::println);

        sc.close();
    }

}
