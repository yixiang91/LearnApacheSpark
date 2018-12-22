package com.yixiangtay.learning.apache.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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

        // flat map
        initialRDD
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .filter(word -> word.matches("[a-zA-Z:]+"))
                .map(value -> value.replace(":", ""))
                .collect().forEach(System.out::println);

        sc.close();
    }

}
