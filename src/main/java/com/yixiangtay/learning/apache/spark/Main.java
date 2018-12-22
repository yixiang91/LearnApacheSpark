package com.yixiangtay.learning.apache.spark;

import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Main {

    public static void main(String[] args) {
        // winutils
        // download from https://github.com/s911415/apache-hadoop-3.1.0-winutils
        System.setProperty("hadoop.home.dir", "C:/Hadoop");

        // logging
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        // local[*] - using all cores
        SparkConf conf = new SparkConf().setAppName("sparkApplication").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // reduce by key
        sc.parallelize(inputData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .reduceByKey((value1, value2) -> value1 + value2)
                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
        // group by key
        // performance issues (not recommended)
        sc.parallelize(inputData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .groupByKey()
                .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));

        sc.close();
    }

}
