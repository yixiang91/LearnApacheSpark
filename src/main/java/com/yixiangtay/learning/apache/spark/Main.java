package com.yixiangtay.learning.apache.spark;

import java.util.ArrayList;
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

        List<Integer> inputData = new ArrayList<Integer>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        // local[*] - using all cores
        SparkConf conf = new SparkConf().setAppName("sparkApplication").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // load in a collection and turn it into a RDD
        JavaRDD<Integer> javaRDD = sc.parallelize(inputData);

        // map operation
        JavaRDD<Double> sqrtRDD = javaRDD.map(value -> Math.sqrt(value));
        // sqrtRDD.foreach(value -> System.out.println(value)); // println is not serializable
        sqrtRDD.collect().forEach(System.out::println);

        // count the elements
        System.out.println(sqrtRDD.count());
        // using just map & reduce
        JavaRDD<Long> singleLongRDD = sqrtRDD.map(value -> 1L);
        Long count = singleLongRDD.reduce((value1, value2) -> value1 + value2);
        System.out.println(count);

        sc.close();
    }

}
