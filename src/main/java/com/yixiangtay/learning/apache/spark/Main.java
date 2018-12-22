package com.yixiangtay.learning.apache.spark;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
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
        // using Java class
        // JavaRDD<IntegerWithSquareRoot> sqrtRDD = javaRDD.map(value -> new IntegerWithSquareRoot(value));
        // using Scala tuple
        JavaRDD<Tuple2<Integer, Double>> sqrtRDD = javaRDD.map(value -> new Tuple2<>(value, Math.sqrt(value)));

        Tuple2<Integer, Double> scalaTuple = new Tuple2<>(9, 3.0);

        sc.close();
    }

}
