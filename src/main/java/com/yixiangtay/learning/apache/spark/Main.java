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
        Logger.getLogger("org.apache").setLevel(Level.INFO);

        List<Double> inputData = new ArrayList<Double>();
        inputData.add(35.5);
        inputData.add(12.49943);
        inputData.add(90.32);
        inputData.add(20.32);

        // local[*] - using all cores
        SparkConf conf = new SparkConf().setAppName("sparkApplication").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // load in a collection and turn it into a RDD
        JavaRDD<Double> javaRDD = sc.parallelize(inputData);

        // reduce operation
        double result = javaRDD.reduce((value1, value2) -> {
           return value1 + value2;
        });

        System.out.println(result);

        sc.close();
    }

}
