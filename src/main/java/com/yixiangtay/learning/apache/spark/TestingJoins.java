package com.yixiangtay.learning.apache.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TestingJoins {

    public static void main(String[] args) {
        // winutils
        // download from https://github.com/s911415/apache-hadoop-3.1.0-winutils
        System.setProperty("hadoop.home.dir", "C:/Hadoop");

        // logging
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // local[*] - using all cores
        SparkConf conf = new SparkConf().setAppName("sparkApplication").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Marybelle"));
        usersRaw.add(new Tuple2<>(6, "Raquel"));

        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

        // inner join
        System.out.println("Inner Join:");
        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRDD = visits.join(users);
        joinedRDD.collect().forEach(System.out::println);
        System.out.println();

        // left outer join
        System.out.println("Left Outer Join:");
        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftOuterJoinedRDD = visits.leftOuterJoin(users);
        leftOuterJoinedRDD.collect().forEach(it-> System.out.println(it._2._2.orElse("-").toUpperCase()));
        System.out.println();

        // right outer join
        System.out.println("Right Outer Join:");
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoinedRDD = visits.rightOuterJoin(users);
        rightOuterJoinedRDD.collect().forEach(it -> System.out.println(it._2._2 + " had " + it._2._1.orElse(0)));
        System.out.println();

        // cartesian join
        System.out.println("Cartesian Join:");
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesianJoinedRDD = visits.cartesian(users);
        cartesianJoinedRDD.collect().forEach(System.out::println);

        sc.close();
    }

}
