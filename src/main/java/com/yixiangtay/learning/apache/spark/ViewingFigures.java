package com.yixiangtay.learning.apache.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures
{
    @SuppressWarnings("resource")
    public static void main(String[] args)
    {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Use true to use hardcoded data identical to that in the PDF guide.
        boolean testMode = false;

        JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
        JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
        JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

        // Warmup
        JavaPairRDD<Integer, Integer> chapterCountRdd = chapterData.mapToPair( row -> new Tuple2<Integer, Integer>(row._2, 1))
                .reduceByKey( (value1, value2) -> value1 + value2);
        chapterCountRdd.collect().forEach(System.out::println);

        // Step 1 - remove any duplicated views
        System.out.println("\nStep #1: Remove duplicated views");
        viewData = viewData.distinct();
        viewData.collect().forEach(System.out::println);

        // Step 2 - get the course Ids into the RDD
        System.out.println("\nStep #2: Get course ids into the RDD");
        viewData = viewData.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, row._1));
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRdd = viewData.join(chapterData);
        joinedRdd.collect().forEach(System.out::println);

        // Step 3 - don't need chapterIds, setting up for a reduce
        System.out.println("\nStep #3: Don't need chapter ids, setting up for a reduce");
        JavaPairRDD<Tuple2<Integer, Integer>, Long> step3 = joinedRdd.mapToPair( row -> new Tuple2<>(row._2, 1L));
        step3.collect().forEach(System.out::println);

        // Step 4 - count how many views for each user per course
        System.out.println("\nStep #4: Count no. of views for each user per course");
        JavaPairRDD<Tuple2<Integer, Integer>, Long> step4 = step3.reduceByKey((value1, value2) -> value1 + value2);
        step4.collect().forEach(System.out::println);

        // step 5 - remove the userIds
        System.out.println("\nStep #5: Remove user ids");
        JavaPairRDD<Integer, Long> step5 = step4.mapToPair(row -> new Tuple2<>(row._1._2, row._2));
        step5.collect().forEach(System.out::println);

        // step 6 - add in the total chapter count
        System.out.println("\nStep #6: Add in total chapter count");
        JavaPairRDD<Integer, Tuple2<Long, Integer>> step6 = step5.join(chapterCountRdd);
        step6.collect().forEach(System.out::println);

        // step 7 - convert to percentage
        System.out.println("\nStep #7: Convert to percentage");
        // mapValues can be used instead of map since keys are not changed
        JavaPairRDD<Integer, Double> step7 = step6.mapValues(value -> (double) value._1/value._2);
        step7.collect().forEach(System.out::println);

        // step 8 - convert to scores
        System.out.println("\nStep #8: Convert to scores");
        // mapValues can be used instead of map since keys are not changed
        JavaPairRDD<Integer, Long> step8 = step7.mapValues(value -> {
            if (value > 0.9) return 10L;
            if (value > 0.5) return 4L;
            if (value > 0.25) return 2L;
            return 0L;
        });
        step8.collect().forEach(System.out::println);

        // step 9 - reduce by key
        System.out.println("\nStep #9: Reduce by key");
        JavaPairRDD<Integer, Long> step9 = step8.reduceByKey((value1, value2) -> value1 + value2);
        step9.collect().forEach(System.out::println);

        // step 10 - get the course titles into the RDD
        System.out.println("\nStep #10: Get the course titles into the RDD");
        JavaPairRDD<Integer, Tuple2<Long, String>> step10 = step9.join(titlesData);
        step10.collect().forEach(System.out::println);

        // step 11 - final results
        System.out.println("\nStep #11: Final results");
        JavaPairRDD<Long, String> step11 = step10.mapToPair(row -> new Tuple2<>(row._2._1, row._2._2));
        step11.sortByKey(false).collect().forEach(System.out::println);

        sc.close();
    }

    private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode)
        {
            // (chapterId, title)
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
            rawTitles.add(new Tuple2<>(1, "How to find a better job"));
            rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs(rawTitles);
        }
        return sc.textFile("src/main/resources/viewing figures/titles.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode)
        {
            // (chapterId, (courseId, courseTitle))
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96,  1));
            rawChapterData.add(new Tuple2<>(97,  1));
            rawChapterData.add(new Tuple2<>(98,  1));
            rawChapterData.add(new Tuple2<>(99,  2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));
            return sc.parallelizePairs(rawChapterData);
        }

        return sc.textFile("src/main/resources/viewing figures/chapters.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode)
        {
            // Chapter views - (userId, chapterId)
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            return  sc.parallelizePairs(rawViewData);
        }

        return sc.textFile("src/main/resources/viewing figures/views-*.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] columns = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
                });
    }
}
