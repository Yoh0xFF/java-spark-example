package io.example.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class TopCoursesByViews {

    public static void main(String[] args) {
        new TopCoursesByViews().run();
    }

    public void run() {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");

        boolean testMode = false;

        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            analyze(sparkContext, testMode);
        }
    }

    private void analyze(JavaSparkContext sparkContext, boolean testMode) {
        /**
         * If a user watches more than 90% of the course, the course gets 10 points.
         * If a user watches > 50% but < 90% it scores 4.
         * If a user watches > 25% but < 50% it scores 2.
         * Otherwise, no score.
         */
        JavaPairRDD<Integer, Integer> viewDataRdd
                = setUpViewDataRdd(sparkContext, testMode); // (userId, chapterId)
        JavaPairRDD<Integer, Integer> chapterDataRdd
                = setUpChapterDataRdd(sparkContext, testMode); // (chapterId, courseId)
        JavaPairRDD<Integer, String> titlesDataRdd
                = setUpTitlesDataRdd(sparkContext, testMode); // (courseId, title)

        // Step 0 - count chapters in courses
        JavaPairRDD<Integer, Integer> chapterCountRdd = chapterDataRdd
                .mapToPair(row -> new Tuple2<>(row._2, 1))
                .reduceByKey((x, y) -> x + y); // (courseId, chapterCount)

        // Step 1 - remove any duplicated views
        viewDataRdd = viewDataRdd.distinct(); // (userId, chapterId)

        // Step 2 - get the course ids into the rdd
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedChapterCourseRdd = viewDataRdd
                .mapToPair(row -> new Tuple2<>(row._2, row._1))
                .join(chapterDataRdd); // (chapterId, (userId, courseId))

        // Step 3 - don't need chapter ids, setting up for a reduce
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> userCourseViewRdd = joinedChapterCourseRdd
                .mapToPair(row -> new Tuple2<>(row._2, 1)); // ((userId, courseId), 1)

        // Step 4 - count how many views for each user per course
        userCourseViewRdd = userCourseViewRdd
                .reduceByKey((x, y) -> x + y); // ((userId, courseId), sumOfChapterViews)

        // Step 5 - remove the user ids
        JavaPairRDD<Integer, Integer> courseViewRdd = userCourseViewRdd
                .mapToPair(row -> new Tuple2<>(row._1._2, row._2)); // (courseId, sumOfChapterViews)

        // Step 6 - add in the total chapter count
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedCourseViewChapterCountRdd = courseViewRdd
                .join(chapterCountRdd); // (courseId, (sumOfChapterViews, totalChapterCount))

        // Step 7 - convert to percentage
        JavaPairRDD<Integer, Double> courseViewPercentageRdd = joinedCourseViewChapterCountRdd
                .mapValues(value -> (double) value._1 / value._2); // (courseId, viewPercentage)

        // Step 8 - convert to scores
        JavaPairRDD<Integer, Long> courseViewScoreRdd = courseViewPercentageRdd
                .mapValues(value -> {
                    if (value > 0.9) return 10L;
                    if (value > 0.5) return 4L;
                    if (value > 0.25) return 2L;
                    return 0L;
                }); // (courseId, viewScore)

        // Step 9 - sum course view scores
        JavaPairRDD<Integer, Long> courseViewScoreSumRdd = courseViewScoreRdd
                .reduceByKey((x, y) -> x + y); // (courseId, sumOfViewScore)

        // Step 10 - join course view scores with titles
        JavaPairRDD<Long, String> resultRdd = courseViewScoreSumRdd
                .join(titlesDataRdd)
                .mapToPair(row -> new Tuple2<>(row._2._1, row._2._2))
                .sortByKey(false); // (sumOfViewScore, title)
        resultRdd.collect().forEach(row -> System.out.println(row));
    }

    private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sparkContext, boolean testMode) {
        if (testMode) {
            // (courseId, title)
            return sparkContext.parallelizePairs(Arrays.asList(
                    new Tuple2<>(1, "How to find a better job"),
                    new Tuple2<>(2, "Work faster harder smarter until you drop"),
                    new Tuple2<>(3, "Content Creation is a Mug's Game")
            ));
        }

        return sparkContext
                .textFile("src/main/resources/io/example/views/titles.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<>(new Integer(cols[0]), cols[1]);
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sparkContext, boolean testMode) {
        if (testMode) {
            // (chapterId, courseId)
            return sparkContext.parallelizePairs(Arrays.asList(
                    new Tuple2<>(96, 1),
                    new Tuple2<>(97, 1),
                    new Tuple2<>(98, 1),
                    new Tuple2<>(99, 2),
                    new Tuple2<>(100, 3),
                    new Tuple2<>(101, 3),
                    new Tuple2<>(102, 3),
                    new Tuple2<>(103, 3),
                    new Tuple2<>(104, 3),
                    new Tuple2<>(105, 3),
                    new Tuple2<>(106, 3),
                    new Tuple2<>(107, 3),
                    new Tuple2<>(108, 3),
                    new Tuple2<>(109, 3)
            ));
        }

        return sparkContext
                .textFile("src/main/resources/io/example/views/chapters.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<>(new Integer(cols[0]), new Integer(cols[1]));
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sparkContext, boolean testMode) {
        if (testMode) {
            // (userId, chapterId)
            return sparkContext.parallelizePairs(Arrays.asList(
                    new Tuple2<>(14, 96),
                    new Tuple2<>(14, 97),
                    new Tuple2<>(13, 96),
                    new Tuple2<>(13, 96),
                    new Tuple2<>(13, 96),
                    new Tuple2<>(14, 99),
                    new Tuple2<>(13, 100)
            ));
        }

        return sparkContext
                .textFile("src/main/resources/io/example/views/views-*.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] columns = commaSeparatedLine.split(",");
                    return new Tuple2<>(new Integer(columns[0]), new Integer(columns[1]));
                });
    }
}
