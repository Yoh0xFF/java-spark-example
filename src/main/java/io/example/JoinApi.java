package io.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Int;
import scala.Option;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class JoinApi {

    private final List<Tuple2<Integer, Integer>> visitsRaw = Arrays.asList(
            new Tuple2<>(4, 18),
            new Tuple2<>(6, 4),
            new Tuple2<>(10, 9)
    );

    private final List<Tuple2<Integer, String>> usersRaw = Arrays.asList(
            new Tuple2<>(1, "John"),
            new Tuple2<>(2, "Bob"),
            new Tuple2<>(3, "Alan"),
            new Tuple2<>(4, "Doris"),
            new Tuple2<>(5, "Marybelle"),
            new Tuple2<>(6, "Raquel"),
            new Tuple2<>(7, "Ada")
    );

    public static void main(String[] args) {
        new JoinApi().run();
    }

    public void run() {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");

        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            innerJoin(sparkContext);

            leftJoin(sparkContext);

            rightJoin(sparkContext);

            fullJoin(sparkContext);
        }
    }

    private void fullJoin(JavaSparkContext sparkContext) {
        System.out.println("\n\n---------------> fullJoin");

        JavaPairRDD<Integer, Integer> visitsRdd = sparkContext.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> usersRdd = sparkContext.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> joinedRdd
                = visitsRdd.fullOuterJoin(usersRdd);
        joinedRdd.collect().forEach(System.out::println);
    }

    private void rightJoin(JavaSparkContext sparkContext) {
        System.out.println("\n\n---------------> rightJoin");

        JavaPairRDD<Integer, Integer> visitsRdd = sparkContext.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> usersRdd = sparkContext.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd
                = visitsRdd.rightOuterJoin(usersRdd);

        joinedRdd.collect().forEach(System.out::println);
    }

    private void leftJoin(JavaSparkContext sparkContext) {
        System.out.println("\n\n---------------> leftJoin");

        JavaPairRDD<Integer, Integer> visitsRdd = sparkContext.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> usersRdd = sparkContext.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd
                = visitsRdd.leftOuterJoin(usersRdd);

        joinedRdd.collect().forEach(System.out::println);
    }

    private void innerJoin(JavaSparkContext sparkContext) {
        System.out.println("\n\n---------------> innerJoin");

        JavaPairRDD<Integer, Integer> visitsRdd = sparkContext.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> usersRdd = sparkContext.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd
                = visitsRdd.join(usersRdd);

        joinedRdd.collect().forEach(System.out::println);
    }
}
