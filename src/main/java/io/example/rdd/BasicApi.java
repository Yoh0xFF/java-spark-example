package io.example.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class BasicApi {

    public static void main(String[] args) {
        new BasicApi().run();
    }

    public void run() {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");

        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            reduce(sparkContext);

            map(sparkContext);

            count(sparkContext);

            tuples(sparkContext);

            pairRdd(sparkContext);

            flatMap(sparkContext);

            filter(sparkContext);
        }
    }

    private void filter(JavaSparkContext sparkContext) {
        System.out.println("\n\n---------------> filter");
        List<String> input = Arrays.asList(
                "WARN: Tuesday 4 September 0405",
                "ERROR: Tuesday 4 September 0408",
                "FATAL: Wednesday 5 September 1632",
                "ERROR: Friday 7 September 1854",
                "WARN: Saturday 8 September 1942");

        sparkContext.parallelize(input)
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
                .filter(word -> word.length() > 1)
                .foreach(word -> System.out.println(word));
    }

    private void flatMap(JavaSparkContext sparkContext) {
        System.out.println("\n\n---------------> flatMap");
        List<String> input = Arrays.asList(
                "WARN: Tuesday 4 September 0405",
                "ERROR: Tuesday 4 September 0408",
                "FATAL: Wednesday 5 September 1632",
                "ERROR: Friday 7 September 1854",
                "WARN: Saturday 8 September 1942");

        sparkContext.parallelize(input)
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
                .foreach(word -> System.out.println(word));
    }

    private void pairRdd(JavaSparkContext sparkContext) {
        System.out.println("\n\n---------------> pairRdd");
        List<String> input = Arrays.asList(
                "WARN: Tuesday 4 September 0405",
                "ERROR: Tuesday 4 September 0408",
                "FATAL: Wednesday 5 September 1632",
                "ERROR: Friday 7 September 1854",
                "WARN: Saturday 8 September 1942");

        sparkContext.parallelize(input)
                .mapToPair(x -> new Tuple2<>(x.split(":")[0].trim(), 1L))
                .reduceByKey((x, y) -> x + y)
                .foreach(tuple -> System.out.println(tuple._1 + ": " + tuple._2));
    }

    private void tuples(JavaSparkContext sparkContext) {
        System.out.println("\n\n---------------> tuples");
        List<Integer> input = Arrays.asList(35, 12, 90, 20, 25);

        JavaRDD<Integer> rdd = sparkContext.parallelize(input);
        JavaRDD<Tuple2<Integer, Double>> sqrtRdd = rdd.map(x -> new Tuple2<>(x, Math.sqrt(x)));
        sqrtRdd.foreach(x -> System.out.println(x));
    }

    private void count(JavaSparkContext sparkContext) {
        System.out.println("\n\n---------------> count");
        List<Integer> input = Arrays.asList(35, 12, 90, 20, 25);

        JavaRDD<Integer> rdd = sparkContext.parallelize(input);
        System.out.println(rdd.count());
        System.out.println(rdd.map(x -> 1).reduce((x, y) -> x + y));
    }

    private void map(JavaSparkContext sparkContext) {
        System.out.println("\n\n---------------> mapSqrt");
        List<Integer> input = Arrays.asList(35, 12, 90, 20, 25);

        JavaRDD<Integer> rdd = sparkContext.parallelize(input);
        JavaRDD<Double> sqrtRdd = rdd.map(x -> Math.sqrt(x));
        sqrtRdd.foreach(x -> System.out.println(x));
    }

    private void reduce(JavaSparkContext sparkContext) {
        System.out.println("\n\n---------------> sumByReduce");
        List<Integer> input = Arrays.asList(35, 12, 90, 20, 25);

        JavaRDD<Integer> rdd = sparkContext.parallelize(input);
        int sum = rdd.reduce((x, y) -> x + y);
        System.out.println(sum);
    }
}
