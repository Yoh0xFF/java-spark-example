package io.example;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class KeywordRanking {

    public static void main(String[] args) {
        new KeywordRanking().run();
    }

    public void run() {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]")
                .set("spark.authenticate", "true")
                .set("spark.authenticate.secret", "some-big-secret");

        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            keywordRanking(sparkContext);
        }
    }

    public void keywordRanking(JavaSparkContext sparkContext) {
        JavaRDD<String> initialRDD = sparkContext
                .textFile("src/main/resources/io/example/subtitles/input-spring.txt");

        JavaPairRDD<Long, String> result = initialRDD
                .map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase()) // leave only letters
                .filter(sentence -> StringUtils.isNotBlank(sentence)) // filter blank lines
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator()) // explode sentence into words
                .filter(word -> StringUtils.isNotBlank(word) && Util.isNotBoring(word)) // filter empty and boring words
                .mapToPair(word -> new Tuple2<>(word, 1L)) // make pair rdd
                .reduceByKey((x, y) -> x + y) // count words
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1)) // switch key and value places
                .sortByKey(false); // sort result by key

        result.take(50).forEach(System.out::println);
    }
}

class Util {

    private static final Set<String> boringWords = new HashSet<>();

    static {
        try (InputStream is = Util.class.getResourceAsStream("subtitles/boring-words.txt")) {
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            br.lines().forEach(boringWords::add);
        } catch (IOException ex) {
            throw new AssertionError(ex);
        }
    }

    public static boolean isBoring(String word) {
        return boringWords.contains(word);
    }

    public static boolean isNotBoring(String word) {
        return !isBoring(word);
    }
}