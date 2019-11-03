package io.example.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.stddev;

public class StudentScoreStatsByYear {

    public static void main(String[] args) {
        new StudentScoreStatsByYear().run();
    }

    public void run() {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        try (SparkSession sparkSession = SparkSession
                .builder()
                .appName("StartingSpark")
                .master("local[*]")
                .getOrCreate()) {
            Dataset<Row> dataset = sparkSession.read()
                    .option("header", true)
                    .csv("src/main/resources/io/example/exams/students.csv");

            dataset = dataset.groupBy(col("subject"))
                    .pivot(col("year"))
                    .agg(
                            round(avg(col("score")), 2).alias("avg_score"),
                            round(stddev(col("score")), 2).alias("stddev_score")
                    );
            dataset.show();
        }
    }
}
