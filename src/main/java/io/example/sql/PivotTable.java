package io.example.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class PivotTable {

    public static void main(String[] args) {
        new PivotTable().run();
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
                    .csv("src/main/resources/io/example/logs/biglog.txt");

            dataset.createOrReplaceTempView("logging_view");

            Dataset<Row> results;

            results = dataset.select(
                    col("level"),
                    date_format(col("datetime"), "MMMM").alias("month")
            );

            List<Object> months = Arrays.asList(
                    "January", "February", "March", "April",
                    "May", "June", "July", "August",
                    "September", "October", "November", "December"
            );

            results = results.groupBy(col("level")).pivot(col("month"), months).count().na().fill(0);

            results.show();
        }
    }
}
