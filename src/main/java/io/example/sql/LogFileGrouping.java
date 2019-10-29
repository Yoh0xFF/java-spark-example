package io.example.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class LogFileGrouping {

    public static void main(String[] args) {
        new LogFileGrouping().run();
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

            sqlApi(sparkSession, dataset);

            dataframeApi(sparkSession, dataset);
        }
    }

    private void dataframeApi(SparkSession sparkSession, Dataset<Row> dataset) {
        System.out.println("\n\n---------------> dataframeApi");

        dataset.createOrReplaceTempView("logging_view");

        Dataset<Row> results;

        results = dataset.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
        results = results.groupBy(col("level"), col("month"), col("monthnum")).count();
        results = results.orderBy(col("monthnum"), col("level"));
        results = results.drop("monthnum");

        results.show();
    }

    private void sqlApi(SparkSession sparkSession, Dataset<Row> dataset) {
        System.out.println("\n\n---------------> sqlApi");

        dataset.createOrReplaceTempView("logging_view");

        Dataset<Row> results = sparkSession
                .sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
                        "from logging_view " +
                        "group by level, month " +
                        "order by cast(first(date_format(datetime, 'M')) as int), level");

        results.show();
    }
}
