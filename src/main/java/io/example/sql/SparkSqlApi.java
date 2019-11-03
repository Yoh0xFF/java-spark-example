package io.example.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class SparkSqlApi {

    public static void main(String[] args) {
        new SparkSqlApi().run();
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

            showAndCountData(dataset);

            readingRow(dataset);

            filterWithExpression(dataset);

            filterWithLambda(dataset);

            filterWithColumn(dataset);

            fullSqlSyntax(sparkSession, dataset);

            groupingWithSql(sparkSession, dataset);

            groupingWithApi(dataset);

            userDefinedFunctionWithSql(sparkSession, dataset);

            userDefinedFunctionWithApi(sparkSession, dataset);
        }
    }

    private void userDefinedFunctionWithApi(SparkSession sparkSession, Dataset<Row> dataset) {
        System.out.println("\n\n---------------> userDefinedFunctionWithApi");

        sparkSession.udf().register("hasPassed", (String grade, String subject) -> {
            if ("Biology".equals(subject)) {
                return grade.startsWith("A");
            }
            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
        }, DataTypes.BooleanType);

        dataset = dataset.withColumn("paas", callUDF("hasPassed", col("grade"), col("subject")));
        dataset.show();
    }

    private void userDefinedFunctionWithSql(SparkSession sparkSession, Dataset<Row> dataset) {
        System.out.println("\n\n---------------> userDefinedFunctionWithSql");

        dataset.createOrReplaceTempView("my_students_view");

        sparkSession.udf().register("hasPassed", (String grade, String subject) -> {
            if ("Biology".equals(subject)) {
                return grade.startsWith("A");
            }
            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
        }, DataTypes.BooleanType);

        dataset = sparkSession
                .sql("select subject, grade, hasPassed(grade, subject) as cnt from my_students_view");
        dataset.show();
    }

    private void groupingWithApi(Dataset<Row> dataset) {
        System.out.println("\n\n---------------> groupingWithApi");

        dataset = dataset.groupBy(col("subject")).agg(
                max(col("score")).alias("max_score"),
                min(col("score")).alias("min_score"),
                avg(col("score")).alias("avg_score")
        );
        dataset.show();
    }

    private void groupingWithSql(SparkSession sparkSession, Dataset<Row> dataset) {
        System.out.println("\n\n---------------> groupingWithSql");

        dataset.createOrReplaceTempView("my_students_view");

        dataset = sparkSession
                .sql("select subject, count(1) as cnt from my_students_view " +
                        "group by subject order by subject asc");
        dataset.show();
    }

    private void fullSqlSyntax(SparkSession sparkSession, Dataset<Row> dataset) {
        System.out.println("\n\n---------------> fullSqlSyntax");

        dataset.createOrReplaceTempView("my_students_view");

        Dataset<Row> mathResults = sparkSession
                .sql("select student_id, score, grade " +
                        "from my_students_view where subject = 'Math' and year = 2005 " +
                        "order by student_id desc");
        mathResults.show();

        Dataset<Row> allSubjectResults = sparkSession
                .sql("select distinct(subject) from my_students_view " +
                        "order by subject asc");
        allSubjectResults.show();
    }

    private void filterWithColumn(Dataset<Row> dataset) {
        System.out.println("\n\n---------------> filterWithColumn");

        dataset = dataset.filter(col("subject").equalTo("Math").and(col("year").equalTo(2005)));
        dataset.show();
    }

    private void filterWithLambda(Dataset<Row> dataset) {
        System.out.println("\n\n---------------> filterWithLambda");

        dataset = dataset.filter(row ->
                row.getAs("subject").equals("Math") && Integer.parseInt(row.getAs("year")) == 2005);
        dataset.show();
    }

    private void filterWithExpression(Dataset<Row> dataset) {
        System.out.println("\n\n---------------> filterWithExpression");

        dataset = dataset.filter("subject = 'Math' and year = 2005");
        dataset.show();
    }

    private void readingRow(Dataset<Row> dataset) {
        System.out.println("\n\n---------------> readingRow");

        Row row = dataset.first();

        String subject = row.getAs("subject");
        int year = Integer.parseInt(row.getAs("year"));
        System.out.println("First row, subject is " + subject + ", year is " + year);
    }

    private void showAndCountData(Dataset<Row> dataset) {
        System.out.println("\n\n---------------> showAndCountData");

        dataset.show();
        System.out.println("There are " + dataset.count() + " rows");
    }
}
