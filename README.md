# Java Spark Example

Sample project demonstrating Spark usage with Java

* **rdd** - examples using Resilient Distributed DataSets
    * **BasicApi** - Demonstrates usage of the basic rdd api
    * **JoinApi** - Demonstrates usage of the join rdd api
    * **KeyworkRanking** - Find most frequently used words in text
    * **TopCoursesByViews** - Find top courses by views
* **sql** - examples using Spark Sql
    * **InMemoryData** - Demonstrate creation of the in-memory data 
    * **LogFileGrouping** - Group log entries by type
    * **PivotTable** - Demonstrate creation of the pivot table
    * **SparkSqlApi** - Demonstrates usage of the basic sql api
    * **StudentScoreStatsByYear** - Calculate students' score statistics by years
    
## SparkUI

To view the execution plan by SparkUI web interface:

1. Pause the execution by adding this line at the end of your code - `new Scanner(System.in).next()`
2. Open this url in your browser - http://localhost:4040