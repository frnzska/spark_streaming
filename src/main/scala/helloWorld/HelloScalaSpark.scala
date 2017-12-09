package helloWorld


import org.apache.spark.sql.SparkSession

object HelloScalaSpark {
  def main(args: Array[String]) {
    val logFile = "/Users/franziskaadler/spark-2.1.2-bin-hadoop2.7/README.md"
    val spark = SparkSession
                .builder()
                .appName("Spark example")
                .config("spark.master", "local")
                .getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
