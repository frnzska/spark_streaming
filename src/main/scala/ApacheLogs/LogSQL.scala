
package ApacheLogs

import java.util.regex.Matcher

import ApacheLogs.Utilities._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/** Illustrates using SparkSQL with Spark Streaming, to issue queries on 
 *  Apache log data extracted from a stream on port 9998.
 */
object LogSQL {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val conf = new SparkConf().setAppName("LogSQL").setMaster("local[*]").set("spark.sql.warehouse.dir", "file:///tmp")
    val ssc = new StreamingContext(conf, Seconds(1))
    
    setupLogging()
    
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9998 locally  -> not working propperly turn on and off while this is running
    val lines = ssc.socketTextStream("127.0.0.1", 9998, StorageLevel.MEMORY_AND_DISK_SER)

    
    // Extract the (URL, status, user agent) we want from each log line
    val requests = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val request = matcher.group(5)
        val requestFields = request.toString().split(" ")
        val url = util.Try(requestFields(1)) getOrElse "[error]"
        (url, matcher.group(6).toInt, matcher.group(9))
      } else {
        ("error", 0, "error")
      }
    })
 
    // Process each RDD from each batch as it comes in
    requests.foreachRDD((rdd, time) => {

      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._


      val requestsDataFrame = rdd.map(w => Record(w._1, w._2, w._3)).toDF()

      requestsDataFrame.createOrReplaceTempView("requests")

      val wordCountsDataFrame =
        sqlContext.sql("select agent, count(*) as total from requests group by agent")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    })
    
    // Kick it off
    ssc.checkpoint("/tmp/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

/** Case class for converting RDD to DataFrame */
case class Record(url: String, status: Int, agent: String)

/** Lazily instantiated singleton instance of SQLContext 
 *  (Straight from included examples in Spark)  */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}


