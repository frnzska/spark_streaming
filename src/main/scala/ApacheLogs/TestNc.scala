
package ApacheLogs
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.log4j.{Level, Logger}


object TestNc {
 
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // 10 msec batch size
    val ssc = new StreamingContext("local[*]", "test", Milliseconds(10))


    // nc -kl 9998 < access_log.txt
    val lines = ssc.socketTextStream("127.0.0.1", 9998)

    val c = lines.count()
    c.print()

    ssc.checkpoint("/tmp/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

