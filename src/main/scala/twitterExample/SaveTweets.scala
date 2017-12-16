package twitterExample

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import Utilities._

/** Listens to a stream of tweets and saves them to disk. */
object SaveTweets {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "SaveTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "SaveTweets", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())
    //statuses.saveAsTextFiles("Tweets", "txt")
    var totalTweets:Long = 0

    statuses.foreachRDD((rdd, time) => {
      // Don't bother with empty batches
      if (rdd.count() > 0) {
        // Combine each partition's results into a single RDD:
        val repartitionedRDD = rdd.repartition(1).cache()
        // And print out a directory with the results.
        repartitionedRDD.saveAsTextFile("Tweets_" + time.milliseconds.toString)
        // Stop once we've collected 1000 tweets.
        totalTweets += repartitionedRDD.count()
        println("Tweet count: " + totalTweets)
        if (totalTweets > 1000) {
          System.exit(0)
        }
      }
    })

    // You can also write results into a database of your choosing, but we'll do that later.

    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("/tmp")
    ssc.start()
    ssc.awaitTermination()
  }
}
