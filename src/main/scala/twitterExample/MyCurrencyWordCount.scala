package twitterExample

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._

/** Listens to a stream of tweets and saves them to disk. */
object MyCurrencyWordCount {
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    setupTwitter()
    val ssc = new StreamingContext("local[*]", "SaveTweets", Seconds(2))
    setupLogging()

    val tweets = TwitterUtils.createStream(ssc, None)
    val statuses = tweets.map(status => status.getText())
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))

    val currencies: Seq[String] = List("bitcoin","Bitcoin","btc", "BTC" ,"ether", "Ether", "etherium", "Etherium",
                                       "litecoin", "Litecoin", "RequestNetwork", "requestnetwork", "req", "REQ", "Req")
    val tweetcurrwords = tweetwords.filter(word => currencies.contains(word))

    val CurrencyKeyValues = tweetcurrwords.map(curr => (curr, 1))

    // look back 10 min and update every 3 sec
    val CurrencyCounts = CurrencyKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(600), Seconds(2))
    val sortedResults = CurrencyCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print(5)
    
    // You can also write results into a database of your choosing, but we'll do that later.
    
    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("/tmp/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
