package twitterExample

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import twitterExample.Utilities._

object MyTweetFilter {


  def has_words(text: Array[String], words: Seq[String]): Boolean ={
    for (w <- text) {
      if (words.contains(w.toLowerCase)) {
        return true
      }
    }
    return false
  }

  def main(args: Array[String]) {

    setupTwitter()
    val ssc = new StreamingContext("local[*]", "SaveTweets", Seconds(1))
    setupLogging()

    val tweets = TwitterUtils.createStream(ssc, None)
    val entries = tweets.map(status => (1, status.getText()))

    val currencies: Seq[String] = List("bitcoin","btc" ,"ether", "etherium",
                                       "litecoin", "requestnetwork", "req")

    val filtered = entries.filter(entry => has_words(entry._2.split(" "), currencies))

    filtered.print()

    ssc.checkpoint("/tmp/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
