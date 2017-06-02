import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import tweets_sanitize._

object Main {
    def main(args: Array[String]) : Unit = {

        val sparkConf = (new SparkConf()).setAppName("SparkTweets").setMaster("local[4]")
        val sc = new SparkContext(sparkConf)

        /**
        *
        * As chamadas para os comandos ficam aqui antes do sc.stop
        *
        */

        val dt = sc.textFile("data/dataset.tsv")
        val tweets = dt map (TweetUtils.rawToTweet)
        println(tweets.first)

        sc.stop()
    }
}
