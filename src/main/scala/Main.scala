import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import tweets_sanitize._
import org.apache.spark.rdd._
import java.time._

object Main {
    def maping(arg: RDD[Tweet]) = {
      arg map (t => ((t.datetime.toLocalDate().toEpochDay() , t.datetime.getHour()),1))
    }
    def question3(arg: RDD[Tweet]) : Unit = {
      val adjusted = maping(arg)
      val result = adjusted.countByKey()
      val sortedKeys = result.keys.toArray.sortBy(x=>x)
      sortedKeys.foreach(k =>
        println("" + LocalDate.ofEpochDay(k._1) + " " + k._2 + "->" + result(k))
      )
    }

    def main(args: Array[String]) : Unit = {

        val sparkConf = (new SparkConf()).setAppName("SparkTweets").setMaster("local[4]")
        val sc = new SparkContext(sparkConf)
        sc.setLogLevel("WARN")
        /**
        *
        * As chamadas para os comandos ficam aqui antes do sc.stop
        *
        */

        val dt = sc.textFile("data/dataset.tsv")
        val tweets = dt map (TweetUtils.rawToTweet)
        println(tweets.first)
        question3(tweets)



        sc.stop()
    }
}
