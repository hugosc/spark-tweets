import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import tweets_sanitize._
import java.time._


object Main {

    def topHashtagsPerDay(dt :RDD[Tweet]) : RDD[(Long, Array[(String, Int)])]= {
        val res1 = dt flatMap { t =>
            t.content.split(" ") filter (_.startsWith("#")) map { c =>
                ((t.datetime.toLocalDate.toEpochDay, c), 1)
            }
        }
        val res2 = res1 reduceByKey (_+_) map {
            case ((l, s), c) => (l, (s, c))
        }

        res2.groupByKey map { case (k, i) => (k, i.toArray.sortWith(_._2 > _._2)) }
    }

    def printTop10Hashtags(agg: RDD[(Long, Array[(String, Int)])]) : Unit = {
        agg foreach {case (d, i) =>
            {println(LocalDate.ofEpochDay(d))
            println("+++++++++++++++++++++")
            i.take(10) foreach {println(_)}
            println("+++++++++++++++++++++")}
        }
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

        println("--------- REPORT ------------")

        val res = topHashtagsPerDay(tweets)
        printTop10Hashtags(res)

        println("-----------------------------")

        sc.stop()
    }
}
