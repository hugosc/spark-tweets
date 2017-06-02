import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Main {
    def main(args: Array[String]) : Unit = {

        val sparkConf = (new SparkConf()).setAppName("SparkTweets").setMaster("local[4]")
        val sc = new SparkContext(sparkConf)

        /**
        *
        * As chamadas para os comandos ficam aqui antes do sc.stop
        *
        */

        sc.stop()
    }
}
