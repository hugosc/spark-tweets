package tweets_sanitize

import java.time._
import java.io.Serializable

case class Tweet(id: Long,
                content: String,
                datetime: LocalDateTime,
                city: String,
                username: String)


object TweetUtils extends Serializable {
    val monthMap = Map[String, Int](
        "Jan" -> 1,
        "Feb" -> 2,
        "Mar" -> 3,
        "Apr" -> 4,
        "May" -> 5,
        "Jun" -> 6,
        "Jul" -> 7,
        "Aug" -> 8,
        "Sep" -> 9,
        "Oct" -> 10,
        "Nov" -> 11,
        "Dez" -> 12
    )

    def rawToTweet(raw: String) : Tweet = {

        val sp = raw filter (_ != '"') split ("\t")

        val rawDateTime = sp(7)

        val tmp = rawDateTime split (" ")

        val year = tmp(5).toInt
        val month = monthMap(tmp(1)).toInt
        val day = tmp(2).toInt

        val time = tmp(3) split (":")

        val hour = time(0).toInt
        val min  = time(1).toInt
        val sec  = time(2).toInt

        val dateTime = LocalDateTime.of(year, month, day, hour, min, sec)

        Tweet(sp(0).toLong, sp(1), dateTime, sp(11), sp(31))

    }
}
