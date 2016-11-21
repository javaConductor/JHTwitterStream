package sample.twitterstreams.statisticsprocessor

import java.net.URL
import java.util.Date

import sample.twitterstreams.extractor.{TweetReceived, UrlFound}
import sample.twitterstreams.model.{DataPoint, Report, Statistic}

/**
  * Creates and updates PercentageOfTweetsWithURLs statistic from TweetReceived,UrlFound dataPoints
  */

  case class PercentageOfTweetsWithPhotoURLs( totalTweets: Long, tweetsWithPhotoURLs: Long ) extends Statistic {
    override def report(): String = {
      val pct:Double = tweetsWithPhotoURLs.toDouble * 100.0 / totalTweets.toDouble
      s"Percentage of Tweets containing photo URLs: ${new java.math.BigDecimal( pct).setScale(2, BigDecimal.RoundingMode.HALF_UP)}%"
    }
  }

  class PercentageOfTweetsWithPhotoURLsProcessor extends StatisticProcessor{
    val started = new Date
    val name = "PercentageOfTweetsWithPhotoURLs"
    def init(report: Report):Report = {
      report.copy(statistics = report.statistics + (name -> new PercentageOfTweetsWithPhotoURLs(0,0) ))
    }

    def hasPhotoUrl(urls:List[String]) = {
      urls.exists (new URL(_).getHost().endsWith("instagram.com")) ||
      urls.exists (new URL(_).getHost().endsWith("pic.twitter.com"))
    }

    def applyDataPoint(report:Report, dataPoint:DataPoint ):Report  = {
      dataPoint match {
        case tr:TweetReceived => {
          report.statistics(name) match {
            case stat: PercentageOfTweetsWithPhotoURLs => {
              val newStat = stat.copy(totalTweets = stat.totalTweets + 1)
              report.copy(statistics = (report.statistics + ((name -> newStat))))
            }
          }
        }
        case tr:UrlFound => {
          report.statistics(name) match {
            case stat: PercentageOfTweetsWithPhotoURLs => {
              hasPhotoUrl(tr.urls) match {
                case true =>
                  val newStat = stat.copy(tweetsWithPhotoURLs = stat.tweetsWithPhotoURLs + 1)
                  report.copy(statistics = (report.statistics + ((name -> newStat))))
                case false =>
                  report
              }
            }
          }
        }
        case _ => { report }
      }
    }
  }


