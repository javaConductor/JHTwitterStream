package sample.twitterstreams.statisticsprocessor

import java.util.Date

import sample.twitterstreams.extractor.{TweetReceived, UrlFound}
import sample.twitterstreams.model.{DataPoint, Report, Statistic}

/**
  * Creates and updates PercentageOfTweetsWithURLs statistic from TweetReceived,UrlFound dataPoints
  */

  case class PercentageOfTweetsWithURLs( totalTweets: Long, tweetsWithURLs: Long ) extends Statistic {
    override def report(): String = {
      val pct:Double = tweetsWithURLs.toDouble * 100.0 / totalTweets.toDouble
      s"Percentage of Tweets containing URLs: ${new java.math.BigDecimal( pct).setScale(2, BigDecimal.RoundingMode.HALF_UP)}%"
    }
  }

  class PercentageOfTweetsWithURLsProcessor extends StatisticProcessor{
    val started = new Date
    val name = "PercentageOfTweetsWithURLs"
    def init(report: Report):Report = {
      report.copy(statistics = report.statistics + (name -> new PercentageOfTweetsWithURLs(0,0) ))
    }

    def applyDataPoint(report:Report, dataPoint:DataPoint ):Report  = {
      dataPoint match {
        case tr:TweetReceived => {
          report.statistics(name) match {
            case stat: PercentageOfTweetsWithURLs => {
              val newStat = stat.copy(totalTweets = stat.totalTweets + 1)
              report.copy(statistics = (report.statistics + ((name -> newStat))))
            }
            case _ => report
          }
        }
        case tr:UrlFound => {
          report.statistics(name) match {
            case stat: PercentageOfTweetsWithURLs => {
              val newStat = stat.copy(tweetsWithURLs = stat.tweetsWithURLs + 1)
              report.copy(statistics = (report.statistics + ((name -> newStat))))
            }
            case _ => report
          }
        }
        case _ => { report }
      }
    }
  }


