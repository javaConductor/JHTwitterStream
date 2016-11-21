package sample.twitterstreams.statisticsprocessor

import java.util.Date

import sample.twitterstreams.extractor.{EmojisFound, TweetReceived, UrlFound}
import sample.twitterstreams.model.{DataPoint, Report, Statistic}

/**
  * Creates and updates PercentageOfTweetsWithEmojis statistic from TweetReceived,EmojisFound dataPoints
  */

  case class PercentageOfTweetsWithEmojis ( totalTweets: Long, tweetsWithEmojis: Long ) extends Statistic {
    override def report(): String = {
      val pct:Double = tweetsWithEmojis.toDouble * 100.0 / totalTweets.toDouble
      s"Percentage of Tweets containing Emojis: ${new java.math.BigDecimal( pct).setScale(2, BigDecimal.RoundingMode.HALF_UP)}%"
    }
  }

  class PercentageOfTweetsWithEmojisProcessor extends StatisticProcessor{
    val started = new Date
    val name = "PercentageOfTweetsWithEmojis"
    def init(report: Report):Report = {
      report.copy(statistics = report.statistics + (name -> new PercentageOfTweetsWithEmojis(0,0) ))
    }

    def applyDataPoint(report:Report, dataPoint:DataPoint ):Report  = {
      dataPoint match {
        case tr:TweetReceived => {
          report.statistics(name) match {
            case stat: PercentageOfTweetsWithEmojis => {
              val newStat = stat.copy(totalTweets = stat.totalTweets + 1)
              report.copy(statistics = (report.statistics + ((name -> newStat))))
            }
          }
        }
        case tr:EmojisFound => {
          report.statistics(name) match {
            case stat: PercentageOfTweetsWithEmojis => {
              val newStat = if (tr.emojis.size == 0) stat else  stat.copy(tweetsWithEmojis = stat.tweetsWithEmojis + 1)
              report.copy(statistics = (report.statistics + ((name -> newStat))))
            }
          }
        }
        case _ => { report }
      }
    }
  }
