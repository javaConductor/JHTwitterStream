package sample.twitterstreams.statisticsprocessor

import java.util.Date

import sample.twitterstreams.extractor.TweetReceived
import sample.twitterstreams.model.{DataPoint, Report, Statistic}

/**
  * Creates and updates AverageTweetsPerSec statistic from TweetsReceived dataPoints
  */

  case class AverageTweetsPerSec( start: Date, totalTweets: Long ) extends Statistic {
    override def report(): String = {
      val duration:Double = new Date().getTime - start.getTime
      val seconds = duration / 1000.0
      val tweetsPerSecond = if (seconds < 1.0) {totalTweets} else { totalTweets / seconds}
      s"Average Tweets Per Second: ${new java.math.BigDecimal( tweetsPerSecond).setScale(2, BigDecimal.RoundingMode.HALF_UP)}"
    }
  }

  class AverageTweetsPerSecProcessor extends StatisticProcessor{
    val started = new Date
    val name = "AverageTweetsPerSec"
    def init(report: Report):Report = {
      report.copy(statistics = report.statistics + (name -> new AverageTweetsPerSec(new Date, 0) ))
    }

    def applyDataPoint(report:Report, dataPoint:DataPoint ):Report  = {
      dataPoint match {
        case tr:TweetReceived => {
          report.statistics(name) match {
            case stat: AverageTweetsPerSec => {
              val newStat = stat.copy(totalTweets = stat.totalTweets + 1)
              report.copy(statistics = (report.statistics + ((name -> newStat))))
            }
          }
        }
        case _ => { report }
      }
    }
  }


