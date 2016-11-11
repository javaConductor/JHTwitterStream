package sample.twitterstreams.statisticsprocessor

import java.util.Date

import sample.twitterstreams.extractor.TweetReceived
import sample.twitterstreams.model.{DataPoint, Report, Statistic}

/**
  * Creates and updates AverageTweetsPerMin statistic from TweetsReceived dataPoints
  */

  case class AverageTweetsPerMin( start: Date, totalTweets: Long ) extends Statistic {
    override def report(): String = {

      val duration:Double = new Date().getTime - start.getTime
      val minutes = duration / 1000.0 / 60.0
      val tweetsPerMin = if (minutes < 1.0) {totalTweets} else { totalTweets / minutes}

      s"Average Tweets Per Minute${if (minutes < 1.0) "(In first minute.)" else "" }: ${new java.math.BigDecimal( tweetsPerMin).setScale(2, BigDecimal.RoundingMode.HALF_UP)}"
    }
  }

  class AverageTweetsPerMinProcessor extends StatisticProcessor{
    val started = new Date
    val name = "AverageTweetsPerMin"
    def init(report: Report):Report = {
      report.copy(statistics = report.statistics + (name -> new AverageTweetsPerMin(new Date, 0) ))
    }

    def applyDataPoint(report:Report, dataPoint:DataPoint ):Report  = {
      dataPoint match {
        case tr:TweetReceived => {
          report.statistics(name) match {
            case stat: AverageTweetsPerMin => {
              val newStat = stat.copy(totalTweets = stat.totalTweets + 1)
              report.copy(statistics = (report.statistics + ((name -> newStat))))
            }
          }
        }
        case _ => { report }
      }
    }
  }

