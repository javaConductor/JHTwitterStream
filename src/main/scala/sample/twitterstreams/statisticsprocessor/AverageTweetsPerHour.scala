package sample.twitterstreams.statisticsprocessor

import java.util.Date

import sample.twitterstreams.extractor.TweetReceived
import sample.twitterstreams.model.{DataPoint, Report, Statistic}

/**
  * Creates and updates AverageTweetsPerHour statistic from TweetsReceived dataPoints
  */

  case class AverageTweetsPerHour( start: Date, totalTweets: Long ) extends Statistic {
    override def report(): String = {

      val duration:Double = new Date().getTime - start.getTime
      val hours = duration / 1000.0 / 60.0 /60.0
      val tweetsPerHour = if (hours < 1.0) {totalTweets} else { totalTweets / hours}
      s"Average Tweets Per Hour${if (hours < 1.0) "(In first hour.)" else "" }: ${new java.math.BigDecimal( tweetsPerHour).setScale(0, BigDecimal.RoundingMode.HALF_UP)}"
    }
  }

  class AverageTweetsPerHourProcessor extends StatisticProcessor{
    val started = new Date

    val name = "AverageTweetsPerHour"
    def init(report: Report):Report = {
      report.copy(statistics = report.statistics + (name -> new AverageTweetsPerHour(new Date, 0) ))
    }

    def applyDataPoint(report:Report, dataPoint:DataPoint ):Report  = {
      dataPoint match {
        case tr:TweetReceived => {
          report.statistics(name) match {
            case stat: AverageTweetsPerHour => {
              val newStat = stat.copy(totalTweets = stat.totalTweets + 1)
              report.copy(statistics = (report.statistics + ((name -> newStat))))
            }
            case _ => {report}
          }
        }
        case _ => { report }
      }
    }
  }


