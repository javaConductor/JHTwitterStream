package sample.twitterstreams.statisticsprocessor

import java.util.Date

import sample.twitterstreams.extractor.TweetReceived
import sample.twitterstreams.model._

case class TotalNumberOfTweets ( total: Int ) extends Statistic {
  override def report(): String = s"Total Number of Tweets: $total"
}

class TotalNumberOfTweetsProcessor extends StatisticProcessor{
  val started = new Date

  override val name: String = "TotalNumberOfTweets"
  def init(report: Report):Report = {
    report.copy(statistics = report.statistics + (name -> new TotalNumberOfTweets(0) ))
  }

  def applyDataPoint(report:Report, dataPoint:DataPoint ):Report  = {
    dataPoint match {
      case tr:TweetReceived => {
        report.statistics("TotalNumberOfTweets") match {
          case stat:TotalNumberOfTweets => {
            val newStat = stat.copy(total  = stat.total + 1)
            report.copy(statistics = (report.statistics + (("TotalNumberOfTweets" -> newStat))))
          }
          case _ => report
        }
      }
      case _ => report
    }
  }
}
