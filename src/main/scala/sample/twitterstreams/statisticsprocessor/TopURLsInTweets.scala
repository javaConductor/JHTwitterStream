package sample.twitterstreams.statisticsprocessor

import java.util.Date

import sample.twitterstreams.extractor.UrlFound
import sample.twitterstreams.model._

/**
  * Created by lcollins on 11/8/2016.
  */
case class TopURLsInTweets(urlsInTweets: Map[String, Int]) extends Statistic {
  def sortUrls = (a: Tuple2[String, Int], b: Tuple2[String, Int]) => a._2 > b._2

  override def report(): String = {
    def sorted: List[Tuple2[String, Int]] = urlsInTweets.toList.sortWith(sortUrls)
    val sortedStrings = sorted.map(x => x._1 + "(" + x._2 + ")").take(5)
    sortedStrings.mkString(s"Top ${sortedStrings.length} URLs:\n\t", "\n\t", "")
  }
}

class TopURLsInTweetsProcessor extends StatisticProcessor {
  val started = new Date
  val name    = "TopURLsInTweets"

  def init(report: Report): Report = {
    report.copy(statistics = report.statistics + (name -> new TopURLsInTweets(Map())))
  }

  def incrementUrlCount(topURLsInTweets: TopURLsInTweets, url: String): TopURLsInTweets = {
    //add slot if needed
    if (!topURLsInTweets.urlsInTweets.isDefinedAt( url ))
      return incrementUrlCount( topURLsInTweets.copy(urlsInTweets = topURLsInTweets.urlsInTweets + (url -> 0)), url )
    val n: Int = topURLsInTweets.urlsInTweets( url )
    val newTuple = url -> (n + 1)
    topURLsInTweets.copy(urlsInTweets = topURLsInTweets.urlsInTweets + newTuple)
  }

  def incrementUrlCounts(report: Report, stat: TopURLsInTweets, urls: List[String]): Report = {
    urls match {
      case x :: xs =>
        var stat = report.statistics(name).asInstanceOf[TopURLsInTweets]
        urls foreach ((s: String) => {
          stat = incrementUrlCount(stat, s)
        })
        report.copy(statistics = report.statistics + (name -> stat))
      case _ => {
        report
      }
    }
  }

  def applyDataPoint(report: Report, dataPoint: DataPoint): Report = {
    dataPoint match {
      case u: UrlFound => {
        report.statistics(name) match {
          case stat: TopURLsInTweets => {
            incrementUrlCounts(report, stat, u.urls)
          }
        }
      }
      case _ => {
        report
      }
    }
  }
}
