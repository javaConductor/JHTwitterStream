package sample.twitterstreams.statisticsprocessor

import java.net.URL
import java.util.Date

import sample.twitterstreams.extractor.UrlFound
import sample.twitterstreams.model._

/**
  * Creates and updates TopURLsInTweets statistic from UrlFound dataPoints
  */
case class TopURLDomainsInTweets(domainUrlsInTweets: Map[String, Int]) extends Statistic {
  def sortUrls = (a: Tuple2[String, Int], b: Tuple2[String, Int]) => a._2 > b._2

  override def report(): String = {
    def sorted: List[Tuple2[String, Int]] = domainUrlsInTweets.toList.sortWith(sortUrls)
    val sortedStrings = sorted.map(x => x._1 + "(" + x._2 + ")").take(5)
    sortedStrings.mkString(s"Top ${sortedStrings.length} URL Domains:\n\t", "\n\t", "")
  }
}

class TopURLDomainsInTweetsProcessor extends StatisticProcessor {
  val started = new Date
  val name    = "TopURLDomainsInTweets"

  def init(report: Report): Report = {
    report.copy(statistics = report.statistics + (name -> new TopURLDomainsInTweets(Map())))
  }

  def incrementDomainCount(topURLDomainsInTweets: TopURLDomainsInTweets, url: String): TopURLDomainsInTweets = {
    //add slot if needed
    if (!topURLDomainsInTweets.domainUrlsInTweets.isDefinedAt(url))
      return incrementDomainCount(topURLDomainsInTweets.copy(domainUrlsInTweets = topURLDomainsInTweets.domainUrlsInTweets + (url -> 0)), url)
    val n: Int = topURLDomainsInTweets.domainUrlsInTweets(url)
    val newTuple = url -> (n + 1)
    topURLDomainsInTweets.copy(domainUrlsInTweets = topURLDomainsInTweets.domainUrlsInTweets + newTuple)
  }

  def incrementDomainCounts(report: Report, stat: TopURLDomainsInTweets, urls: List[String]): Report = {
    urls match {
      case x :: xs =>
        var stat = report.statistics(name).asInstanceOf[TopURLDomainsInTweets]
        urls foreach ((s: String) => {
          stat = incrementDomainCount(stat, s)
        })
        report.copy(statistics = report.statistics + (name -> stat))
      case _ => {
        report
      }
    }
  }

  def urlDomain(url : String):String = {
    new URL(url).getHost
  }

  def applyDataPoint(report: Report, dataPoint: DataPoint): Report = {
    dataPoint match {
      case u: UrlFound => {
        report.statistics(name) match {
          case stat: TopURLDomainsInTweets => {
            incrementDomainCounts(report, stat, u.urls.map(urlDomain).distinct)
          }
        }
      }
      case _ => {
        report
      }
    }
  }
}
