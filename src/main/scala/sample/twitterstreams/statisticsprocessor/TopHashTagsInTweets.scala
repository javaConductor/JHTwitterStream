package sample.twitterstreams.statisticsprocessor

import java.util.Date

import sample.twitterstreams.extractor.HashTagsFound
import sample.twitterstreams.model.{DataPoint, Report, Statistic}

/**
  *
  * Created by lcollins on 11/8/2016.
  */
case class TopHashTagsInTweets( hashTagsInTweets: Map[String, Int ] ) extends Statistic {
  def sortHashTags = (a : Tuple2[String, Int], b: Tuple2[String, Int]) =>  a._2 > b._2
  override def report(): String = {
    def sorted: List[Tuple2[String, Int]] = hashTagsInTweets.toList.sortWith(sortHashTags)
    def sortedStrings = sorted.map(x =>
                                     "#"+x._1 + "(" + x._2 + ")"
                                  ).take(5)
    sortedStrings.mkString(s"Top ${sortedStrings.length} hashtags:\n\t", "\n\t", "")
  }
}

class TopHashTagsInTweetsProcessor extends StatisticProcessor{
  val started = new Date
  val name = "TopHashTagsInTweets" ;
  def init(report: Report):Report = {
    report.copy(statistics = report.statistics + (name -> new TopHashTagsInTweets(Map()) ))
  }

  def incrementHashTagCount(topHashTagsInTweets:TopHashTagsInTweets,  hashTag: String): TopHashTagsInTweets = {
    //add slot if needed
    if( !topHashTagsInTweets.hashTagsInTweets.isDefinedAt(hashTag) )
      return incrementHashTagCount(
          topHashTagsInTweets.copy(hashTagsInTweets = topHashTagsInTweets.hashTagsInTweets + ( hashTag -> 0)),
          hashTag
        )
    val n:Int = topHashTagsInTweets.hashTagsInTweets(hashTag)
    val newTuple = hashTag -> (n + 1)
    topHashTagsInTweets.copy(hashTagsInTweets = topHashTagsInTweets.hashTagsInTweets + newTuple  )
  }

  def incrementHashTagCounts(report:Report, stat: TopHashTagsInTweets, hashTags: List[String]): Report = {
    hashTags match {
      case x :: xs =>
        var stat = report.statistics(name).asInstanceOf[TopHashTagsInTweets]
        hashTags foreach ((s:String) => {
          stat = incrementHashTagCount ( stat, s)
        } )
        report.copy(statistics = report.statistics + (name -> stat ))
      case _ => { report }
    }
  }

  def applyDataPoint(report:Report, dataPoint:DataPoint ):Report  = {
    dataPoint match {
      case ht:HashTagsFound => {
        report.statistics(name) match {
          case stat: TopHashTagsInTweets => {
            incrementHashTagCounts(report, stat, ht.hashTags)
          }
        }
      }
      case _ => { report }
    }
  }

}

