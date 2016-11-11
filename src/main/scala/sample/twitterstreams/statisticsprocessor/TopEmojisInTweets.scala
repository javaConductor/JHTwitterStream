package sample.twitterstreams.statisticsprocessor

import java.util.Date

import sample.twitterstreams.extractor.EmojisFound
import sample.twitterstreams.model._
import sample.twitterstreams.service.EmojiService

/**
  * Creates and updates TopEmojisInTweets statistic from EmojisFound dataPoints
  */
case class TopEmojisInTweets(emojis: Map[String, Int] ) extends Statistic {

  val emojiService = new EmojiService()
  def sortEmojis = (a : Tuple2[String, Int], b: Tuple2[String, Int]) =>  a._2 > b._2

  override def report(): String = {
    def sorted:List[Tuple2[String, Int]] = emojis.toList.sortWith (sortEmojis)
    def sortedStrings = sorted.map ( x =>
      x._1 + s"[${emojiService.hexString(x._1)}]"  + "(" + x._2 + ")"
    ).take( 10 )
    sortedStrings.mkString(s"Top ${sortedStrings.length} emojis:\n\t", "\n\t", "")
  }
}

class TopEmojisInTweetsProcessor extends StatisticProcessor{
  val started = new Date
  val name = "TopEmojisInTweets" ;
  def init(report: Report):Report = {
    report.copy(statistics = report.statistics + (name -> new TopEmojisInTweets(Map()) ))
  }

  def incrementEmojiCount(topEmojisInTweets:TopEmojisInTweets, emoji: String): TopEmojisInTweets = {
    //add slot if needed
    if( !topEmojisInTweets.emojis.isDefinedAt(emoji) )
      return incrementEmojiCount(
                                  topEmojisInTweets.copy(emojis = topEmojisInTweets.emojis + ( emoji -> 0)),
                                  emoji
                                )
    val n:Int = topEmojisInTweets.emojis(emoji)
    topEmojisInTweets.copy(emojis = topEmojisInTweets.emojis + (emoji -> (n + 1))  )
  }

  def incrementEmojiCounts(report:Report, stat: TopEmojisInTweets, emojis: List[String]): Report = {
    emojis match {
      case x :: xs =>
        var stat = report.statistics(name).asInstanceOf[TopEmojisInTweets]
        emojis foreach ((s:String) => {
          stat = incrementEmojiCount(stat, s)
        })
        report.copy(statistics = report.statistics + (name -> stat ))
      case _ => { report }
    }
  }

  def applyDataPoint(report:Report, dataPoint:DataPoint ):Report  = {
    dataPoint match {
      case ht:EmojisFound => {
        report.statistics(name) match {
          case stat: TopEmojisInTweets => {
            incrementEmojiCounts(report, stat, ht.emojis)
          }
        }
      }
      case _ => { report }
    }
  }
}
