package sample.twitterstreams.extractor
import java.util.Date

import org.json4s._
import sample.twitterstreams.model.{DataPoint}
import sample.twitterstreams.service.EmojiService

/**
  * Created by lcollins on 11/10/2016.
  */
class EmojisFoundExtractor extends DataPointExtractor{
  override def extractDataPoints(json: JValue): List[DataPoint] = {
    val msgId = (json \ "id").values.toString()
    (json \ "text").toOption match {
      case Some(t: JString) => List(new EmojisFound(msgId, findEmojis(t.values), new Date()))
      case _ =>
        List()
    }
  }

  val emojiService = new EmojiService()

  def findEmojis(line: String): List[String] = {
    line.toCharArray.map(_.toString).toList.flatMap((s: String) => {
      if (emojiService.isEmoji(s))
        List( s )
      else
        List()
    })
  }
}
case class EmojisFound(tweetId: String, emojis:List[String], when:Date) extends DataPoint
