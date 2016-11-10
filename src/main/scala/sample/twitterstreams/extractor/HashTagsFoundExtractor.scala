package sample.twitterstreams.extractor

import java.util.Date

import org.json4s._
import sample.twitterstreams.model.{DataPoint}
import sample.twitterstreams.service.EmojiService

/**
  * find hash tags in message.
  */
class HashTagsFoundExtractor extends DataPointExtractor{
  override def extractDataPoints(json: JValue): List[DataPoint] = {
    val msgId = (json \ "id").values.toString()
    (json \ "entities" \ "hashtags" \ "text").toOption match {
      case Some(arr: JArray) if (arr.children.length > 0) =>
        List(new HashTagsFound(msgId,
                               arr.children.map(_.values).map(_.toString),
                               new Date()))
      case _ =>
        List()
    }
  }
}
case class HashTagsFound(tweetId: String, hashTags: List[String], when:Date) extends DataPoint
