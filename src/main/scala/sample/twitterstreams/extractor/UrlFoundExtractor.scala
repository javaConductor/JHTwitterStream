package sample.twitterstreams.extractor

import java.util.Date

import org.json4s._
import sample.twitterstreams.model.{DataPoint}

/**
  * find hash tags in message.
  */

class UrlFoundExtractor extends DataPointExtractor{
  override def extractDataPoints(json: JValue): List[DataPoint] = {
    val msgId = (json \ "id").values.toString()
    (json \ "entities" \ "urls" \ "expanded_url").toOption match {
      case Some(uarr: JArray) if (uarr.children.length > 0) =>
        val urls = uarr.values.filter(_ != null).map(_.toString)
        List(new UrlFound(msgId, urls, new Date()))
      case _ =>
        List()
    }
  }
}
case class UrlFound(tweetId: String, urls: List[String], when:Date) extends DataPoint
